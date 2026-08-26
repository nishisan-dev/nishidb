package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	nishiconfig "github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/protocolo"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/query/lexer"
	"github.com/nishisan-dev/nishidb/internal/query/orcamento"
)

func runQuery(args []string, amb Ambiente) int {
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	fs.SetOutput(amb.Stderr)
	var (
		argConfig = fs.String("config", "", "caminho do nishidb.yaml")
		dataset   = fs.String("dataset", "", "nome do dataset")
		limit     = fs.Int("limit", -1, "sobrepõe o LIMIT da query")
		servidor  = fs.String("server", "", "consulta pelo protocolo remoto em vez de abrir o dataset local")
	)
	var params listaDeParametros
	fs.Var(&params, "param", "valor de um `?` posicional, no formato <tipo>:<valor> ("+tiposDeParametro+"); repetível, na ordem dos ?")
	credFlags := registraFlagsRemotas(fs)
	if err := fs.Parse(args); err != nil {
		return ExitUso
	}
	// A credencial só é USADA no caminho --server (consultaRemota, abaixo),
	// mas a validação roda sempre: --user/--password-file/NISHIDB_PASSWORD
	// mal combinados são erro de uso independente de --config ou --server, e
	// resolver aqui garante a MESMA mensagem que status/datasets/inspect/
	// compact/checkpoint dão para o mesmo engano.
	cred, err := credFlags.resolve()
	if err != nil {
		// `_, _ =`: mesma disciplina de avisa (operacao.go) para código NOVO
		// desta task — o gate de lint deste projeto cobra 0 apontamentos
		// novos, e as chamadas bare a fmt.Fprintln já existentes neste
		// arquivo são débito anterior a esta task, não um precedente a repetir.
		_, _ = fmt.Fprintln(amb.Stderr, "query:", err)
		return ExitUso
	}
	// Com --server o dataset não é aberto aqui, então não há nishidb.yaml para
	// ler: quem resolve caminho, teto e timeout é o servidor. Exigir --config
	// obrigaria o operador a manter uma cópia da configuração do servidor na
	// máquina de onde ele consulta.
	switch {
	case *dataset == "":
		fmt.Fprintln(amb.Stderr, "query: --dataset é obrigatório")
		return ExitUso
	case *servidor == "" && *argConfig == "":
		fmt.Fprintln(amb.Stderr, "query: informe --config (consulta local) ou --server (consulta remota)")
		return ExitUso
	case *servidor != "" && *argConfig != "":
		fmt.Fprintln(amb.Stderr, "query: --config e --server são exclusivos; o --server usa a configuração do servidor")
		return ExitUso
	}
	if fs.NArg() != 1 {
		// A dica sobre a ordem existe por causa do --param: flag.Parse para no
		// primeiro argumento posicional, então `query --dataset d 'SELECT...'
		// --param string:x` não parseia a flag — ela vira Arg(1) e cai aqui,
		// com uma mensagem que sozinha não ajudaria em nada.
		_, _ = fmt.Fprintln(amb.Stderr, "query: informe exatamente uma query SQL (as flags vêm ANTES dela)")
		return ExitUso
	}

	// A validação roda ANTES de qualquer conexão ou abertura de dataset, e usa
	// protocolo.Valores — a MESMA função que o servidor usa, nunca uma segunda
	// gramática. É o que faz `--param int64:9223372036854775808` receber a
	// mensagem de overflow do protocolo na linha de comando, em vez de depois
	// de um round-trip.
	valores, err := protocolo.Valores(params)
	if err != nil {
		_, _ = fmt.Fprintln(amb.Stderr, "query:", err)
		return ExitUso
	}
	sql := fs.Arg(0)
	if *limit >= 0 {
		sql = comLimit(sql, *limit)
	}

	if *servidor != "" {
		return consultaRemota(*servidor, *dataset, sql, params, cred, amb)
	}

	cfg, err := nishiconfig.CarregaConfig(*argConfig)
	if err != nil {
		fmt.Fprintln(amb.Stderr, "query:", err)
		return ExitUso
	}

	st, compilado, err := abreDataset(cfg, *dataset)
	if err != nil {
		fmt.Fprintln(amb.Stderr, "query:", err)
		dicaDeServidor(err, *argConfig, *dataset, amb)
		return codigoDoErro(err)
	}
	defer st.Close()

	eng := query.NewEngine()
	if err := eng.Register(*dataset, compilado, st); err != nil {
		fmt.Fprintln(amb.Stderr, "query:", err)
		return ExitUso
	}

	// O caminho local recebe os MESMOS valores. Ignorá-los aqui faria a flag
	// mentir num dos dois modos, e quebraria a promessa escrita em
	// consultaRemota: "um script que troca --config por --server não muda mais
	// nada".
	it, err := eng.Execute(amb.Ctx, *dataset, sql, valores)
	if err != nil {
		// O erro de sintaxe/bind sai ANTES de qualquer byte no stdout: quem
		// consome a saída não pode receber linha parcial seguida de erro.
		imprimeErroDeQuery(amb.Stderr, sql, err)
		return codigoDoErro(err)
	}
	defer it.Close()

	// Streaming de verdade: o writer descarrega por limiar e nada do resultado
	// é acumulado.
	//
	// O orçamento sai do MESMO nishidb.yaml que o servidor lê, e não de um
	// "sem teto" local: um `runtime.query.maxBufferPerQuery` que vale pela porta
	// e não vale pelo CLI é a chave mentindo em metade dos caminhos, que é o
	// defeito de que este orçamento nasceu (débito 15). O processo do CLI serve
	// UMA consulta, então o Global é dele sozinho.
	orc := orcamento.NovoGlobal(cfg.MemoryTarget()).Query(cfg.MaxBufferPerQuery())
	if !orc.Admite() {
		// Só acontece com `maxBufferPerQuery` acima do `memoryTarget`, que o
		// CarregaConfig já recusa — mas o retorno é conferido do mesmo jeito:
		// ignorá-lo por "não pode acontecer" é como o writer ficaria sem
		// orçamento no dia em que a validação mudasse de lugar.
		// `_, _ =`: a disciplina que este arquivo já declara para código NOVO —
		// o gate de lint cobra 0 apontamentos novos, e as chamadas bare a
		// fmt.Fprintln daqui são débito anterior, não precedente a repetir.
		_, _ = fmt.Fprintln(amb.Stderr,
			"query: o orçamento de memória de consulta não comporta esta consulta "+
				"(runtime.memoryTarget contra runtime.query.maxBufferPerQuery)")
		return ExitUso
	}
	defer orc.Libera()
	w := executor.NewStreamWriter(amb.Stdout, executor.DefaultBufSize, orc)
	for it.Next() {
		if err := w.WriteRow(it.Row()); err != nil {
			fmt.Fprintln(amb.Stderr, "query: escrevendo o resultado:", err)
			return ExitUso
		}
	}
	if err := w.Flush(); err != nil {
		fmt.Fprintln(amb.Stderr, "query: escrevendo o resultado:", err)
		return ExitUso
	}
	if err := it.Err(); err != nil {
		// O truncamento sai por aqui como qualquer outro erro: o codigoDoErro
		// já o traduz no exit code próprio, e a mensagem do sentinela já diz
		// "resultado truncado no teto de query.maxRows" com o número. Um ramo
		// especial aqui seria indistinguível deste — código que nenhum teste
		// consegue separar é decoração.
		//
		// O que NÃO pode acontecer é sair 0: as linhas emitidas são válidas,
		// mas o resultado não está completo, e quem consome precisa saber.
		fmt.Fprintln(amb.Stderr, "query:", err)
		return codigoDoErro(err)
	}
	return ExitOK
}

// comLimit acrescenta (ou substitui) o LIMIT da query.
//
// A substituição é textual e deliberadamente simples: reescrever a AST exigiria
// serializá-la de volta, e a v1 não tem serializador. Se a query já tem LIMIT,
// o --limit vence, que é o que o operador espera de uma flag explícita.
func comLimit(sql string, n int) string {
	corte := strings.TrimRight(strings.TrimSpace(sql), ";")
	if i := ultimoLimit(corte); i >= 0 {
		corte = strings.TrimSpace(corte[:i])
	}
	return fmt.Sprintf("%s LIMIT %d", corte, n)
}

// ultimoLimit devolve o offset da última cláusula LIMIT fora de literal, ou -1.
//
// Quem decide o que é cláusula e o que é literal é o LEXER DE VERDADE, o mesmo
// que o Execute vai usar. Uma varredura textual própria é uma segunda
// gramática, e a daqui já divergia da de verdade: reconhecia só a aspa
// SIMPLES, então um identificador entre aspas duplas — obrigatórias para
// os nomes reais do dataset, que têm hífen e espaço — com a palavra limit no
// meio fazia o corte cair DENTRO do identificador e reescrever a query.
//
// Query que nem tokeniza devolve -1, e o --limit é apenas anexado: sem
// gramática íntegra não há como saber o que é literal, e cortar por adivinhação
// entregaria ao usuário um erro de sintaxe sobre um texto que ele não escreveu.
func ultimoLimit(sql string) int {
	var lx lexer.Lexer
	lx.Init(sql)
	achado := -1
	for {
		tok, err := lx.Next()
		if err != nil {
			return -1
		}
		switch tok.Type {
		case lexer.TokenEOF:
			return achado
		case lexer.TokenLimit:
			achado = tok.Pos
		}
	}
}

// imprimeErroDeQuery mostra a POSIÇÃO e o trecho ofensor, não só a mensagem.
// A posição é a informação que o front-end já carrega e que o usuário precisa
// para achar o erro numa query de várias linhas.
func imprimeErroDeQuery(w io.Writer, sql string, err error) {
	fmt.Fprintln(w, "query:", err)

	var se *lexer.SyntaxError
	pos := -1
	if errors.As(err, &se) {
		pos = se.Pos
	} else {
		var be *binder.Error
		if errors.As(err, &be) {
			pos = be.Pos
		}
	}
	if pos < 0 || pos > len(sql) {
		return
	}
	linha, coluna, texto := recorta(sql, pos)
	fmt.Fprintf(w, "  linha %d, coluna %d:\n    %s\n    %s^\n",
		linha, coluna, texto, strings.Repeat(" ", coluna-1))
}

// recorta devolve a linha (1-based), a coluna (1-based) e o texto da linha
// onde o offset cai.
func recorta(sql string, pos int) (int, int, string) {
	linha, inicio := 1, 0
	for i := 0; i < pos && i < len(sql); i++ {
		if sql[i] == '\n' {
			linha++
			inicio = i + 1
		}
	}
	fim := strings.IndexByte(sql[inicio:], '\n')
	if fim < 0 {
		fim = len(sql)
	} else {
		fim += inicio
	}
	return linha, pos - inicio + 1, sql[inicio:fim]
}
