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
	"github.com/nishisan-dev/nishidb/internal/query/parser"
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
	w := executor.NewStreamWriter(amb.Stdout, executor.DefaultBufSize)
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

// comLimit acrescenta (ou substitui) o LIMIT da query, PRESERVANDO o OFFSET.
//
// A substituição continua textual — reescrever a AST exigiria serializá-la de
// volta, e a v1 não tem serializador —, mas o corte deixou de ser "da última
// cláusula LIMIT em diante". Com OFFSET, aquele corte produzia RESULTADO
// ERRADO, não inconveniência:
//
//   - `... LIMIT 5 OFFSET 10` com --limit 3 virava `... LIMIT 3`. A paginação
//     EVAPORAVA e o comando devolvia as 3 PRIMEIRAS linhas como se fossem a
//     página pedida — sem erro, sem aviso.
//   - `... OFFSET 10` sem LIMIT virava `... OFFSET 10 LIMIT 3`, que é ordem
//     inválida: o usuário recebia erro de sintaxe sobre um texto que não
//     escreveu.
//
// Agora o corte é no PRIMEIRO dos dois (LIMIT ou OFFSET), e o OFFSET original é
// reemitido depois do LIMIT novo. Quem decide onde as cláusulas começam é o
// PARSER de verdade, o mesmo que o Execute vai usar — pelo mesmo argumento que
// já tinha tirado essa decisão de uma varredura textual própria: duas gramáticas
// divergem, e a divergência aparece como query reescrita errada.
func comLimit(sql string, n int) string {
	corte := strings.TrimRight(strings.TrimSpace(sql), ";")
	st, err := parser.Parse(corte)
	if err != nil {
		// Query que nem parseia: só anexa, como antes. Sem gramática íntegra
		// não há como saber o que é cláusula, e cortar por adivinhação
		// entregaria um erro de sintaxe sobre um texto que o usuário não
		// escreveu.
		return fmt.Sprintf("%s LIMIT %d", corte, n)
	}
	inicio := len(corte)
	if st.HasLimit && st.LimitPos < inicio {
		inicio = inicioDaClausula(corte, st.LimitPos)
	}
	if st.HasOffset && inicioDaClausula(corte, st.OffsetPos) < inicio {
		inicio = inicioDaClausula(corte, st.OffsetPos)
	}
	corte = strings.TrimSpace(corte[:inicio])
	if st.HasOffset {
		return fmt.Sprintf("%s LIMIT %d OFFSET %d", corte, n, st.Offset)
	}
	return fmt.Sprintf("%s LIMIT %d", corte, n)
}

// inicioDaClausula recua da posição do LITERAL até o início da palavra-chave
// que o precede.
//
// O parser guarda a posição do NÚMERO (`LimitPos` aponta para o `10` de
// `LIMIT 10`), que é o que a mensagem de erro precisa. Para cortar, é preciso a
// posição da PALAVRA — cortar no número deixaria um `LIMIT` órfão no texto.
func inicioDaClausula(sql string, posDoLiteral int) int {
	i := posDoLiteral
	// Recua o espaço entre a palavra e o número.
	for i > 0 && (sql[i-1] == ' ' || sql[i-1] == '\t' || sql[i-1] == '\n' || sql[i-1] == '\r') {
		i--
	}
	// Recua a palavra.
	for i > 0 && !ehEspaco(sql[i-1]) {
		i--
	}
	return i
}

func ehEspaco(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

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
