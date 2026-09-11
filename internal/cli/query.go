package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	nishiconfig "github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/metakafka"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/budget"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/query/lexer"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
)

func runQuery(args []string, amb Context) int {
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	fs.SetOutput(amb.Stderr)
	var (
		argConfig = fs.String("config", "", "caminho do nishidb.yaml")
		dataset   = fs.String("dataset", "", "nome do dataset")
		limit     = fs.Int("limit", -1, "sobrepõe o LIMIT da query")
		server    = fs.String("server", "", "consulta pelo protocolo remoto em vez de abrir o dataset local")
	)
	var params listaOfParametros
	fs.Var(&params, "param", "valor de um `?` posicional, no formato <tipo>:<valor> ("+tiposOfParametro+"); repetível, na ordem dos ?")
	credFlags := setupRemoteFlags(fs)
	if err := fs.Parse(args); err != nil {
		return ExitUsageError
	}
	// A credencial só é USADA no caminho --server (consultaRemota, abaixo),
	// mas a validação roda sempre: --user/--password-file/NISHIDB_PASSWORD
	// mal combinados são erro de uso independente de --config ou --server, e
	// resolver aqui garante a MESMA mensagem que status/datasets/inspect/
	// compact/checkpoint dão para o mesmo engano.
	cred, err := credFlags.resolve()
	if err != nil {
		// `_, _ =`: mesma disciplina de avisa (operation.go) para código NOVO
		// desta task — o gate de lint deste projeto cobra 0 apontamentos
		// novos, e as chamadas bare a fmt.Fprintln já existentes neste
		// arquivo são débito anterior a esta task, não um precedente a repetir.
		_, _ = fmt.Fprintln(amb.Stderr, "query:", err)
		return ExitUsageError
	}
	// Com --server o dataset não é aberto aqui, então não há nishidb.yaml para
	// ler: quem resolve caminho, teto e timeout é o servidor. Exigir --config
	// obrigaria o operador a manter uma cópia da configuração do servidor na
	// máquina de onde ele consulta.
	switch {
	case *dataset == "":
		fmt.Fprintln(amb.Stderr, "query: --dataset é obrigatório")
		return ExitUsageError
	case *server == "" && *argConfig == "":
		fmt.Fprintln(amb.Stderr, "query: informe --config (consulta local) ou --server (consulta remota)")
		return ExitUsageError
	case *server != "" && *argConfig != "":
		fmt.Fprintln(amb.Stderr, "query: --config e --server são exclusivos; o --server usa a configuração do servidor")
		return ExitUsageError
	}
	if fs.NArg() != 1 {
		// A dica sobre a ordem existe por causa do --param: flag.Parse para no
		// primeiro argumento posicional, então `query --dataset d 'SELECT...'
		// --param string:x` não parseia a flag — ela vira Arg(1) e cai aqui,
		// com uma mensagem que sozinha não ajudaria em nada.
		_, _ = fmt.Fprintln(amb.Stderr, "query: informe exatamente uma query SQL (as flags vêm ANTES dela)")
		return ExitUsageError
	}

	// A validação roda ANTES de qualquer conexão ou abertura de dataset, e usa
	// protocol.Values — a MESMA função que o servidor usa, nunca uma segunda
	// gramática. É o que faz `--param int64:9223372036854775808` receber a
	// mensagem de overflow do protocolo na linha de comando, em vez de depois
	// de um round-trip.
	values, err := protocol.Values(params)
	if err != nil {
		_, _ = fmt.Fprintln(amb.Stderr, "query:", err)
		return ExitUsageError
	}
	sql := fs.Arg(0)
	if *limit >= 0 {
		sql = withLimit(sql, *limit)
	}

	if *server != "" {
		return remoteQuery(*server, *dataset, sql, params, cred, amb)
	}

	cfg, err := nishiconfig.Load(*argConfig)
	if err != nil {
		fmt.Fprintln(amb.Stderr, "query:", err)
		return ExitUsageError
	}

	st, compilado, spec, close, err := openForQuery(cfg, *dataset)
	if err != nil {
		fmt.Fprintln(amb.Stderr, "query:", err)
		serverHint(err, *argConfig, *dataset, amb)
		return errorCode(err)
	}
	defer close()

	eng := query.NewEngine()
	if err := eng.Register(*dataset, compilado, st); err != nil {
		fmt.Fprintln(amb.Stderr, "query:", err)
		return ExitUsageError
	}
	if err := counters.Register(eng, *dataset, st); err != nil {
		notify(amb, "query: %v\n", err)
		return ExitUsageError
	}
	// `_meta.kafka` responde aqui com o que EXISTE fora do servidor: a spec
	// (tópico, grupo, bootstrap, enabled) e a posição durável do checkpoint.
	// Sem consumidor, as colunas vivas — fila, desfecho, marca d'água,
	// watchdog — são NULL, e o `state` também: este processo não observa
	// ingestão nenhuma, e DEGRADED ("ainda subindo") seria mentira. Com
	// `enabled: false`, o estado é DISABLED, que o YAML prova sozinho.
	//
	// Só no dataset DECLARADO, como no servidor: a irmã não tem fonte própria.
	if spec != nil {
		if err := metakafka.Register(eng, *dataset, metakafka.Source{
			Declarada: metakafka.OfYAML(spec.Source.Kafka),
			Duraveis:  st.SourceOffsets,
		}); err != nil {
			notify(amb, "query: %v\n", err)
			return ExitUsageError
		}
	}

	// O caminho local recebe os MESMOS valores. Ignorá-los aqui faria a flag
	// mentir num dos dois modos, e quebraria a promessa escrita em
	// consultaRemota: "um script que troca --config por --server não muda mais
	// nada".
	it, err := eng.Execute(amb.Ctx, *dataset, sql, values)
	if err != nil {
		// O erro de sintaxe/bind sai ANTES de qualquer byte no stdout: quem
		// consome a saída não pode receber linha parcial seguida de erro.
		printQueryError(amb.Stderr, sql, err)
		return errorCode(err)
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
	orc := budget.NewGlobal(cfg.MemoryTarget()).Query(cfg.MaxBufferPerQuery())
	if !orc.Admit() {
		// Só acontece com `maxBufferPerQuery` acima do `memoryTarget`, que o
		// Load já recusa — mas o retorno é conferido do mesmo jeito:
		// ignorá-lo por "não pode acontecer" é como o writer ficaria sem
		// orçamento no dia em que a validação mudasse de lugar.
		// `_, _ =`: a disciplina que este arquivo já declara para código NOVO —
		// o gate de lint cobra 0 apontamentos novos, e as chamadas bare a
		// fmt.Fprintln daqui são débito anterior, não precedente a repetir.
		_, _ = fmt.Fprintln(amb.Stderr,
			"query: o orçamento de memória de consulta não comporta esta consulta "+
				"(runtime.memoryTarget contra runtime.query.maxBufferPerQuery)")
		return ExitUsageError
	}
	defer orc.Release()
	w := executor.NewStreamWriter(amb.Stdout, executor.DefaultBufSize, orc)
	for it.Next() {
		if err := w.WriteRow(it.Row()); err != nil {
			fmt.Fprintln(amb.Stderr, "query: escrevendo o resultado:", err)
			return ExitUsageError
		}
	}
	if err := w.Flush(); err != nil {
		fmt.Fprintln(amb.Stderr, "query: escrevendo o resultado:", err)
		return ExitUsageError
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
		return errorCode(err)
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
func withLimit(sql string, n int) string {
	corte := strings.TrimRight(strings.TrimSpace(sql), ";")
	st, err := parser.Parse(corte)
	if err != nil {
		// Query que nem parseia: só anexa, como antes. Sem gramática íntegra
		// não há como saber o que é cláusula, e cortar por adivinhação
		// entregaria um erro de sintaxe sobre um texto que o usuário não
		// escreveu.
		return fmt.Sprintf("%s LIMIT %d", corte, n)
	}
	start := len(corte)
	if st.HasLimit && st.LimitPos < start {
		start = clauseStart(corte, st.LimitPos)
	}
	if st.HasOffset && clauseStart(corte, st.OffsetPos) < start {
		start = clauseStart(corte, st.OffsetPos)
	}
	corte = strings.TrimSpace(corte[:start])
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
func clauseStart(sql string, posOfLiteral int) int {
	i := posOfLiteral
	// Recua o espaço entre a palavra e o número.
	for i > 0 && (sql[i-1] == ' ' || sql[i-1] == '\t' || sql[i-1] == '\n' || sql[i-1] == '\r') {
		i--
	}
	// Recua a palavra.
	for i > 0 && !isSpace(sql[i-1]) {
		i--
	}
	return i
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

// imprimeErroDeQuery mostra a POSIÇÃO e o trecho ofensor, não só a mensagem.
// A posição é a informação que o front-end já carrega e que o usuário precisa
// para achar o erro numa query de várias linhas.
func printQueryError(w io.Writer, sql string, err error) {
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
	row, column, text := recorta(sql, pos)
	fmt.Fprintf(w, "  linha %d, coluna %d:\n    %s\n    %s^\n",
		row, column, text, strings.Repeat(" ", column-1))
}

// recorta devolve a line (1-based), a coluna (1-based) e o texto da line
// onde o offset cai.
func recorta(sql string, pos int) (int, int, string) {
	row, start := 1, 0
	for i := 0; i < pos && i < len(sql); i++ {
		if sql[i] == '\n' {
			row++
			start = i + 1
		}
	}
	end := strings.IndexByte(sql[start:], '\n')
	if end < 0 {
		end = len(sql)
	} else {
		end += start
	}
	return row, pos - start + 1, sql[start:end]
}
