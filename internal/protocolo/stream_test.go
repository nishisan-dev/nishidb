package protocolo_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/protocolo"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// escreveStream monta uma resposta completa: schema, rows cruas e fecho.
func escreveStream(t *testing.T, fecho any, rows ...string) string {
	t.Helper()
	var sb bytes.Buffer
	err := protocolo.EscreveLinha(&sb, protocolo.LinhaSchema{
		Nishidb: protocolo.MolduraSchema,
		Dataset: "radius_sessions",
		Columns: []protocolo.Coluna{
			protocolo.ColunaDe("id", types.TypeString),
			protocolo.ColunaDe("ip", types.TypeIPv4),
		},
	})
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	for _, r := range rows {
		sb.WriteString(r)
		sb.WriteByte('\n')
	}
	if fecho != nil {
		if err := protocolo.EscreveLinha(&sb, fecho); err != nil {
			t.Fatalf("fecho: %v", err)
		}
	}
	return sb.String()
}

func TestLeitorPercorreSchemaRowsEFecho(t *testing.T) {
	stream := escreveStream(t,
		protocolo.LinhaFim{Nishidb: protocolo.MolduraFim, Rows: 2},
		`{"id":"ABC","ip":"177.1.2.3"}`,
		`{"id":"DEF","ip":"177.1.2.4"}`,
	)

	l := protocolo.NovoLeitor(strings.NewReader(stream))
	var recebidas []string
	for l.Next() {
		// A linha é entregue CRUA: o cliente a repassa sem redecodificar.
		recebidas = append(recebidas, string(l.Linha()))
	}
	if err := l.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	if l.Schema() == nil || l.Schema().Dataset != "radius_sessions" {
		t.Fatalf("schema = %+v", l.Schema())
	}
	if got := len(l.Schema().Columns); got != 2 {
		t.Fatalf("colunas = %d, quer 2", got)
	}
	if l.Schema().Columns[1].JDBC != protocolo.JDBCVarchar {
		t.Fatalf("ipv4 saiu como %q, quer VARCHAR (§73)", l.Schema().Columns[1].JDBC)
	}
	if len(recebidas) != 2 || recebidas[0] != `{"id":"ABC","ip":"177.1.2.3"}` {
		t.Fatalf("rows = %q", recebidas)
	}
	if l.Fim() == nil || l.Fim().Rows != 2 || l.Fim().Truncated {
		t.Fatalf("fecho = %+v", l.Fim())
	}
	if l.Erro() != nil {
		t.Fatalf("Erro = %+v, quer nil", l.Erro())
	}
}

// O fecho de erro é o que separa "acabou" de "quebrou depois de 10.000 linhas".
// Sem ele o cliente apresentaria um resultado parcial como completo.
func TestFechoDeErroChegaDepoisDasRows(t *testing.T) {
	stream := escreveStream(t,
		protocolo.LinhaErro{
			Nishidb: protocolo.MolduraErro,
			Code:    protocolo.CodMaxRows,
			Rows:    1,
			Message: "resultado truncado no teto de query.maxRows: 1",
		},
		`{"id":"ABC"}`,
	)

	l := protocolo.NovoLeitor(strings.NewReader(stream))
	n := 0
	for l.Next() {
		n++
	}
	if err := l.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	if n != 1 {
		t.Fatalf("rows = %d, quer 1", n)
	}
	if l.Fim() != nil {
		t.Fatalf("Fim = %+v, quer nil", l.Fim())
	}
	if l.Erro() == nil || l.Erro().Code != protocolo.CodMaxRows || l.Erro().Rows != 1 {
		t.Fatalf("Erro = %+v", l.Erro())
	}
}

// Um stream que acaba sem fecho é falha de TRANSPORTE, não fim de resultado: é
// assim que uma conexão cortada deixa de ser indistinguível de sucesso.
func TestStreamSemFechoEhFalha(t *testing.T) {
	stream := escreveStream(t, nil, `{"id":"ABC"}`)

	l := protocolo.NovoLeitor(strings.NewReader(stream))
	for l.Next() {
	}
	if !errors.Is(l.Err(), protocolo.ErrStream) {
		t.Fatalf("Err = %v, quer ErrStream", l.Err())
	}
	if !strings.Contains(l.Err().Error(), "sem a linha de fecho") {
		t.Fatalf("mensagem %q não nomeia a causa", l.Err())
	}
	if l.Fim() != nil || l.Erro() != nil {
		t.Fatal("nenhum fecho deveria ter sido publicado")
	}
}

func TestPrimeiraLinhaSemSchemaEhFalha(t *testing.T) {
	l := protocolo.NovoLeitor(strings.NewReader("{\"id\":\"ABC\"}\n"))
	if l.Next() {
		t.Fatal("Next devolveu true sem schema")
	}
	if !errors.Is(l.Err(), protocolo.ErrStream) {
		t.Fatalf("Err = %v, quer ErrStream", l.Err())
	}
}

func TestMolduraDesconhecidaEhFalha(t *testing.T) {
	stream := "{\"nishidb\":\"schema\",\"dataset\":\"d\",\"columns\":[]}\n" +
		"{\"nishidb\":\"comeco_do_futuro\"}\n"
	l := protocolo.NovoLeitor(strings.NewReader(stream))
	for l.Next() {
	}
	if !errors.Is(l.Err(), protocolo.ErrStream) {
		t.Fatalf("Err = %v, quer ErrStream", l.Err())
	}
}

// O EscreveLinha não pode escapar HTML: as rows não escapam, e duas políticas
// no mesmo stream fariam o mesmo nome de coluna sair de dois jeitos.
func TestMolduraNaoEscapaHTML(t *testing.T) {
	var sb bytes.Buffer
	err := protocolo.EscreveLinha(&sb, protocolo.LinhaSchema{
		Nishidb: protocolo.MolduraSchema,
		Dataset: "d",
		Columns: []protocolo.Coluna{protocolo.ColunaDe("a<b>c&d", types.TypeString)},
	})
	if err != nil {
		t.Fatalf("EscreveLinha: %v", err)
	}
	if !strings.Contains(sb.String(), "a<b>c&d") {
		t.Fatalf("nome escapado: %s", sb.String())
	}
}

// O prefixo de moldura é o discriminador: uma linha de dados jamais pode
// começar por ele, e é por isso que o servidor recusa a projeção com uma coluna
// chamada "nishidb".
func TestPrefixoDeMolduraEhOQueOEncoderProduz(t *testing.T) {
	var sb bytes.Buffer
	if err := protocolo.EscreveLinha(&sb, protocolo.LinhaFim{Nishidb: protocolo.MolduraFim}); err != nil {
		t.Fatalf("EscreveLinha: %v", err)
	}
	if !strings.HasPrefix(sb.String(), `{"`+protocolo.ChaveMoldura+`":`) {
		t.Fatalf("fecho não começa pelo prefixo de moldura: %s", sb.String())
	}
}
