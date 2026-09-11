package query_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// O teste que pega a classe de bug mais perigosa do bloco: a coerção da QUERY
// produzindo um types.Value diferente do que a INGESTÃO gravou, para o mesmo
// texto.
//
// Ele falha como RESULTADO VAZIO, não como erro, e nenhum outro teste destes
// pacotes o detecta: os do binder conferem a coerção contra constantes
// escritas à mão, os do planner conferem o plano, e ninguém confere os dois
// lados JUNTOS. O único jeito é ingerir de verdade e consultar de verdade.
const tiposYAML = "../../testdata/todos_os_tipos.yaml"

type tiposHarness struct {
	t      *testing.T
	sch    *schema.Compiled
	store  *fakestorage.Store
	mat    *materializer.Materializer
	engine *query.Engine
}

func newTiposHarness(t *testing.T) *tiposHarness {
	t.Helper()
	spec, err := schema.Load(tiposYAML, schema.BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	sch, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	text, _ := sch.FieldByName([]byte("texto"))
	v4, _ := sch.FieldByName([]byte("v4"))
	st := fakestorage.New(fakestorage.Indexes{
		Fields: []types.FieldID{text.ID, v4.ID},
	})
	eng := query.NewEngine()
	if err := eng.Register("todos_os_tipos", sch, st); err != nil {
		t.Fatal(err)
	}
	mat, err := materializer.New(sch, st)
	if err != nil {
		t.Fatal(err)
	}
	return &tiposHarness{t: t, sch: sch, store: st, mat: mat, engine: eng}
}

func (h *tiposHarness) ingere(event string) {
	h.t.Helper()
	if r := h.mat.Apply([]byte(event)); r.Err != nil {
		h.t.Fatalf("ingestão de %s: %v", event, r.Err)
	}
}

// achaIDs roda a query e devolve a coluna id.
func (h *tiposHarness) achaIDs(sql string) []string {
	h.t.Helper()
	it, err := h.engine.Execute(context.Background(), "todos_os_tipos", sql, nil)
	if err != nil {
		h.t.Fatalf("query %q: %v", sql, err)
	}
	defer it.Close()
	var out []string
	for it.Next() {
		out = append(out, string(it.Row().Col(0).Val.Bytes))
	}
	if err := it.Err(); err != nil {
		h.t.Fatalf("query %q: %v", sql, err)
	}
	return out
}

// TestIngestaoEQueryConcordamPorTipo: para cada tipo comparável, o valor
// ingerido é encontrado pelo literal equivalente.
func TestIngestaoEQueryConcordamPorTipo(t *testing.T) {
	casos := []struct {
		name string
		// campo e valor como aparecem no evento JSON.
		fieldJSON string
		valueJSON string
		// predicado SQL que TEM de achar a row.
		where string
	}{
		{"string", "texto", `"Stop"`, `t = 'Stop'`},
		{"int64", "inteiro", `4096`, `inteiro = 4096`},
		{"int64 com literal float", "inteiro", `4096`, `inteiro = 4096.0`},
		{"double", "real", `10.5`, `real = 10.5`},
		// O literal INTEIRO sobre coluna double: os dois lados viram
		// Float64bits, e confundir isso devolveria vazio.
		{"double com literal inteiro", "real", `5.0`, `real = 5`},
		{"boolean", "booleano", `true`, `booleano = TRUE`},
		{"timestamp RFC3339", "quando", `"2026-08-13T12:00:00Z"`, `quando = '2026-08-13T12:00:00Z'`},
		// O MESMO instante escrito com offset: a query tem de achar o que a
		// ingestão gravou como Z. É o caso que quebra quando um dos lados
		// normaliza e o outro não.
		{"timestamp com offset", "quando", `"2026-08-13T12:00:00Z"`, `quando = '2026-08-13T09:00:00-03:00'`},
		{"timestamp epoch millis", "quando", `1786568400000`, `quando = 1786568400000`},
		{"ipv4", "v4", `"10.0.0.1"`, `v4 = '10.0.0.1'`},
		{"ipv6", "v6", `"2001:db8::1"`, `v6 = '2001:db8::1'`},
		// Forma expandida do MESMO endereço: os dois lados normalizam para os
		// mesmos 16 bytes.
		{"ipv6 expandido", "v6", `"2001:db8::1"`, `v6 = '2001:0db8:0000:0000:0000:0000:0000:0001'`},
		// Host bits PRESERVADOS dos dois lados: se um dos lados mascarasse,
		// 10.0.0.1/24 viraria 10.0.0.0/24 e a comparação daria vazio.
		{"ipv4_prefix com host bits", "rede4", `"10.0.0.1/24"`, `rede4 = '10.0.0.1/24'`},
		{"ipv6_prefix", "rede6", `"2001:db8::/32"`, `rede6 = '2001:db8::/32'`},

		// O texto que o produtor manda, escrito como literal, acha a row. É a
		// propriedade que justifica o literal usar o format DECLARADO do
		// campo em vez de RFC3339.
		{"datetime java medium", "momento", `"Aug 22, 2026 08:54:02 PM"`, `momento = 'Aug 22, 2026 08:54:02 PM'`},
		{"datetime iso local", "momento_iso", `"2026-08-22T20:54:02"`, `momento_iso = '2026-08-22T20:54:02'`},
		{"time java medium", "hora", `"08:54:02 PM"`, `hora = '08:54:02 PM'`},
		{"time iso com millis", "hora_iso", `"20:54:02.123"`, `hora_iso = '20:54:02.123'`},
		// A constante literal É o ponto: time não depende de fuso, então o
		// número é o mesmo em qualquer host. O caso equivalente para datetime
		// NÃO pode ser constante — está em TestDatetimePorEpochMillis.
		{"time por millis do dia", "hora", `"08:54:02 PM"`, `hora = 75242000`},
	}

	for _, tc := range casos {
		t.Run(tc.name, func(t *testing.T) {
			h := newTiposHarness(t)
			h.ingere(fmt.Sprintf(`{"id":"K1","_SEQ":1,%q:%s}`, tc.fieldJSON, tc.valueJSON))

			got := h.achaIDs(`SELECT id FROM todos_os_tipos WHERE ` + tc.where)
			if len(got) != 1 || got[0] != "K1" {
				t.Errorf("query não achou a row ingerida: %v\n  evento: %s = %s\n  where:  %s",
					got, tc.fieldJSON, tc.valueJSON, tc.where)
			}
		})
	}
}

// Os três modos numéricos de comparação (CompareInt, CompareFloatBits e
// CompareIntAsFloat) percorridos por TODOS os operadores. O modo errado não
// dá erro: dá resultado errado em silêncio, porque o payload de int64 guarda o
// inteiro cru e o de double guarda Float64bits — comparar um pelo outro
// devolve lixo ordenado.
func TestOperadoresNosTresModosNumericos(t *testing.T) {
	h := newTiposHarness(t)
	h.ingere(`{"id":"K1","_SEQ":1,"inteiro":100,"real":10.5}`)

	casos := []struct {
		where string
		acha  bool
	}{
		// double × literal float
		{`real = 10.5`, true}, {`real != 10.5`, false},
		{`real > 10.4`, true}, {`real > 10.6`, false},
		{`real >= 10.5`, true}, {`real < 10.6`, true},
		{`real <= 10.5`, true}, {`real < 10.4`, false},
		// double × literal inteiro (o literal também vira Float64bits)
		{`real > 10`, true}, {`real < 11`, true}, {`real = 10`, false},
		// int64 × literal float (CompareIntAsFloat: esquerda crua, direita bits)
		{`inteiro > 99.9`, true}, {`inteiro < 100.1`, true},
		{`inteiro = 100.0`, true}, {`inteiro != 100.0`, false},
		{`inteiro >= 100.0`, true}, {`inteiro <= 99.9`, false},
		// int64 × literal inteiro
		{`inteiro = 100`, true}, {`inteiro > 100`, false}, {`inteiro <= 100`, true},
	}
	for _, tc := range casos {
		t.Run(tc.where, func(t *testing.T) {
			got := h.achaIDs(`SELECT id FROM todos_os_tipos WHERE ` + tc.where)
			if achou := len(got) == 1; achou != tc.acha {
				t.Errorf("achou = %v, want %v (resultado: %v)", achou, tc.acha, got)
			}
		})
	}
}

// IEEE-754 inverte a ordem dos NEGATIVOS no espaço de bits: comparar
// Value.I64 diretamente faz `-1 > -2` devolver false. Os bits só servem para
// IGUALDADE; para ordenar é obrigatório converter com Float64frombits antes.
//
// Com valores positivos a ordem dos bits COINCIDE com a dos floats — por isso
// um teste que só use positivos passa com o código errado e não prova nada.
func TestOrdemDeDoubleNegativoNaoUsaBits(t *testing.T) {
	h := newTiposHarness(t)
	h.ingere(`{"id":"K1","_SEQ":1,"real":-1.0,"inteiro":-1}`)

	casos := []struct {
		where string
		acha  bool
	}{
		{`real > -2.0`, true}, // o caso que falha comparando bits
		{`real < -0.5`, true},
		{`real >= -1.0`, true},
		{`real <= -1.0`, true},
		{`real > -0.5`, false},
		{`real < -2.0`, false},
		// O mesmo pelo caminho CompareIntAsFloat (coluna int64, literal float).
		{`inteiro > -2.0`, true},
		{`inteiro < -0.5`, true},
		{`inteiro > -0.5`, false},
	}
	for _, tc := range casos {
		t.Run(tc.where, func(t *testing.T) {
			got := h.achaIDs(`SELECT id FROM todos_os_tipos WHERE ` + tc.where)
			if achou := len(got) == 1; achou != tc.acha {
				t.Errorf("achou = %v, want %v", achou, tc.acha)
			}
		})
	}
}

// O MESMO literal contra uma coluna int64 e uma coluna double produz um Val
// IDÊNTICO — mesmo Type (double) e mesmos bits. Só o CompareKind separa os
// dois casos. Um executor que despachasse por Val.Type varreria o dataset
// inteiro e devolveria vazio, sem erro nenhum.
func TestMesmoLiteralEmColunasDeTipoDiferente(t *testing.T) {
	h := newTiposHarness(t)
	h.ingere(`{"id":"K1","_SEQ":1,"inteiro":2,"real":2.0}`)

	// inteiro=2 > 1.5 (CompareIntAsFloat) e real=2.0 > 1.5 (CompareFloatBits).
	if got := h.achaIDs(`SELECT id FROM todos_os_tipos WHERE inteiro > 1.5`); len(got) != 1 {
		t.Errorf("coluna int64 com literal 1.5: %v", got)
	}
	if got := h.achaIDs(`SELECT id FROM todos_os_tipos WHERE real > 1.5`); len(got) != 1 {
		t.Errorf("coluna double com literal 1.5: %v", got)
	}
	// E o simétrico, onde o modo errado devolveria a row: a coluna int64 vale
	// 2, então > 2.5 é falso; se ela fosse lida como Float64bits, o inteiro 2
	// viraria ~1e-323 e a comparação também daria falso — o teste acima é o
	// que separa, porque lá o modo errado dá VAZIO onde deveria achar.
	if got := h.achaIDs(`SELECT id FROM todos_os_tipos WHERE inteiro > 2.5`); len(got) != 0 {
		t.Errorf("inteiro > 2.5 deveria ser vazio: %v", got)
	}
}

// bytes e json não comparam (o binder recusa com E4): a concordância dos dois
// lados é provada pela PROJEÇÃO — o valor projetado é byte a byte o ingerido.
func TestIngestaoEQueryConcordamNosTiposOpacos(t *testing.T) {
	h := newTiposHarness(t)
	// "cru" é base64 na ingestão; o valor gravado são os bytes decodificados.
	h.ingere(`{"id":"K1","_SEQ":1,"cru":"aGVsbG8=","doc":{"a":[1,2],"b":"txt"}}`)

	it, err := h.engine.Execute(context.Background(), "todos_os_tipos",
		`SELECT cru, doc FROM todos_os_tipos WHERE id = 'K1'`, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	if !it.Next() {
		t.Fatalf("row não saiu: %v", it.Err())
	}
	raw, doc := it.Row().Col(0), it.Row().Col(1)
	if string(raw.Val.Bytes) != "hello" {
		t.Errorf("bytes = %q, want os bytes decodificados do base64", raw.Val.Bytes)
	}
	if raw.Val.Type != types.TypeBytes {
		t.Errorf("tipo físico de bytes = %v", raw.Val.Type)
	}
	// O json é gravado VERBATIM (a janela do evento), e é assim que o encoder
	// o emite.
	if string(doc.Val.Bytes) != `{"a":[1,2],"b":"txt"}` {
		t.Errorf("json = %q, want verbatim", doc.Val.Bytes)
	}

	// E a comparação sobre eles é recusada no BIND, não no executor.
	_, err = h.engine.Execute(context.Background(), "todos_os_tipos",
		`SELECT id FROM todos_os_tipos WHERE doc = 'x'`, nil)
	if err == nil {
		t.Fatal("comparação sobre json deveria falhar no bind")
	}
}

// O valor inválido é recusado nos DOIS lados, e essa simetria é o que garante
// que não existe valor "ingerível mas não consultável" (nem o contrário).
func TestValorInvalidoEhRecusadoNosDoisLados(t *testing.T) {
	casos := []struct {
		name      string
		fieldJSON string
		value     string // literal, sem aspas JSON
		where     string
	}{
		// Zeros à esquerda em ipv4: ambíguo (octal?), recusado na ingestão e
		// no bind.
		{"ipv4 com zeros à esquerda", "v4", "010.0.0.1", `v4 = '010.0.0.1'`},
		{"ipv4 fora de faixa", "v4", "10.0.0.256", `v4 = '10.0.0.256'`},
		{"ipv6 com zona", "v6", "fe80::1%eth0", `v6 = 'fe80::1%eth0'`},
		{"timestamp sem timezone", "quando", "2026-08-13T12:00:00", `quando = '2026-08-13T12:00:00'`},
	}
	for _, tc := range casos {
		t.Run(tc.name, func(t *testing.T) {
			h := newTiposHarness(t)

			r := h.mat.Apply([]byte(fmt.Sprintf(`{"id":"K1","_SEQ":1,%q:%q}`, tc.fieldJSON, tc.value)))
			if r.Err == nil {
				t.Errorf("a INGESTÃO aceitou %q", tc.value)
			}

			_, err := h.engine.Execute(context.Background(), "todos_os_tipos",
				`SELECT id FROM todos_os_tipos WHERE `+tc.where, nil)
			if err == nil {
				t.Errorf("a QUERY aceitou %q", tc.value)
			}
		})
	}
}

// Parâmetro e literal têm de produzir o MESMO valor: são dois caminhos de
// coerção, e divergir entre eles faz a mesma query dar resultados diferentes
// conforme o cliente use `?` ou embuta o valor.
func TestParametroELiteralConcordam(t *testing.T) {
	h := newTiposHarness(t)
	h.ingere(`{"id":"K1","_SEQ":1,"v4":"10.0.0.1","quando":"2026-08-13T12:00:00Z","real":10.5}`)

	casos := []struct {
		where string
		param types.Value
	}{
		{`v4 = ?`, types.Value{Type: types.TypeString, Bytes: []byte("10.0.0.1")}},
		{`quando = ?`, types.Value{Type: types.TypeString, Bytes: []byte("2026-08-13T12:00:00Z")}},
		{`real = ?`, types.Value{Type: types.TypeDouble, I64: int64(0x4025000000000000)}}, // 10.5
	}
	for _, tc := range casos {
		t.Run(tc.where, func(t *testing.T) {
			sql := `SELECT id FROM todos_os_tipos WHERE ` + tc.where
			it, err := h.engine.Execute(context.Background(), "todos_os_tipos", sql,
				[]types.Value{tc.param})
			if err != nil {
				t.Fatalf("com parâmetro: %v", err)
			}
			defer it.Close()
			if !it.Next() {
				t.Errorf("parâmetro não achou a row (%v)", it.Err())
			}
		})
	}
}

// A ingestão aceita string numérica para coluna int64 (o Connect-Info da base
// real vem assim); a query NÃO aceita literal string para coluna int64. A
// assimetria é DELIBERADA — a ingestão tolera a origem suja, a query não
// adivinha — e este teste existe para que ela seja uma decisão registrada, e
// não uma descoberta de produção.
func TestAssimetriaDeliberadaEntreIngestaoEQuery(t *testing.T) {
	h := newTiposHarness(t)
	h.ingere(`{"id":"K1","_SEQ":1,"inteiro":"4096"}`)

	if got := h.achaIDs(`SELECT id FROM todos_os_tipos WHERE inteiro = 4096`); len(got) != 1 {
		t.Fatalf("a string numérica não foi convertida na ingestão: %v", got)
	}
	_, err := h.engine.Execute(context.Background(), "todos_os_tipos",
		`SELECT id FROM todos_os_tipos WHERE inteiro = '4096'`, nil)
	if err == nil {
		t.Fatal("a query aceitou literal string para coluna int64")
	}
	if !strings.Contains(err.Error(), "não é comparável") {
		t.Errorf("erro = %q", err)
	}
}

// O caso de epoch millis do datetime NÃO pode ser constante na tabela acima:
// o instante depende do time.Local do host que roda o teste. Montá-lo em
// runtime é o que torna o teste correto em qualquer fuso de CI — uma
// constante passaria só na máquina onde foi escrita.
func TestDatetimePorEpochMillisConcordaComOTextoIngerido(t *testing.T) {
	h := newTiposHarness(t)
	h.ingere(`{"id":"K1","_SEQ":1,"momento":"Aug 22, 2026 08:54:02 PM"}`)

	ms := time.Date(2026, 8, 22, 20, 54, 2, 0, time.Local).UnixMilli()
	got := h.achaIDs(fmt.Sprintf(`SELECT id FROM todos_os_tipos WHERE momento = %d`, ms))
	if len(got) != 1 || got[0] != "K1" {
		t.Fatalf("epoch millis não achou a row ingerida pelo texto: %v (millis=%d, TZ=%s)",
			got, ms, time.Local)
	}
}

// A falha que este bloco inteiro existe para impedir: literal numa grafia que
// o campo não declara tem de ser ERRO DE BIND, nunca resultado vazio. Vazio
// parece resposta, e quem escreveu a query concluiria que não há dado.
func TestLiteralNoFormatoErradoNaoAchaEmSilencio(t *testing.T) {
	h := newTiposHarness(t)
	h.ingere(`{"id":"K1","_SEQ":1,"momento":"Aug 22, 2026 08:54:02 PM","hora":"08:54:02 PM"}`)

	casos := []struct{ name, where string }{
		{"rfc3339 em campo java medium", `momento = '2026-08-22T20:54:02Z'`},
		{"iso local em campo java medium", `momento_iso = 'Aug 22, 2026 08:54:02 PM'`},
		{"texto de hora em campo de data", `momento = '08:54:02 PM'`},
		{"epoch millis em campo time", `hora = 1786568400000`},
	}
	for _, tc := range casos {
		t.Run(tc.name, func(t *testing.T) {
			_, err := h.engine.Execute(context.Background(), "todos_os_tipos",
				`SELECT id FROM todos_os_tipos WHERE `+tc.where, nil)
			if err == nil {
				t.Fatalf("literal em grafia inválida foi aceito: %s", tc.where)
			}
		})
	}
}

// A mensagem precisa dizer em que grafia escrever: "literal inválido para a
// coluna momento (datetime)" sozinho não é acionável.
func TestMensagemDeLiteralInvalidoNomeiaOFormatDeclarado(t *testing.T) {
	h := newTiposHarness(t)
	_, err := h.engine.Execute(context.Background(), "todos_os_tipos",
		`SELECT id FROM todos_os_tipos WHERE momento = '2026-08-22T20:54:02Z'`, nil)
	if err == nil {
		t.Fatal("esperava erro de bind")
	}
	if !strings.Contains(err.Error(), "MMM d, yyyy hh:mm:ss a") {
		t.Fatalf("a mensagem não nomeia o format declarado: %v", err)
	}
}

// datetime e time são fisicamente 8 bytes como o timestamp, mas o predicado
// exige igualdade EXATA de tag: um valor gravado com uma tag não casa com
// coluna declarada com outra. É o que torna o retipo de um campo existente
// uma operação hostil, e o teste fixa esse comportamento em vez de deixá-lo
// implícito.
func TestColunasTemporaisNaoSeConfundemEntreSi(t *testing.T) {
	h := newTiposHarness(t)
	// Mesmo instante de relógio nos três campos.
	h.ingere(`{"id":"K1","_SEQ":1,"quando":"2026-08-22T20:54:02Z",` +
		`"momento":"Aug 22, 2026 08:54:02 PM","hora":"08:54:02 PM"}`)

	// A hora do dia como millis não pode achar nada em quando nem em momento.
	if got := h.achaIDs(`SELECT id FROM todos_os_tipos WHERE quando = 75242000`); len(got) != 0 {
		t.Errorf("millis do dia achou row numa coluna timestamp: %v", got)
	}
}
