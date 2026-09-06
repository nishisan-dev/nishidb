package materializer

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/jsonp"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// mustNew monta o materializer exigindo sucesso. O New falha quando uma regra
// de TTL não compila, e nesses testes a regra é sempre válida — o caso de
// recusa vive em deadline_test.go.
func mustNew(t *testing.T, c *schema.Compiled, st Store) *Materializer {
	t.Helper()
	m, err := New(c, st)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return m
}

// harness amarra o trio schema compilado + store fake + materializer, que é
// como o writer roda em produção.
type harness struct {
	t  *testing.T
	m  *Materializer
	st *fakestorage.Store
	c  *schema.Compiled
}

// newHarness compila o YAML, cria o fake com índice bitmap nos campos pedidos
// (por NOME — os IDs só existem depois do Compile) e monta o materializer.
func newHarness(t *testing.T, yml string, bitmapFields ...string) *harness {
	t.Helper()
	c := compileYAML(t, yml)
	var idx fakestorage.Indexes
	for _, name := range bitmapFields {
		f, ok := c.FieldByName([]byte(name))
		if !ok {
			t.Fatalf("campo %q não compilado", name)
		}
		idx.Campos = append(idx.Campos, f.ID)
	}
	st := fakestorage.New(idx)
	return &harness{t: t, m: mustNew(t, c, st), st: st, c: c}
}

func (h *harness) fieldID(name string) types.FieldID {
	h.t.Helper()
	f, ok := h.c.FieldByName([]byte(name))
	if !ok {
		h.t.Fatalf("campo %q não compilado", name)
	}
	return f.ID
}

// apply aplica o evento exigindo sucesso e devolve o resultado.
func (h *harness) apply(event string) ApplyResult {
	h.t.Helper()
	r := h.m.Apply([]byte(event))
	if r.Err != nil {
		h.t.Fatalf("Apply(%s): %v", event, r.Err)
	}
	return r
}

// state devolve o estado materializado da key, indexado por NOME de campo —
// a visão que o executor terá.
func (h *harness) state(key string) map[string]types.Value {
	h.t.Helper()
	rec, _, ok, err := h.st.GetByKey(h.st.AbreSnapshot(), []byte(key), nil)
	if err != nil {
		h.t.Fatal(err)
	}
	if !ok {
		h.t.Fatalf("key %q ausente do store", key)
	}
	out := map[string]types.Value{}
	var r encoding.FieldReader
	r.Reset(rec.Payload)
	for {
		id, v, ok := r.Next()
		if !ok {
			if err := r.Err(); err != nil {
				h.t.Fatalf("payload gravado corrompido: %v", err)
			}
			return out
		}
		f, known := h.c.FieldByID(id)
		if !known {
			h.t.Fatalf("payload tem campo %d que o schema não conhece", id)
		}
		out[f.Name] = v
	}
}

func (h *harness) wantString(state map[string]types.Value, name, want string) {
	h.t.Helper()
	v, ok := state[name]
	if !ok {
		h.t.Errorf("campo %q ausente do estado", name)
		return
	}
	if v.Type != types.TypeString || string(v.Bytes) != want {
		h.t.Errorf("campo %q = %+v, want string %q", name, v, want)
	}
}

func (h *harness) wantInt(state map[string]types.Value, name string, want int64) {
	h.t.Helper()
	v, ok := state[name]
	if !ok {
		h.t.Errorf("campo %q ausente do estado", name)
		return
	}
	if v.Type != types.TypeInt64 || v.I64 != want {
		h.t.Errorf("campo %q = %+v, want int64 %d", name, v, want)
	}
}

func (h *harness) wantAbsent(state map[string]types.Value, name string) {
	h.t.Helper()
	if v, ok := state[name]; ok {
		h.t.Errorf("campo %q deveria estar ausente, got %+v", name, v)
	}
}

// lastPut devolve a última chamada de Put registrada pelo fake.
func (h *harness) lastPut() fakestorage.PutCall {
	h.t.Helper()
	if len(h.st.PutCalls) == 0 {
		h.t.Fatal("nenhum Put registrado")
	}
	return h.st.PutCalls[len(h.st.PutCalls)-1]
}

// bitmapRows lista as rows que o índice bitmap devolve para o valor.
func (h *harness) bitmapRows(name string, v types.Value) []types.RowID {
	h.t.Helper()
	it, err := h.st.Lookup(h.fieldID(name), v)
	if err != nil {
		h.t.Fatal(err)
	}
	defer it.Close()
	var out []types.RowID
	for it.Next() {
		out = append(out, it.RowID())
	}
	if err := it.Err(); err != nil {
		h.t.Fatal(err)
	}
	return out
}

// ---------------------------------------------------------------- create/merge

func TestApplyCreate(t *testing.T) {
	h := newHarness(t, patchYAML)
	r := h.apply(`{"id":"CMP-001","_SEQ":100,"Acct-Status-Type":"Start","Acct-Input-Octets":10}`)

	if r.Action != ActionCreated {
		t.Errorf("Action = %v, want Created", r.Action)
	}
	st := h.state("CMP-001")
	h.wantString(st, "id", "CMP-001")
	h.wantInt(st, "_SEQ", 100)
	h.wantString(st, "Acct-Status-Type", "Start")
	h.wantInt(st, "Acct-Input-Octets", 10)

	// Key nova não tem estado anterior a repassar, e a sequence do evento vai
	// para o record (é dela que o stale check do próximo evento vive).
	put := h.lastPut()
	if put.HadPrev {
		t.Error("create não pode passar prev ao Put")
	}
	if put.Seq != 100 || put.Flags != 0 {
		t.Errorf("Put = %+v, want seq 100 e flags 0", put)
	}

	mt := h.m.Metrics()
	if mt.EventsTotal != 1 || mt.CreatedTotal != 1 || mt.UpdatedTotal != 0 {
		t.Errorf("métricas = %+v", mt)
	}
}

// §22: o patch parcial mescla no estado, o campo ausente sobrevive e o rowId
// da entidade NÃO muda — é o que garante que os índices continuam apontando
// para a mesma row.
func TestApplyMergeMantemRowIDEPreservaCampos(t *testing.T) {
	h := newHarness(t, patchYAML)
	created := h.apply(`{"id":"CMP-001","_SEQ":100,"Acct-Status-Type":"Start","Acct-Input-Octets":10}`)
	updated := h.apply(`{"id":"CMP-001","_SEQ":101,"Acct-Input-Octets":99}`)

	if updated.Action != ActionUpdated {
		t.Errorf("Action = %v, want Updated", updated.Action)
	}
	if updated.RowID != created.RowID {
		t.Errorf("rowId mudou no update: %d → %d", created.RowID, updated.RowID)
	}
	st := h.state("CMP-001")
	h.wantInt(st, "Acct-Input-Octets", 99)
	h.wantInt(st, "_SEQ", 101)
	h.wantString(st, "Acct-Status-Type", "Start") // ausente do patch: preservado

	// O estado corrente já foi lido para o merge: repassá-lo ao Put evita um
	// segundo lookup por evento e é o que dá ao Put a base do diff de índices.
	if put := h.lastPut(); !put.HadPrev {
		t.Error("update precisa repassar prev ao Put")
	}

	mt := h.m.Metrics()
	if mt.EventsTotal != 2 || mt.CreatedTotal != 1 || mt.UpdatedTotal != 1 {
		t.Errorf("métricas = %+v", mt)
	}
}

// §24: null explícito remove o campo do estado, fim a fim.
func TestApplyNullRemoveCampo(t *testing.T) {
	h := newHarness(t, patchYAML)
	h.apply(`{"id":"CMP-001","_SEQ":100,"Acct-Status-Type":"Start","Acct-Input-Octets":10}`)
	h.apply(`{"id":"CMP-001","_SEQ":101,"Acct-Status-Type":null}`)

	st := h.state("CMP-001")
	h.wantAbsent(st, "Acct-Status-Type")
	h.wantInt(st, "Acct-Input-Octets", 10) // não mencionado: intacto
}

const replaceYAML = `
version: 1
name: radius_replace
materialization:
  mode: replace
  key:
    field: id
  sequence:
    field: _SEQ
schema:
  include:
    - "*"
  unknownFields: include
  fields:
    id:
      type: string
    _SEQ:
      type: int64
    Acct-Status-Type:
      type: string
      index: true
    Acct-Input-Octets:
      type: int64
`

// §23: o replace descarta o estado anterior inteiro do PAYLOAD. A posting do
// valor antigo NÃO é limpa — não há fase de Clear (spec do subsistema 1, §5.2):
// ela fica no índice do segmento em que nasceu como candidata a mais, que a
// revalidação do §44 descarta. O que este teste prova é o par: estado trocado
// por inteiro, e prev entregue ao Put mesmo assim (é dele que o storage
// resolve o rowId sem refazer o Find).
func TestApplyReplaceDescartaOEstadoEAPostingAntigaSobrevive(t *testing.T) {
	h := newHarness(t, replaceYAML, "Acct-Status-Type")
	created := h.apply(`{"id":"CMP-001","_SEQ":100,"Acct-Status-Type":"Start","Acct-Input-Octets":10}`)

	start := types.Value{Type: types.TypeString, Bytes: []byte("Start")}
	if rows := h.bitmapRows("Acct-Status-Type", start); len(rows) != 1 || rows[0] != created.RowID {
		t.Fatalf("postings de Start = %v, want [%d]", rows, created.RowID)
	}

	r := h.apply(`{"id":"CMP-001","_SEQ":101,"Acct-Status-Type":"Stop"}`)
	if r.Action != ActionReplaced {
		t.Errorf("Action = %v, want Replaced", r.Action)
	}
	if r.RowID != created.RowID {
		t.Errorf("rowId mudou no replace: %d → %d", created.RowID, r.RowID)
	}
	st := h.state("CMP-001")
	h.wantString(st, "Acct-Status-Type", "Stop")
	h.wantAbsent(st, "Acct-Input-Octets") // estado anterior descartado

	// No replace o estado anterior é descartado do PAYLOAD, mas o prev vai
	// para o Put assim mesmo: é dele que o storage resolve o rowId da linha
	// sem refazer o Find.
	if put := h.lastPut(); !put.HadPrev {
		t.Error("replace precisa repassar prev ao Put")
	}

	// A posting do valor ANTIGO sobrevive, e isso deixou de ser defeito: o
	// índice por segmento não tem fase de CLEAR (spec §5.2), porque a posting
	// mora no índice do segmento em que o record está e morre quando aquele
	// segmento é aposentado. Ela é candidato a mais, que a revalidação do §44
	// descarta contra o payload corrente — e o payload corrente já foi conferido
	// acima, com o wantString/wantAbsent.
	if rows := h.bitmapRows("Acct-Status-Type", start); len(rows) != 1 || rows[0] != created.RowID {
		t.Errorf("postings de Start = %v, want [%d]: candidato tolerado, não resultado", rows, created.RowID)
	}
	stop := types.Value{Type: types.TypeString, Bytes: []byte("Stop")}
	if rows := h.bitmapRows("Acct-Status-Type", stop); len(rows) != 1 || rows[0] != created.RowID {
		t.Errorf("postings de Stop = %v, want [%d]", rows, created.RowID)
	}

	mt := h.m.Metrics()
	if mt.ReplacedTotal != 1 || mt.UpdatedTotal != 0 {
		t.Errorf("métricas = %+v", mt)
	}
}

// Replace de key NOVA é create, não replace: o Action vem do Put.
func TestApplyReplaceDeKeyNovaEhCreate(t *testing.T) {
	h := newHarness(t, replaceYAML)
	r := h.apply(`{"id":"NOVA","_SEQ":1,"Acct-Status-Type":"Start"}`)
	if r.Action != ActionCreated {
		t.Errorf("Action = %v, want Created", r.Action)
	}
}

// ---------------------------------------------------------------- sequence §25

func TestApplySequenceGreaterAplica(t *testing.T) {
	h := newHarness(t, patchYAML)
	h.apply(`{"id":"CMP-001","_SEQ":100,"Acct-Input-Octets":10}`)
	r := h.apply(`{"id":"CMP-001","_SEQ":101,"Acct-Input-Octets":20}`)
	if r.Action != ActionUpdated {
		t.Fatalf("Action = %v, want Updated", r.Action)
	}
	h.wantInt(h.state("CMP-001"), "Acct-Input-Octets", 20)
}

func TestApplySequenceEqualEMenorDescartam(t *testing.T) {
	for _, tc := range []struct {
		nome string
		seq  int64
	}{
		{"equal", 100},
		{"lower", 99},
	} {
		t.Run(tc.nome, func(t *testing.T) {
			h := newHarness(t, patchYAML)
			created := h.apply(`{"id":"CMP-001","_SEQ":100,"Acct-Input-Octets":10}`)
			putsAntes := len(h.st.PutCalls)

			r := h.apply(fmt.Sprintf(`{"id":"CMP-001","_SEQ":%d,"Acct-Input-Octets":999}`, tc.seq))
			if r.Action != ActionStale {
				t.Errorf("Action = %v, want Stale", r.Action)
			}
			if r.RowID != created.RowID {
				t.Errorf("RowID = %d, want %d (a row do estado corrente)", r.RowID, created.RowID)
			}
			// Descarte é SEM efeito colateral: nenhuma escrita chegou ao store.
			if len(h.st.PutCalls) != putsAntes {
				t.Errorf("evento stale gerou Put: %+v", h.st.PutCalls[putsAntes:])
			}
			h.wantInt(h.state("CMP-001"), "Acct-Input-Octets", 10)
			if mt := h.m.Metrics(); mt.StaleTotal != 1 || mt.EventsTotal != 2 {
				t.Errorf("métricas = %+v", mt)
			}
		})
	}
}

const semSequenceYAML = `
version: 1
name: sem_sequence
materialization:
  key:
    field: id
schema:
  include:
    - "*"
  unknownFields: include
  fields:
    id:
      type: string
    counter:
      type: int64
`

// Sem sequence configurada vale a ordem de chegada (§26): nada é stale, nem
// mesmo um evento que "volta no tempo".
func TestApplySemSequenceAplicaTudo(t *testing.T) {
	h := newHarness(t, semSequenceYAML)
	h.apply(`{"id":"A","counter":10}`)
	r := h.apply(`{"id":"A","counter":5}`)
	if r.Action != ActionUpdated {
		t.Fatalf("Action = %v, want Updated", r.Action)
	}
	h.wantInt(h.state("A"), "counter", 5)
	if mt := h.m.Metrics(); mt.StaleTotal != 0 {
		t.Errorf("StaleTotal = %d, want 0", mt.StaleTotal)
	}
}

// ---------------------------------------------------------------- campo desconhecido

const unknownYAMLTmpl = `
version: 1
name: unknown_%s
materialization:
  key:
    field: id
schema:
  include:
    - "*"
  unknownFields: %s
  fields:
    id:
      type: string
`

func TestApplyUnknownFieldPolicies(t *testing.T) {
	event := `{"id":"A","novo":"valor"}`

	t.Run("include", func(t *testing.T) {
		h := newHarness(t, fmt.Sprintf(unknownYAMLTmpl, "include", "include"))
		h.apply(event)
		h.wantString(h.state("A"), "novo", "valor")
	})

	t.Run("ignore", func(t *testing.T) {
		h := newHarness(t, fmt.Sprintf(unknownYAMLTmpl, "ignore", "ignore"))
		h.apply(event)
		h.wantAbsent(h.state("A"), "novo")
	})

	t.Run("reject", func(t *testing.T) {
		h := newHarness(t, fmt.Sprintf(unknownYAMLTmpl, "reject", "reject"))
		r := h.m.Apply([]byte(event))
		if r.Action != ActionSkipped || r.Category != CategorySchemaValidation {
			t.Fatalf("resultado = %+v, want Skipped/SCHEMA_VALIDATION", r)
		}
		if !errors.Is(r.Err, schema.ErrUnknownField) {
			t.Errorf("erro = %v, want schema.ErrUnknownField", r.Err)
		}
		if len(h.st.PutCalls) != 0 {
			t.Errorf("evento rejeitado gerou Put: %+v", h.st.PutCalls)
		}
	})
}

// Campo excluído no YAML não entra no estado nem sob wildcard.
func TestApplyExcludeEWildcard(t *testing.T) {
	h := newHarness(t, patchYAML)
	h.apply(`{"id":"A","_SEQ":1,"debug":"segredo","RemoteServerAddr":"10.0.0.9"}`)
	st := h.state("A")
	h.wantAbsent(st, "debug")                        // exclude
	h.wantString(st, "RemoteServerAddr", "10.0.0.9") // wildcard + unknown include
}

// §103 caso 12 + §19: os wrappers do Mongo Extended JSON são forma de
// TRANSPORTE. O que a spec proíbe é ARMAZENÁ-LOS — e só um teste que chegue
// ao payload gravado prova isso; parar no PatchBuffer deixaria de fora
// justamente o passo em questão.
func TestApplyMongoExtendedJSONNaoArmazenaWrapper(t *testing.T) {
	h := newHarness(t, patchYAML)
	r := h.apply(`{
		"id":"M-1",
		"_SEQ":{"$numberLong":"7455409733"},
		"Event-Timestamp":{"$date":"2026-05-01T10:00:00Z"},
		"Acct-Input-Octets":{"$numberLong":"5"}
	}`)
	if r.Action != ActionCreated {
		t.Fatalf("Action = %v, want Created", r.Action)
	}

	st := h.state("M-1")
	// Os valores chegam como tipos binários nativos: nem string com o JSON do
	// wrapper, nem json opaco.
	h.wantInt(st, "_SEQ", 7455409733)
	h.wantInt(st, "Acct-Input-Octets", 5)
	ts, ok := st["Event-Timestamp"]
	if !ok {
		t.Fatal("Event-Timestamp ausente do estado")
	}
	want, err := time.Parse(time.RFC3339, "2026-05-01T10:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	if ts.Type != types.TypeTimestamp || ts.I64 != want.UnixMilli() {
		t.Errorf("Event-Timestamp = %+v, want timestamp %d", ts, want.UnixMilli())
	}
	// Nenhum campo do estado pode carregar o texto do wrapper.
	for name, v := range st {
		if strings.Contains(string(v.Bytes), "$numberLong") || strings.Contains(string(v.Bytes), "$date") {
			t.Errorf("campo %q armazenou o wrapper: %q", name, v.Bytes)
		}
	}

	// O seq extraído do $numberLong é o MESMO que alimenta o stale check:
	// reenviar o evento é descartado.
	if again := h.m.Apply([]byte(`{"id":"M-1","_SEQ":{"$numberLong":"7455409733"},"Acct-Input-Octets":9}`)); again.Action != ActionStale {
		t.Fatalf("Action = %v, want Stale (o $numberLong precisa alimentar o sequence check)", again.Action)
	}
	h.wantInt(h.state("M-1"), "Acct-Input-Octets", 5)

	// Shape canonical do $date (millis em $numberLong) segue o mesmo caminho.
	h.apply(`{"id":"M-2","_SEQ":1,"Event-Timestamp":{"$date":{"$numberLong":"1786399690000"}}}`)
	ts2 := h.state("M-2")["Event-Timestamp"]
	if ts2.Type != types.TypeTimestamp || ts2.I64 != 1786399690000 {
		t.Errorf("Event-Timestamp canonical = %+v, want 1786399690000", ts2)
	}
}

const includeExplicitoYAML = `
version: 1
name: include_explicito
materialization:
  key:
    field: id
schema:
  include:
    - id
    - a
  fields:
    id:
      type: string
    a:
      type: int64
    b:
      type: int64
`

// §13/§14: com include EXPLÍCITO o universo é fechado — um campo DECLARADO em
// fields: mas fora do include: não materializa. É o que separa "declarei o
// tipo" de "quero o campo", e sem o teste a diferença entre o wildcard e a
// lista explícita some.
//
// (A spec já impede a combinação oposta: unknownFields: include com lista
// explícita é recusado no Compile, porque seria universo fechado e aberto ao
// mesmo tempo.)
func TestApplyIncludeExplicitoFechaOUniverso(t *testing.T) {
	h := newHarness(t, includeExplicitoYAML)
	h.apply(`{"id":"X","a":1,"b":2,"c":3}`)

	st := h.state("X")
	h.wantInt(st, "a", 1)
	h.wantAbsent(st, "b") // declarado, mas fora do include
	h.wantAbsent(st, "c") // desconhecido, e o universo está fechado
	if n := len(st); n != 2 {
		t.Errorf("estado com %d campos: %+v (esperado só id e a)", n, st)
	}
}

// ---------------------------------------------------------------- erros §101

func TestApplyCategoriasDeEventoRuim(t *testing.T) {
	cases := []struct {
		nome  string
		yaml  string
		event string
		cat   ErrorCategory
		is    error
		msg   string
	}{
		{
			nome:  "invalid_json",
			yaml:  patchYAML,
			event: `{"id":"A", "_SEQ":`,
			cat:   CategoryInvalidJSON,
			is:    jsonp.ErrInvalidJSON,
			// A posição do erro é o que torna o log acionável num evento de
			// 2 KB; sem ela o operador recebe "json inválido" e nada mais.
			msg: "posição",
		},
		{
			nome:  "schema_validation",
			yaml:  fmt.Sprintf(unknownYAMLTmpl, "reject", "reject"),
			event: `{"id":"A","novo":1}`,
			cat:   CategorySchemaValidation,
			is:    schema.ErrUnknownField,
			msg:   "novo",
		},
		{
			nome:  "invalid_key_ausente",
			yaml:  patchYAML,
			event: `{"_SEQ":1,"Acct-Input-Octets":3}`,
			cat:   CategoryInvalidKey,
			is:    ErrInvalidKey,
			msg:   "ausente no evento",
		},
		{
			nome:  "invalid_key_nula",
			yaml:  patchYAML,
			event: `{"id":null,"_SEQ":1}`,
			cat:   CategoryInvalidKey,
			is:    ErrInvalidKey,
			msg:   "nulo",
		},
		{
			nome:  "invalid_key_vazia",
			yaml:  patchYAML,
			event: `{"id":"","_SEQ":1}`,
			cat:   CategoryInvalidKey,
			is:    ErrInvalidKey,
			msg:   "vazio",
		},
		{
			nome:  "invalid_sequence_ausente",
			yaml:  patchYAML,
			event: `{"id":"A","Acct-Input-Octets":3}`,
			cat:   CategoryInvalidSequence,
			is:    ErrInvalidSequence,
			msg:   "ausente no evento",
		},
		{
			nome:  "invalid_sequence_nula",
			yaml:  patchYAML,
			event: `{"id":"A","_SEQ":null}`,
			cat:   CategoryInvalidSequence,
			is:    ErrInvalidSequence,
			msg:   "nulo",
		},
		{
			nome:  "type_conversion",
			yaml:  patchYAML,
			event: `{"id":"A","_SEQ":1,"Acct-Input-Octets":"não é número"}`,
			cat:   CategoryTypeConversion,
			is:    ErrTypeConversion,
			msg:   "Acct-Input-Octets",
		},
	}

	for _, tc := range cases {
		t.Run(tc.nome, func(t *testing.T) {
			h := newHarness(t, tc.yaml)
			r := h.m.Apply([]byte(tc.event))

			// Evento ruim é Skipped: a ingestão segue no próximo evento.
			if r.Action != ActionSkipped {
				t.Errorf("Action = %v, want Skipped", r.Action)
			}
			if r.Category != tc.cat {
				t.Errorf("Category = %v, want %v", r.Category, tc.cat)
			}
			if !errors.Is(r.Err, tc.is) {
				t.Errorf("erro = %v, want errors.Is %v", r.Err, tc.is)
			}
			if tc.msg != "" && !strings.Contains(r.Err.Error(), tc.msg) {
				t.Errorf("mensagem = %q, want conter %q", r.Err, tc.msg)
			}
			if r.Fatal() {
				t.Error("evento ruim não pode ser fatal para a ingestão")
			}
			if len(h.st.PutCalls) != 0 {
				t.Errorf("evento inválido gerou Put: %+v", h.st.PutCalls)
			}
			mt := h.m.Metrics()
			if mt.InvalidTotal != 1 || mt.ByCategory[tc.cat] != 1 || mt.FailedTotal != 0 {
				t.Errorf("métricas = %+v", mt)
			}
		})
	}
}

// Evento ruim não contamina o próximo: os buffers do PatchBuffer ficam em
// estado PARCIAL depois do erro e o Parse seguinte precisa resetá-los.
func TestApplyContinuaAposEventoRuim(t *testing.T) {
	h := newHarness(t, patchYAML)
	if r := h.m.Apply([]byte(`{"id":"A","_SEQ":1,"Acct-Input-Octets":"xxx"}`)); r.Action != ActionSkipped {
		t.Fatalf("Action = %v, want Skipped", r.Action)
	}
	r := h.apply(`{"id":"B","_SEQ":2,"Acct-Input-Octets":7}`)
	if r.Action != ActionCreated {
		t.Fatalf("Action = %v, want Created", r.Action)
	}
	st := h.state("B")
	h.wantInt(st, "Acct-Input-Octets", 7)
	h.wantString(st, "id", "B")
	if n := len(st); n != 3 {
		t.Errorf("estado com %d campos: %+v (resíduo do evento anterior?)", n, st)
	}
	if mt := h.m.Metrics(); mt.EventsTotal != 2 || mt.InvalidTotal != 1 || mt.CreatedTotal != 1 {
		t.Errorf("métricas = %+v", mt)
	}
}

// storeErro injeta erro no Put mantendo o resto do fake intacto.
type storeErro struct {
	*fakestorage.Store
	put error
	get error
}

func (s storeErro) Put(key []byte, seq int64, flags uint8, payload []byte, prev *types.Record) (types.RowID, bool, error) {
	if s.put != nil {
		return 0, false, s.put
	}
	return s.Store.Put(key, seq, flags, payload, prev)
}

func (s storeErro) Estado(key, buf []byte) (types.Record, []byte, types.Presence, error) {
	if s.get != nil {
		return types.Record{}, buf, types.PresenceAbsent, s.get
	}
	return s.Store.Estado(key, buf)
}

func (s storeErro) GetByKey(snap types.Snapshot, key, buf []byte) (types.Record, []byte, bool, error) {
	if s.get != nil {
		return types.Record{}, buf, false, s.get
	}
	return s.Store.GetByKey(snap, key, buf)
}

// Writer em modo falho (§14.14) é o único erro TERMINAL: reprocessar o evento
// é inútil até o dataset reabrir, então o Apply marca Fatal e o caller para.
func TestApplyWriterFailedEhFatal(t *testing.T) {
	c := compileYAML(t, patchYAML)
	st := storeErro{Store: fakestorage.New(fakestorage.Indexes{}),
		put: fmt.Errorf("%w: fdatasync do segmento", types.ErrWriterFailed)}
	m := mustNew(t, c, st)

	r := m.Apply([]byte(`{"id":"A","_SEQ":1,"Acct-Input-Octets":3}`))
	if r.Action != ActionFailed {
		t.Errorf("Action = %v, want Failed", r.Action)
	}
	if r.Category != CategoryWriterFailed {
		t.Errorf("Category = %v, want WRITER_FAILED", r.Category)
	}
	if !errors.Is(r.Err, types.ErrWriterFailed) {
		t.Errorf("erro = %v, want types.ErrWriterFailed", r.Err)
	}
	if !r.Fatal() {
		t.Error("writer em modo falho precisa ser fatal: retry do mesmo evento nunca vai passar")
	}
	mt := m.Metrics()
	if mt.FailedTotal != 1 || mt.InvalidTotal != 0 || mt.ByCategory[CategoryWriterFailed] != 1 {
		t.Errorf("métricas = %+v", mt)
	}
}

// Erro do Store que NÃO é modo falho indica bug nosso (payload inválido, prev
// da key errada): conta em categoria SEPARADA da de evento ruim, senão o
// defeito se esconde na métrica de fonte ruidosa.
func TestApplyErroDeStorageNaoEhEventoRuim(t *testing.T) {
	c := compileYAML(t, patchYAML)
	st := storeErro{Store: fakestorage.New(fakestorage.Indexes{}),
		put: fmt.Errorf("%w: key vazia", types.ErrInvalidArgument)}
	m := mustNew(t, c, st)

	r := m.Apply([]byte(`{"id":"A","_SEQ":1,"Acct-Input-Octets":3}`))
	if r.Action != ActionFailed || r.Category != CategoryStorage {
		t.Fatalf("resultado = %+v, want Failed/STORAGE_ERROR", r)
	}
	if r.Fatal() {
		t.Error("erro de argumento não é terminal: o writer segue utilizável")
	}
	mt := m.Metrics()
	if mt.FailedTotal != 1 || mt.InvalidTotal != 0 || mt.ByCategory[CategoryStorage] != 1 {
		t.Errorf("métricas = %+v", mt)
	}
}

// Leitura do estado corrente corrompida também é Failed, não Skipped: pular o
// evento gravaria um create sobre uma entidade que existe.
func TestApplyErroDeLeituraEhFailed(t *testing.T) {
	c := compileYAML(t, patchYAML)
	st := storeErro{Store: fakestorage.New(fakestorage.Indexes{}),
		get: fmt.Errorf("%w: CRC do record", encoding.ErrCorrupt)}
	m := mustNew(t, c, st)

	r := m.Apply([]byte(`{"id":"A","_SEQ":1,"Acct-Input-Octets":3}`))
	if r.Action != ActionFailed || r.Category != CategoryStorage {
		t.Fatalf("resultado = %+v, want Failed/STORAGE_ERROR", r)
	}
	if !errors.Is(r.Err, encoding.ErrCorrupt) {
		t.Errorf("erro = %v, want encoding.ErrCorrupt", r.Err)
	}
	if len(st.Store.PutCalls) != 0 {
		t.Errorf("erro de leitura não pode virar escrita: %+v", st.Store.PutCalls)
	}
}

// storePayloadTruncado devolve um estado corrente ILEGÍVEL: é a corrupção
// vindo do storage, que o merge detecta e o Apply precisa classificar como
// defeito NOSSO, nunca como evento ruim.
type storePayloadTruncado struct {
	*fakestorage.Store
	rec types.Record
}

func (s storePayloadTruncado) Estado(key, buf []byte) (types.Record, []byte, types.Presence, error) {
	buf = buf[:0]
	buf = append(buf, key...)
	keyEnd := len(buf)
	buf = append(buf, s.rec.Payload...)
	return types.Record{
		RowID:    s.rec.RowID,
		Sequence: s.rec.Sequence,
		Key:      buf[:keyEnd:keyEnd],
		Payload:  buf[keyEnd:len(buf):len(buf)],
	}, buf, types.PresenceLive, nil
}

func (s storePayloadTruncado) GetByKey(_ types.Snapshot, key, buf []byte) (types.Record, []byte, bool, error) {
	buf = buf[:0]
	buf = append(buf, key...)
	keyEnd := len(buf)
	buf = append(buf, s.rec.Payload...)
	return types.Record{
		RowID:    s.rec.RowID,
		Sequence: s.rec.Sequence,
		Key:      buf[:keyEnd:keyEnd],
		Payload:  buf[keyEnd:len(buf):len(buf)],
	}, buf, true, nil
}

// A regra é normativa e inegociável: erro que indica BUG NOSSO jamais entra na
// métrica de evento ruim, senão esconde exatamente o defeito que a próxima
// mudança pode introduzir. Este teste é a rede dela — cobre a classificação
// (INTERNAL_STATE, não TYPE_CONVERSION), o balde (Failed, não Skipped), a
// contabilidade (FailedTotal, não InvalidTotal) e o corte do write path
// (nenhum payload parcial de um prev corrompido pode chegar ao Put).
func TestApplyPrevIlegivelEhInternalState(t *testing.T) {
	c := compileYAML(t, patchYAML)
	// Payload de um campo string, truncado no meio do valor.
	prev := encodeFields(t, fieldKV{1, vi64(10)}, fieldKV{2, vstr("abcdef")})
	st := storePayloadTruncado{
		Store: fakestorage.New(fakestorage.Indexes{}),
		rec:   types.Record{RowID: 7, Sequence: 1, Payload: prev[:len(prev)-3]},
	}
	m := mustNew(t, c, st)

	r := m.Apply([]byte(`{"id":"A","_SEQ":99,"Acct-Input-Octets":3}`))

	if r.Action != ActionFailed {
		t.Errorf("Action = %v, want Failed (evento bom, estado podre)", r.Action)
	}
	if r.Category != CategoryInternal {
		t.Errorf("Category = %v, want INTERNAL_STATE", r.Category)
	}
	if r.Fatal() {
		t.Error("prev corrompido não é writer em modo falho: o caller decide, não para de vez")
	}
	if !errors.Is(r.Err, ErrInternalState) {
		t.Errorf("erro = %v, want ErrInternalState", r.Err)
	}
	if !strings.Contains(r.Err.Error(), "estado anterior ilegível") {
		t.Errorf("mensagem = %q, want identificar a guarda", r.Err)
	}
	// Um payload montado a partir de prev ilegível é PARCIAL: se escapasse
	// para o Put, gravaria estado incompleto por cima do bom.
	if len(st.Store.PutCalls) != 0 {
		t.Errorf("merge falho chegou ao Put: %+v", st.Store.PutCalls)
	}
	mt := m.Metrics()
	if mt.FailedTotal != 1 || mt.InvalidTotal != 0 || mt.ByCategory[CategoryInternal] != 1 {
		t.Errorf("métricas = %+v, want FailedTotal=1 InvalidTotal=0 ByCategory[INTERNAL_STATE]=1", mt)
	}
	if mt.ByCategory[CategoryTypeConversion] != 0 {
		t.Error("erro interno contabilizado como TYPE_CONVERSION")
	}
}

// Os rótulos são o que o operador lê na métrica e no log: um mapeamento
// trocado manda investigar a coisa errada.
func TestRotulosDeActionECategoria(t *testing.T) {
	actions := map[Action]string{
		ActionCreated:  "created",
		ActionUpdated:  "updated",
		ActionReplaced: "replaced",
		ActionStale:    "stale",
		ActionSkipped:  "skipped",
		ActionFailed:   "failed",
	}
	for a, want := range actions {
		if got := a.String(); got != want {
			t.Errorf("Action(%d).String() = %q, want %q", a, got, want)
		}
	}
	if got := Action(200).String(); got != "action(?)" {
		t.Errorf("Action fora de faixa = %q", got)
	}

	cats := map[ErrorCategory]string{
		CategoryNone:             "none",
		CategoryInvalidJSON:      "INVALID_JSON",
		CategorySchemaValidation: "SCHEMA_VALIDATION",
		CategoryInvalidKey:       "INVALID_KEY",
		CategoryInvalidSequence:  "INVALID_SEQUENCE",
		CategoryTypeConversion:   "TYPE_CONVERSION",
		CategoryEventTooLarge:    "EVENT_TOO_LARGE",
		CategoryWriterFailed:     "WRITER_FAILED",
		CategoryStorage:          "STORAGE_ERROR",
		CategoryInternal:         "INTERNAL_STATE",
	}
	for c, want := range cats {
		if got := c.String(); got != want {
			t.Errorf("ErrorCategory(%d).String() = %q, want %q", c, got, want)
		}
	}
	if got := ErrorCategory(200).String(); got != "category(?)" {
		t.Errorf("ErrorCategory fora de faixa = %q", got)
	}
	// As cinco categorias §101 MAIS o teto de formato (EVENT_TOO_LARGE, que a
	// spec não nomeia) são as únicas de evento ruim: a fronteira do balde é o
	// que separa "pule este evento" de "temos um bug".
	for c := CategoryNone; c < numCategories; c++ {
		want := c >= CategoryInvalidJSON && c <= CategoryEventTooLarge
		if got := c.BadEvent(); got != want {
			t.Errorf("%v.BadEvent() = %v, want %v", c, got, want)
		}
	}
}

// dictErro falha ao registrar um campo novo — o que o fielddict real faz
// quando o fdatasync do dicionário falha.
type dictErro struct {
	inner *fakestorage.Dict
	falha string
}

func (d dictErro) LookupOrCreate(name []byte) (types.FieldID, error) {
	if string(name) == d.falha {
		return 0, errors.New("fielddict: fdatasync falhou")
	}
	return d.inner.LookupOrCreate(name)
}

func (d dictErro) TypeHint(id types.FieldID) (types.FieldType, bool) { return d.inner.TypeHint(id) }

func (d dictErro) SetTypeHint(id types.FieldID, t types.FieldType) error {
	return d.inner.SetTypeHint(id, t)
}

func (d dictErro) Len() int { return d.inner.Len() }

func (d dictErro) Name(id types.FieldID) (string, bool) { return d.inner.Name(id) }

// Falha ao registrar campo desconhecido vem do STORAGE, não do evento: não
// pode ser contabilizada como SCHEMA_VALIDATION nem virar Skipped.
func TestApplyFalhaDoDicionarioEhFailed(t *testing.T) {
	s, err := schema.Parse([]byte(fmt.Sprintf(unknownYAMLTmpl, "include", "include")), schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.Compile(dictErro{inner: fakestorage.NewDict(), falha: "novo"})
	if err != nil {
		t.Fatal(err)
	}
	m := mustNew(t, c, fakestorage.New(fakestorage.Indexes{}))

	r := m.Apply([]byte(`{"id":"A","novo":1}`))
	if r.Action != ActionFailed || r.Category != CategoryStorage {
		t.Fatalf("resultado = %+v, want Failed/STORAGE_ERROR", r)
	}
	if mt := m.Metrics(); mt.InvalidTotal != 0 || mt.FailedTotal != 1 {
		t.Errorf("métricas = %+v", mt)
	}
}

// ---------------------------------------------------------------- buffers

// Dois Applies seguidos com payloads de tamanhos diferentes: o estado gravado
// não pode carregar resíduo do evento anterior (buffers são reutilizados).
func TestApplyReutilizaBuffersEntreEventos(t *testing.T) {
	h := newHarness(t, patchYAML)
	h.apply(`{"id":"LONGO","_SEQ":1,"Acct-Status-Type":"valor-bem-mais-longo-que-o-proximo","Acct-Input-Octets":123456}`)
	h.apply(`{"id":"CURTO","_SEQ":1,"Acct-Status-Type":"x"}`)

	st := h.state("CURTO")
	h.wantString(st, "Acct-Status-Type", "x")
	h.wantString(st, "id", "CURTO")
	h.wantAbsent(st, "Acct-Input-Octets")
	if n := len(st); n != 3 {
		t.Errorf("estado de CURTO com %d campos: %+v", n, st)
	}
	// O estado anterior segue intacto — o segundo Apply não o corrompeu.
	h.wantInt(h.state("LONGO"), "Acct-Input-Octets", 123456)
}

// ---------------------------------------------------------------- fixture real

// Pipeline completo sobre a amostra REAL de evento RADIUS com o YAML de
// produção (spec §12): evento cheio cria a entidade, patch parcial com _SEQ
// maior mescla, e o estado final é conferido campo a campo.
func TestApplyFixtureRadiusCompletoDepoisPatch(t *testing.T) {
	spec, err := schema.Load(fixtureSpec, schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	c, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	statusField, ok := c.FieldByName([]byte("Acct-Status-Type"))
	if !ok {
		t.Fatal("Acct-Status-Type não compilado")
	}
	st := fakestorage.New(fakestorage.Indexes{Campos: []types.FieldID{statusField.ID}})
	m := mustNew(t, c, st)
	h := &harness{t: t, m: m, st: st, c: c}

	event, err := os.ReadFile(fixtureEvent)
	if err != nil {
		t.Fatal(err)
	}
	created := m.Apply(event)
	if created.Err != nil {
		t.Fatalf("Apply da amostra real: %v", created.Err)
	}
	if created.Action != ActionCreated {
		t.Fatalf("Action = %v, want Created", created.Action)
	}

	const key = "BLM-816P7LRL2Y-013"
	cheio := h.state(key)
	if len(cheio) != 43 {
		t.Errorf("estado com %d campos, want 43 (a amostra inteira)", len(cheio))
	}
	h.wantString(cheio, "Acct-Status-Type", "Alive")
	h.wantInt(cheio, "Acct-Output-Octets", 2613239581)
	h.wantInt(cheio, "_SEQ", 7479960553)

	// Patch parcial da MESMA sessão, com _SEQ maior.
	patch := []byte(`{"id":"BLM-816P7LRL2Y-013","_SEQ":7479960554,` +
		`"Acct-Status-Type":"Stop","Acct-Output-Octets":2613239999,"Acct-Session-Time":88888}`)
	upd := m.Apply(patch)
	if upd.Err != nil {
		t.Fatalf("Apply do patch: %v", upd.Err)
	}
	if upd.Action != ActionUpdated || upd.RowID != created.RowID {
		t.Fatalf("resultado = %+v, want Updated na row %d", upd, created.RowID)
	}

	final := h.state(key)
	if len(final) != len(cheio) {
		t.Errorf("estado final com %d campos, want %d (patch não pode perder campo)", len(final), len(cheio))
	}
	// Campos do patch: novos valores.
	h.wantString(final, "Acct-Status-Type", "Stop")
	h.wantInt(final, "Acct-Output-Octets", 2613239999)
	h.wantInt(final, "Acct-Session-Time", 88888)
	h.wantInt(final, "_SEQ", 7479960554)
	// Todo campo que o patch não mencionou tem o valor do evento cheio.
	tocados := map[string]bool{"Acct-Status-Type": true, "Acct-Output-Octets": true,
		"Acct-Session-Time": true, "_SEQ": true}
	for name, want := range cheio {
		if tocados[name] {
			continue
		}
		got, ok := final[name]
		if !ok {
			t.Errorf("campo %q sumiu no merge", name)
			continue
		}
		if !valueEqual(got, want) {
			t.Errorf("campo %q = %+v, want %+v (não estava no patch)", name, got, want)
		}
	}

	// O índice acompanhou a troca de status: o valor NOVO tem de estar lá
	// (falso negativo nunca), e o antigo permanece como candidato porque não há
	// fase de CLEAR no índice por segmento (spec §5.2). O estado corrente já foi
	// conferido campo a campo acima, que é onde a troca realmente se prova.
	alive := types.Value{Type: types.TypeString, Bytes: []byte("Alive")}
	if rows := h.bitmapRows("Acct-Status-Type", alive); len(rows) != 1 || rows[0] != created.RowID {
		t.Errorf("postings de Alive = %v, want [%d]: candidato tolerado, não resultado", rows, created.RowID)
	}
	stop := types.Value{Type: types.TypeString, Bytes: []byte("Stop")}
	if rows := h.bitmapRows("Acct-Status-Type", stop); len(rows) != 1 || rows[0] != created.RowID {
		t.Errorf("postings de Stop = %v, want [%d]", rows, created.RowID)
	}

	// Evento stale da mesma sessão não desfaz nada.
	stale := m.Apply([]byte(`{"id":"BLM-816P7LRL2Y-013","_SEQ":7479960553,"Acct-Status-Type":"Start"}`))
	if stale.Action != ActionStale {
		t.Fatalf("Action = %v, want Stale", stale.Action)
	}
	h.wantString(h.state(key), "Acct-Status-Type", "Stop")
}

// ------------------------------------------------------- teto de campos

const tetoYAMLTmpl = `version: 1
name: t
materialization:
  key:
    field: id
schema:
  include: ["*"]
  unknownFields: include
  maxCols: %d
  onMaxCols: %s
  fields:
    id:
      type: string
`

func compilaComTeto(t *testing.T, maxCols int, onMax string) *schema.Compiled {
	t.Helper()
	s, err := schema.Parse([]byte(fmt.Sprintf(tetoYAMLTmpl, maxCols, onMax)), schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// O teto de campos alcançado NÃO derruba o evento: sob `ignore` a chave
// excedente é descartada e todas as outras entram. É o contrário do que o
// binário fazia antes — o erro do dicionário subia sem sentinela, caía no
// balde de "bug nosso" e PARAVA o writer, com o offset congelado, de forma
// que o restart reproduzia o mesmo estouro para sempre.
func TestApplyAcimaDoTetoSobIgnoreGravaOEvento(t *testing.T) {
	c := compilaComTeto(t, 2, schema.OnMaxColsIgnore)
	m := mustNew(t, c, fakestorage.New(fakestorage.Indexes{}))

	r := m.Apply([]byte(`{"id":"A","cabe":1,"naoCabe":2}`))
	if r.Action != ActionCreated {
		t.Fatalf("resultado = %+v, quer Created — o evento tem de entrar", r)
	}
	if got := c.CamposDescartados(); got != 1 {
		t.Errorf("CamposDescartados = %d, quer 1", got)
	}
	if mt := m.Metrics(); mt.FailedTotal != 0 || mt.InvalidTotal != 0 {
		t.Errorf("métricas = %+v; descartar campo não é falha de evento", mt)
	}
}

// Sob `stop` a parada é DECLARADA, e carrega categoria própria: até aqui ela
// aconteceria de qualquer jeito, mas pelo default do classify, aparecendo ao
// operador como STORAGE_ERROR — "bug nosso ou dataset avariado". O teto
// alcançado não é nenhum dos dois.
func TestApplyAcimaDoTetoSobStopTemCategoriaPropria(t *testing.T) {
	c := compilaComTeto(t, 2, schema.OnMaxColsStop)
	m := mustNew(t, c, fakestorage.New(fakestorage.Indexes{}))

	r := m.Apply([]byte(`{"id":"A","cabe":1,"naoCabe":2}`))
	if r.Action != ActionFailed || r.Category != CategoryFieldsExhausted {
		t.Fatalf("resultado = %+v, quer Failed/FIELDS_EXHAUSTED", r)
	}
}

// A categoria do teto fica FORA da faixa de BadEvent(), e isso é o que faz o
// `stop` realmente parar. Dentro da faixa ela seria governada pela política de
// evento inválido, cujo default é log-and-skip: o writer descartaria o evento
// e seguiria, entregando o oposto do que a chave pede.
func TestCategoriaDeTetoNaoEEventoRuim(t *testing.T) {
	if CategoryFieldsExhausted.BadEvent() {
		t.Error("CategoryFieldsExhausted.BadEvent() = true; `stop` deixaria de parar")
	}
	if got := CategoryFieldsExhausted.String(); got != "FIELDS_EXHAUSTED" {
		t.Errorf("String() = %q, quer FIELDS_EXHAUSTED", got)
	}
}

const tetoAninhadoYAMLTmpl = `version: 1
name: t
materialization:
  key:
    field: id
schema:
  include: ["*"]
  unknownFields: include
  maxCols: %d
  onMaxCols: %s
  fields:
    id:
      type: string
    doc:
      type: json
      unknownFields: include
`

func compilaComTetoAninhado(t *testing.T, maxCols int, onMax string) *schema.Compiled {
	t.Helper()
	s, err := schema.Parse([]byte(fmt.Sprintf(tetoAninhadoYAMLTmpl, maxCols, onMax)), schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestTetoSobStopParaAIngestaoTambemNoCaminhoANINHADO fixa a metade que
// faltava: a política que o operador declarou valia no caminho de topo e era
// silenciosamente REBAIXADA a `ignore` no caminho aninhado.
//
// O ResolveAninhado devolvia erro para as duas políticas, e o `continue` do
// extract engolia as duas — então `stop` seguia ingerindo, com o contador de
// descarte subindo e nada parando. O operador que escolheu `stop` recebia o
// comportamento de quem escolheu `ignore`, sem uma linha de log dizendo isso.
func TestTetoSobStopParaAIngestaoTambemNoCaminhoANINHADO(t *testing.T) {
	c := compilaComTetoAninhado(t, 3, schema.OnMaxColsStop)
	m := mustNew(t, c, fakestorage.New(fakestorage.Indexes{}))

	r := m.Apply([]byte(`{"id":"A","doc":{"cabe":1,"naoCabe":2}}`))
	if r.Action != ActionFailed || r.Category != CategoryFieldsExhausted {
		t.Fatalf("resultado = %+v, quer Failed/FIELDS_EXHAUSTED", r)
	}
}

// E sob `ignore` o caminho aninhado continua fazendo o que sempre fez: a chave
// excedente some, o evento entra, e a perda fica no contador. É a metade que
// JÁ funcionava, e ela está aqui para provar que a correção do `stop` não a
// trocou de lado.
func TestTetoSobIgnoreNoCaminhoANINHADOGravaOEvento(t *testing.T) {
	c := compilaComTetoAninhado(t, 3, schema.OnMaxColsIgnore)
	m := mustNew(t, c, fakestorage.New(fakestorage.Indexes{}))

	r := m.Apply([]byte(`{"id":"A","doc":{"cabe":1,"naoCabe":2}}`))
	if r.Action != ActionCreated {
		t.Fatalf("resultado = %+v, quer Created — o evento tem de entrar", r)
	}
	if got := c.CamposDescartados(); got == 0 {
		t.Error("CamposDescartados = 0; a chave excedente sumiu sem ser contada")
	}
	if mt := m.Metrics(); mt.FailedTotal != 0 || mt.InvalidTotal != 0 {
		t.Errorf("métricas = %+v; descartar campo não é falha de evento", mt)
	}
}
