package query_test

import (
	"context"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Fixture: o YAML REAL da spec §12. Os testes §106 rodam contra o schema de
// produção — alias, campo quoted e índices são os de verdade.
const fixtureYAML = "../../testdata/radius.materialization.yaml"

const dataset = "radius_sessions"

// harness costura ingestão e query sobre o MESMO store: os dados entram pelo
// Materializer.Apply (o pipeline real, não um atalho de teste) e saem pela
// Engine.
type harness struct {
	t      *testing.T
	sch    *schema.Compiled
	store  *fakestorage.Store
	mat    *materializer.Materializer
	engine *query.Engine
}

func novoHarness(t *testing.T) *harness {
	t.Helper()
	spec, err := schema.Load(fixtureYAML, schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	sch, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	// TODOS os campos indexados do fixture entram no conjunto do dublê. A lista
	// era passada em Bitmap E em Hash enquanto o dublê tinha dois conjuntos,
	// porque o Store real respondia true nos dois para qualquer campo indexado;
	// com um conjunto só, a paridade com o real é estrutural.
	todosOsIndexados := ids(t, sch, "Acct-Status-Type", "Framed-Protocol", "Acct-Terminate-Cause",
		"Filter-Id", "NAS-Port-Type", "Service-Type", "Acct-Authentic",
		"NAS-IP-Address", "NAS-Identifier", "Framed-IP-Address")
	idx := fakestorage.Indexes{Campos: todosOsIndexados}
	st := fakestorage.New(idx)
	eng := query.NewEngine()
	if err := eng.Register(dataset, sch, st); err != nil {
		t.Fatal(err)
	}
	mat, err := materializer.New(sch, st)
	if err != nil {
		t.Fatal(err)
	}
	return &harness{t: t, sch: sch, store: st, mat: mat, engine: eng}
}

func ids(t *testing.T, sch *schema.Compiled, nomes ...string) []types.FieldID {
	t.Helper()
	out := make([]types.FieldID, len(nomes))
	for i, n := range nomes {
		f, ok := sch.FieldByName([]byte(n))
		if !ok {
			t.Fatalf("campo %q não existe no fixture", n)
		}
		out[i] = f.ID
	}
	return out
}

// ingere aplica eventos pelo pipeline real e falha no primeiro descarte: um
// evento silenciosamente pulado tornaria o teste de query verde por falta de
// dado.
func (h *harness) ingere(eventos ...string) {
	h.t.Helper()
	for _, ev := range eventos {
		if r := h.mat.Apply([]byte(ev)); r.Err != nil {
			h.t.Fatalf("ingestão de %s: %v (%v)", ev, r.Err, r.Category)
		}
	}
}

// rowID resolve o rowId de uma key pelo próprio Store — os testes que mexem
// no índice precisam dele, e ler pelo GetByKey evita alargar a superfície do
// dublê só para teste.
func (h *harness) rowID(key string) types.RowID {
	h.t.Helper()
	rec, _, ok, err := h.store.GetByKey([]byte(key), nil)
	if err != nil || !ok {
		h.t.Fatalf("key %q não materializada (ok=%v err=%v)", key, ok, err)
	}
	return rec.RowID
}

// linha é uma row do resultado materializada para asserção (o executor é
// streaming; os testes é que precisam comparar).
type linha map[string]string

// consulta executa e materializa TODAS as rows, copiando os valores — o
// contrato zero-copy exige a cópia antes do próximo Next.
func (h *harness) consulta(sql string, params ...types.Value) ([]linha, error) {
	h.t.Helper()
	it, err := h.engine.Execute(context.Background(), dataset, sql, params)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var out []linha
	for it.Next() {
		l := make(linha, it.Row().Len())
		for _, c := range it.Row().Cols() {
			l[c.Name] = render(c)
		}
		out = append(out, l)
	}
	return out, it.Err()
}

// consultaOK falha o teste em qualquer erro.
func (h *harness) consultaOK(sql string, params ...types.Value) []linha {
	h.t.Helper()
	rows, err := h.consulta(sql, params...)
	if err != nil {
		h.t.Fatalf("query %q: %v", sql, err)
	}
	return rows
}

// chaves devolve o valor da coluna dada, na ordem de saída — o jeito mais
// legível de comparar conjuntos de resultado.
func chaves(rows []linha, col string) []string {
	out := make([]string, 0, len(rows))
	for _, l := range rows {
		out = append(out, l[col])
	}
	return out
}

func render(c executor.ColView) string {
	if c.Null {
		return "<null>"
	}
	switch c.Val.Type {
	case types.TypeString, types.TypeJSON:
		return string(c.Val.Bytes)
	case types.TypeInt64, types.TypeTimestamp:
		return itoa(c.Val.I64)
	case types.TypeBool:
		if c.Val.I64 != 0 {
			return "true"
		}
		return "false"
	default:
		return hex(c.Val.Bytes)
	}
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	var b [24]byte
	i := len(b)
	u := uint64(v)
	if neg {
		u = uint64(-v)
	}
	for u > 0 {
		i--
		b[i] = byte('0' + u%10)
		u /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

func hex(b []byte) string {
	const d = "0123456789abcdef"
	var sb strings.Builder
	sb.WriteString("0x")
	for _, c := range b {
		sb.WriteByte(d[c>>4])
		sb.WriteByte(d[c&0xf])
	}
	return sb.String()
}
