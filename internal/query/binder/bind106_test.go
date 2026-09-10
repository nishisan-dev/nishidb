package binder_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// As 12 classes da §106 na camada do binder. NÃO fecham os itens — a
// rastreabilidade normativa é do Q15, fim a fim. Aqui o que se prova é que
// cada forma liga corretamente e produz o que o planner precisa para decidir
// o plano.

func comparisonOf(t *testing.T, bq *binder.BoundQuery) *binder.BoundComparison {
	t.Helper()
	c, ok := bq.Where.(*binder.BoundComparison)
	if !ok {
		t.Fatalf("Where = %#v, want comparação", bq.Where)
	}
	return c
}

func TestBind106PrimaryLookup(t *testing.T) {
	bq := bindOK(t, "SELECT * FROM radius_sessions WHERE id = 'CMP-V0001PPCLP-013'")
	c := comparisonOf(t, bq)
	// O planner decide point lookup por IsKey + CmpEq: os dois precisam
	// chegar prontos.
	if !c.Field.IsKey {
		t.Error("IsKey = false na coluna key")
	}
	if c.Op != parser.CmpEq || c.Kind != binder.CompareBytes {
		t.Errorf("comparação = %+v", c)
	}
	if string(c.Val.Bytes) != "CMP-V0001PPCLP-013" {
		t.Errorf("valor = %q", c.Val.Bytes)
	}
}

// `key = ?` também vira point lookup no PLAN-TIME, porque o parâmetro é
// coerido no bind. Sem isso o planner teria de adiar a decisão para a
// execução.
func TestBind106PrimaryLookupComParametro(t *testing.T) {
	bq := bindOK(t, "SELECT * FROM radius_sessions WHERE id = ?", wireString("CMP-1"))
	c := comparisonOf(t, bq)
	if !c.Field.IsKey || c.Op != parser.CmpEq {
		t.Errorf("comparação = %+v", c)
	}
	if !c.IsParam {
		t.Error("IsParam = false")
	}
	if string(c.Val.Bytes) != "CMP-1" {
		t.Errorf("valor não resolvido no bind: %q", c.Val.Bytes)
	}
}

func TestBind106IndexLookup(t *testing.T) {
	bq := bindOK(t, "SELECT id FROM radius_sessions WHERE status = 'Stop'")
	c := comparisonOf(t, bq)
	// O planner escolhe o caminho indexado por Indexado + CmpEq.
	if !c.Field.Indexed {
		t.Error("Indexed = false, want true")
	}
	if c.Op != parser.CmpEq {
		t.Errorf("Op = %v", c.Op)
	}
}

func TestBind106And(t *testing.T) {
	bq := bindOK(t, "SELECT id FROM radius_sessions WHERE status = 'Stop' AND octets > 100")
	l, ok := bq.Where.(*binder.BoundLogical)
	if !ok || l.Op != parser.LogicalAnd {
		t.Fatalf("Where = %#v", bq.Where)
	}
	if _, ok := l.Left.(*binder.BoundComparison); !ok {
		t.Errorf("esquerda = %#v", l.Left)
	}
}

func TestBind106Or(t *testing.T) {
	bq := bindOK(t, "SELECT id FROM radius_sessions WHERE status = 'Stop' OR status = 'Start'")
	l, ok := bq.Where.(*binder.BoundLogical)
	if !ok || l.Op != parser.LogicalOr {
		t.Fatalf("Where = %#v", bq.Where)
	}
}

func TestBind106Comparison(t *testing.T) {
	for _, op := range []struct {
		sql  string
		want parser.CmpOp
	}{
		{"=", parser.CmpEq}, {"!=", parser.CmpNe}, {">", parser.CmpGt},
		{">=", parser.CmpGte}, {"<", parser.CmpLt}, {"<=", parser.CmpLte},
	} {
		t.Run(op.sql, func(t *testing.T) {
			bq := bindOK(t, "SELECT id FROM radius_sessions WHERE octets "+op.sql+" 100")
			if c := comparisonOf(t, bq); c.Op != op.want {
				t.Errorf("Op = %v, want %v", c.Op, op.want)
			}
		})
	}
}

func TestBind106Null(t *testing.T) {
	bq := bindOK(t, "SELECT id FROM radius_sessions WHERE quando IS NULL")
	nc, ok := bq.Where.(*binder.BoundNullCheck)
	if !ok || nc.Not {
		t.Fatalf("Where = %#v", bq.Where)
	}
	bq = bindOK(t, "SELECT id FROM radius_sessions WHERE quando IS NOT NULL")
	if nc := bq.Where.(*binder.BoundNullCheck); !nc.Not {
		t.Error("Not = false para IS NOT NULL")
	}
}

func TestBind106Limit(t *testing.T) {
	bq := bindOK(t, "SELECT id FROM radius_sessions LIMIT 100")
	if !bq.HasLimit || bq.Limit != 100 || bq.EffectiveLimit != 100 {
		t.Errorf("limites = %v/%d/%d", bq.HasLimit, bq.Limit, bq.EffectiveLimit)
	}
}

func TestBind106FullScan(t *testing.T) {
	bq := bindOK(t, "SELECT * FROM radius_sessions")
	if bq.Where != nil {
		t.Errorf("Where = %#v, want nil", bq.Where)
	}
	// Sem LIMIT, o teto é o do dataset — e o executor precisa saber que a
	// parada veio dele.
	if bq.EffectiveLimit != bq.MaxRows {
		t.Errorf("EffectiveLimit = %d, want MaxRows = %d", bq.EffectiveLimit, bq.MaxRows)
	}
}

func TestBind106Aliases(t *testing.T) {
	bq := bindOK(t, "SELECT service_id, status FROM radius_sessions WHERE status = ?", wireString("Stop"))
	if bq.Columns[0].Field.Name != "id" {
		t.Errorf("alias não resolveu: %+v", bq.Columns[0])
	}
	if bq.Columns[0].Name != "service_id" {
		t.Errorf("rótulo = %q, want o nome escrito", bq.Columns[0].Name)
	}
}

func TestBind106QuotedFields(t *testing.T) {
	// Quoted e bare chegam iguais na AST: o binder resolve pela mesma chave.
	bq := bindOK(t, `SELECT "status" FROM radius_sessions WHERE "octets" > 1`)
	if bq.Columns[0].Field.Name != "status" {
		t.Errorf("coluna = %+v", bq.Columns[0])
	}
	if c := comparisonOf(t, bq); c.Field.Name != "octets" {
		t.Errorf("predicado = %+v", c)
	}
}

func TestBind106Parameters(t *testing.T) {
	bq := bindOK(t, "SELECT id FROM radius_sessions WHERE status = ? AND octets >= ?",
		wireString("Stop"), wireInt(500))
	l := bq.Where.(*binder.BoundLogical)
	left := l.Left.(*binder.BoundComparison)
	right := l.Right.(*binder.BoundComparison)
	if !left.IsParam || !right.IsParam {
		t.Error("parâmetros não marcados")
	}
	if left.ParamIdx != 0 || right.ParamIdx != 1 {
		t.Errorf("índices = %d, %d", left.ParamIdx, right.ParamIdx)
	}
	if string(left.Val.Bytes) != "Stop" || right.Val.I64 != 500 {
		t.Errorf("valores = %+v / %+v", left.Val, right.Val)
	}
}

// Extended JSON não passa por aqui (é da ingestão), mas o tipo binário
// resultante é o mesmo que a query precisa produzir — este caso prova que o
// timestamp coerido da query bate com o millis que a ingestão grava.
func TestBind106TimestampCasaComIngestao(t *testing.T) {
	bq := bindOK(t, "SELECT id FROM radius_sessions WHERE quando = '2026-08-12T21:00:00.000+00:00'")
	c := comparisonOf(t, bq)
	if c.Val.Type != types.TypeTimestamp || c.Val.I64 != 1786568400000 {
		t.Errorf("valor = %+v", c.Val)
	}
}
