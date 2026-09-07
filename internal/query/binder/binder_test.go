package binder_test

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// YAML de teste com um campo de cada tipo comparável, mais os dois que não
// comparam (bytes/json) e um auto.
const dsYAML = `
version: 1
name: radius_sessions
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
      alias: service_id
    status:
      type: string
      index: true
    octets:
      type: int64
    ratio:
      type: double
    ativo:
      type: boolean
    quando:
      type: timestamp
    nas_ip:
      type: ipv4
    v6:
      type: ipv6
    rede4:
      type: ipv4_prefix
    rede6:
      type: ipv6_prefix
    raw:
      type: bytes
    doc:
      type: json
    livre:
      type: auto
query:
  maxRows: 1000
  timeout: 15s
`

func compile(t *testing.T) *schema.Compiled {
	t.Helper()
	spec, err := schema.Parse([]byte(dsYAML), schema.BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	c, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func bindOK(t *testing.T, sql string, params ...types.Value) *binder.BoundQuery {
	t.Helper()
	st, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse(%q): %v", sql, err)
	}
	bq, err := binder.Bind(st, compile(t), params)
	if err != nil {
		t.Fatalf("Bind(%q): %v", sql, err)
	}
	return bq
}

func bindErr(t *testing.T, sql string, params ...types.Value) error {
	t.Helper()
	st, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse(%q): %v", sql, err)
	}
	bq, err := binder.Bind(st, compile(t), params)
	if err == nil {
		t.Fatalf("Bind(%q) não falhou: %#v", sql, bq)
	}
	if !errors.Is(err, binder.ErrBind) {
		t.Errorf("erro = %v, want errors.Is ErrBind", err)
	}
	return err
}

func TestProjecaoExplicita(t *testing.T) {
	bq := bindOK(t, "SELECT id, status FROM radius_sessions")
	if bq.Star {
		t.Error("Star = true numa projeção explícita")
	}
	if len(bq.Columns) != 2 {
		t.Fatalf("%d colunas", len(bq.Columns))
	}
	// Nome de SAÍDA é o que o usuário escreveu.
	if bq.Columns[0].Name != "id" || bq.Columns[1].Name != "status" {
		t.Errorf("nomes = %q, %q", bq.Columns[0].Name, bq.Columns[1].Name)
	}
	if bq.Dataset != "radius_sessions" || bq.Schema == nil {
		t.Errorf("dataset = %q, schema = %v", bq.Dataset, bq.Schema)
	}
}

// Alias resolve para o mesmo campo, e o rótulo continua sendo o que o usuário
// escreveu — quem pede `service_id` recebe a coluna chamada service_id.
func TestProjecaoPorAlias(t *testing.T) {
	bq := bindOK(t, "SELECT service_id FROM radius_sessions")
	if bq.Columns[0].Name != "service_id" {
		t.Errorf("nome de saída = %q, want service_id", bq.Columns[0].Name)
	}
	if bq.Columns[0].Field.Name != "id" {
		t.Errorf("campo resolvido = %q, want id", bq.Columns[0].Field.Name)
	}
}

// No '*' o usuário não escreveu nome nenhum, então o rótulo é o alias quando
// existe.
func TestProjecaoEstrelaUsaAlias(t *testing.T) {
	bq := bindOK(t, "SELECT * FROM radius_sessions")
	if !bq.Star {
		t.Fatal("Star = false")
	}
	fields := compile(t).FieldsByID()
	if len(bq.Columns) != len(fields) {
		t.Fatalf("%d colunas, want %d", len(bq.Columns), len(fields))
	}
	// Ordem de FieldID e alias no rótulo.
	for i := 1; i < len(bq.Columns); i++ {
		if bq.Columns[i].Field.ID <= bq.Columns[i-1].Field.ID {
			t.Fatalf("colunas fora de ordem de FieldID na posição %d", i)
		}
	}
	var found bool
	for _, c := range bq.Columns {
		if c.Field.Name == "id" {
			found = true
			if c.Name != "service_id" {
				t.Errorf("rótulo do campo id = %q, want o alias service_id", c.Name)
			}
		}
	}
	if !found {
		t.Error("campo id ausente do *")
	}
}

// Os slots são a ordem do PAYLOAD: crescente, sem duplicata, cobrindo
// projeção ∪ WHERE.
func TestSlotsOrdenadosECobremProjecaoEWhere(t *testing.T) {
	bq := bindOK(t, "SELECT status FROM radius_sessions WHERE octets > 10 AND id = 'x'")

	if bq.Slots.Len() != 3 {
		t.Fatalf("Slots = %v, want 3 campos (status, octets, id)", bq.Slots.IDs)
	}
	for i := 1; i < bq.Slots.Len(); i++ {
		if bq.Slots.IDs[i] <= bq.Slots.IDs[i-1] {
			t.Fatalf("slots fora de ordem: %v", bq.Slots.IDs)
		}
	}
	// Todo Slot preenchido aponta para o campo certo.
	for _, c := range bq.Columns {
		if got := bq.Slots.IDs[c.Slot]; got != c.Field.ID {
			t.Errorf("coluna %q no slot %d aponta para o campo %d", c.Name, c.Slot, got)
		}
	}
	var walk func(binder.BoundExpr)
	walk = func(e binder.BoundExpr) {
		switch n := e.(type) {
		case *binder.BoundLogical:
			walk(n.Left)
			walk(n.Right)
		case *binder.BoundComparison:
			if got := bq.Slots.IDs[n.Slot]; got != n.Field.ID {
				t.Errorf("comparação de %q no slot %d aponta para o campo %d", n.Field.Name, n.Slot, got)
			}
		case *binder.BoundNullCheck:
			if got := bq.Slots.IDs[n.Slot]; got != n.Field.ID {
				t.Errorf("null check de %q no slot %d aponta para o campo %d", n.Field.Name, n.Slot, got)
			}
		}
	}
	walk(bq.Where)
}

// A forma da árvore é preservada: o planner acha as cadeias de AND pela
// espinha à esquerda, e reescrever aqui o quebraria em silêncio.
func TestFormaDaArvorePreservada(t *testing.T) {
	bq := bindOK(t, "SELECT id FROM radius_sessions WHERE id = 'a' AND status = 'b' AND octets = 1")
	root, ok := bq.Where.(*binder.BoundLogical)
	if !ok || root.Op != parser.LogicalAnd {
		t.Fatalf("raiz = %#v", bq.Where)
	}
	if _, ok := root.Left.(*binder.BoundLogical); !ok {
		t.Errorf("esquerda = %#v, want conectivo (árvore à esquerda)", root.Left)
	}
	if _, ok := root.Right.(*binder.BoundComparison); !ok {
		t.Errorf("direita = %#v, want comparação", root.Right)
	}
}

// Os valores coeridos: cada tipo com o binário que a INGESTÃO gravaria.
func TestCoercaoDeLiterais(t *testing.T) {
	cases := []struct {
		name  string
		sql   string
		kind  binder.CompareKind
		check func(t *testing.T, v types.Value)
	}{
		{
			name: "string", sql: "SELECT id FROM radius_sessions WHERE status = 'Stop'",
			kind: binder.CompareBytes,
			check: func(t *testing.T, v types.Value) {
				if v.Type != types.TypeString || string(v.Bytes) != "Stop" {
					t.Errorf("valor = %+v", v)
				}
			},
		},
		{
			name: "int64", sql: "SELECT id FROM radius_sessions WHERE octets = 42",
			kind: binder.CompareInt,
			check: func(t *testing.T, v types.Value) {
				if v.Type != types.TypeInt64 || v.I64 != 42 {
					t.Errorf("valor = %+v", v)
				}
			},
		},
		{
			// A promoção que o executor precisa saber tratar: o literal vira
			// bits de float, mas o payload da coluna é inteiro cru.
			name: "int64 com literal float", sql: "SELECT id FROM radius_sessions WHERE octets > 1.5",
			kind: binder.CompareIntAsFloat,
			check: func(t *testing.T, v types.Value) {
				if v.Type != types.TypeDouble {
					t.Fatalf("tipo = %v, want double", v.Type)
				}
				if got := math.Float64frombits(uint64(v.I64)); got != 1.5 {
					t.Errorf("literal = %v, want 1.5", got)
				}
			},
		},
		{
			name: "double com literal int", sql: "SELECT id FROM radius_sessions WHERE ratio = 3",
			kind: binder.CompareFloatBits,
			check: func(t *testing.T, v types.Value) {
				if got := math.Float64frombits(uint64(v.I64)); got != 3.0 {
					t.Errorf("literal = %v, want 3.0", got)
				}
			},
		},
		{
			name: "boolean", sql: "SELECT id FROM radius_sessions WHERE ativo = TRUE",
			kind: binder.CompareInt,
			check: func(t *testing.T, v types.Value) {
				if v.Type != types.TypeBool || v.I64 != 1 {
					t.Errorf("valor = %+v", v)
				}
			},
		},
		{
			name: "timestamp string", sql: "SELECT id FROM radius_sessions WHERE quando >= '2026-08-12T21:00:00.000Z'",
			kind: binder.CompareInt,
			check: func(t *testing.T, v types.Value) {
				if v.Type != types.TypeTimestamp || v.I64 != 1786568400000 {
					t.Errorf("valor = %+v, want millis 1786568400000", v)
				}
			},
		},
		{
			// Número é epoch MILLIS, sem heurística de segundos.
			name: "timestamp int", sql: "SELECT id FROM radius_sessions WHERE quando = 1786568400000",
			kind: binder.CompareInt,
			check: func(t *testing.T, v types.Value) {
				if v.I64 != 1786568400000 {
					t.Errorf("valor = %+v", v)
				}
			},
		},
		{
			name: "ipv4", sql: "SELECT id FROM radius_sessions WHERE nas_ip = '10.0.0.1'",
			kind: binder.CompareBytes,
			check: func(t *testing.T, v types.Value) {
				if !bytes.Equal(v.Bytes, []byte{10, 0, 0, 1}) {
					t.Errorf("bytes = %v, want [10 0 0 1]", v.Bytes)
				}
			},
		},
		{
			name: "ipv6", sql: "SELECT id FROM radius_sessions WHERE v6 = '2001:db8::1'",
			kind: binder.CompareBytes,
			check: func(t *testing.T, v types.Value) {
				want := []byte{32, 1, 13, 184, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
				if !bytes.Equal(v.Bytes, want) {
					t.Errorf("bytes = %v", v.Bytes)
				}
			},
		},
		{
			// Host bits preservados, igual à ingestão.
			name: "ipv4_prefix", sql: "SELECT id FROM radius_sessions WHERE rede4 = '10.0.0.1/24'",
			kind: binder.CompareBytes,
			check: func(t *testing.T, v types.Value) {
				if !bytes.Equal(v.Bytes, []byte{10, 0, 0, 1, 24}) {
					t.Errorf("bytes = %v, want [10 0 0 1 24]", v.Bytes)
				}
			},
		},
		{
			name: "ipv6_prefix", sql: "SELECT id FROM radius_sessions WHERE rede6 = '2001:db8::/32'",
			kind: binder.CompareBytes,
			check: func(t *testing.T, v types.Value) {
				if len(v.Bytes) != 17 || v.Bytes[16] != 32 {
					t.Errorf("bytes = %v", v.Bytes)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bq := bindOK(t, tc.sql)
			cmp, ok := bq.Where.(*binder.BoundComparison)
			if !ok {
				t.Fatalf("Where = %#v", bq.Where)
			}
			if cmp.Kind != tc.kind {
				t.Errorf("Kind = %v, want %v", cmp.Kind, tc.kind)
			}
			if cmp.Kind == binder.CompareUnset {
				t.Error("CompareUnset é sempre bug")
			}
			tc.check(t, cmp.Val)
		})
	}
}

// A armadilha do arena: várias comparações com valor parseado, e cada janela
// tem de continuar apontando para os SEUS bytes depois de todos os appends.
func TestArenaComVariosValoresParseados(t *testing.T) {
	bq := bindOK(t, "SELECT id FROM radius_sessions WHERE nas_ip = '10.0.0.1' "+
		"AND v6 = '2001:db8::1' AND rede4 = '192.168.1.0/24' AND nas_ip != '172.16.0.9'")

	var seen [][]byte
	var walk func(binder.BoundExpr)
	walk = func(e binder.BoundExpr) {
		switch n := e.(type) {
		case *binder.BoundLogical:
			walk(n.Left)
			walk(n.Right)
		case *binder.BoundComparison:
			seen = append(seen, n.Val.Bytes)
		}
	}
	walk(bq.Where)

	if len(seen) != 4 {
		t.Fatalf("%d comparações", len(seen))
	}
	want := [][]byte{
		{10, 0, 0, 1},
		{32, 1, 13, 184, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		{192, 168, 1, 0, 24},
		{172, 16, 0, 9},
	}
	for i := range want {
		if !bytes.Equal(seen[i], want[i]) {
			t.Errorf("valor %d = %v, want %v (janela do arena corrompida por realocação?)",
				i, seen[i], want[i])
		}
	}
	// As janelas têm cap limitado: um append posterior no arena não pode
	// sobrescrever o valor de ninguém.
	for i, v := range seen {
		if cap(v) != len(v) {
			t.Errorf("valor %d com cap %d > len %d: append futuro corromperia o vizinho", i, cap(v), len(v))
		}
	}
}

func TestLimitEEfetivo(t *testing.T) {
	cases := []struct {
		name      string
		sql       string
		has       bool
		limit     int64
		effective int64
	}{
		{"sem limit", "SELECT id FROM radius_sessions", false, 0, 1000},
		{"limit menor que maxRows", "SELECT id FROM radius_sessions LIMIT 10", true, 10, 10},
		{"limit maior que maxRows", "SELECT id FROM radius_sessions LIMIT 5000", true, 5000, 1000},
		{"limit zero", "SELECT id FROM radius_sessions LIMIT 0", true, 0, 0},
		{"limit igual a maxRows", "SELECT id FROM radius_sessions LIMIT 1000", true, 1000, 1000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bq := bindOK(t, tc.sql)
			if bq.HasLimit != tc.has || bq.Limit != tc.limit || bq.EffectiveLimit != tc.effective {
				t.Errorf("HasLimit=%v Limit=%d Effective=%d, want %v/%d/%d",
					bq.HasLimit, bq.Limit, bq.EffectiveLimit, tc.has, tc.limit, tc.effective)
			}
			// O trio viaja separado para o executor saber QUAL teto cortou.
			if bq.MaxRows != 1000 {
				t.Errorf("MaxRows = %d, want 1000", bq.MaxRows)
			}
		})
	}
}

func TestTimeoutVemDoSchema(t *testing.T) {
	bq := bindOK(t, "SELECT id FROM radius_sessions")
	if bq.Timeout.Seconds() != 15 {
		t.Errorf("Timeout = %v, want 15s", bq.Timeout)
	}
}

// IS [NOT] NULL vale para qualquer tipo, inclusive os que não comparam.
func TestNullCheckEmQualquerTipo(t *testing.T) {
	for _, col := range []string{"id", "octets", "raw", "doc", "livre"} {
		t.Run(col, func(t *testing.T) {
			bq := bindOK(t, "SELECT id FROM radius_sessions WHERE "+col+" IS NULL")
			nc, ok := bq.Where.(*binder.BoundNullCheck)
			if !ok {
				t.Fatalf("Where = %#v", bq.Where)
			}
			if nc.Not {
				t.Error("Not = true para IS NULL")
			}
			if nc.Field.Name != col {
				t.Errorf("campo = %q", nc.Field.Name)
			}
		})
	}
	bq := bindOK(t, "SELECT id FROM radius_sessions WHERE raw IS NOT NULL")
	if nc := bq.Where.(*binder.BoundNullCheck); !nc.Not {
		t.Error("Not = false para IS NOT NULL")
	}
}

func TestErrosDeBind(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		msg  string
	}{
		{"dataset errado", "SELECT id FROM outro", `dataset "outro" desconhecido`},
		{"coluna inexistente", "SELECT foo FROM radius_sessions", `coluna "foo" não existe`},
		{"coluna inexistente no where", "SELECT id FROM radius_sessions WHERE foo = 1", `coluna "foo" não existe`},
		{"literal incompativel", "SELECT id FROM radius_sessions WHERE nas_ip = TRUE", "não é comparável com literal do tipo"},
		{"string em int64", "SELECT id FROM radius_sessions WHERE octets = '1000'", "não é comparável com literal do tipo"},
		{"coluna nao comparavel", "SELECT id FROM radius_sessions WHERE raw = 'x'", "só admite IS NULL / IS NOT NULL"},
		{"json nao comparavel", "SELECT id FROM radius_sessions WHERE doc = 'x'", "só admite IS NULL / IS NOT NULL"},
		{"operador em boolean", "SELECT id FROM radius_sessions WHERE ativo > TRUE", "não é permitido para a coluna"},
		{"operador em prefixo", "SELECT id FROM radius_sessions WHERE rede4 > '10.0.0.0/8'", "não é permitido para a coluna"},
		{"ipv4 invalido", "SELECT id FROM radius_sessions WHERE nas_ip = '10.0.0.256'", "inválido para a coluna"},
		{"ipv6 invalido", "SELECT id FROM radius_sessions WHERE v6 = '10.0.0.1'", "inválido para a coluna"},
		{"timestamp invalido", "SELECT id FROM radius_sessions WHERE quando = 'ontem'", "inválido para a coluna"},
		{"auto sem tipo", "SELECT id FROM radius_sessions WHERE livre = 'x'", "nunca foi observada"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := bindErr(t, tc.sql)
			if !strings.Contains(err.Error(), tc.msg) {
				t.Errorf("erro = %q, want conter %q", err, tc.msg)
			}
			// A posição permite ao cliente destacar o trecho.
			var be *binder.Error
			if errors.As(err, &be) && be.Pos < 0 {
				t.Errorf("erro sem posição: %q", err)
			}
		})
	}
}

// Campo auto COM tipo fixado é comparável como qualquer campo declarado — é o
// que torna filtrável o dataset real, que usa wildcard com unknownFields
// include.
func TestAutoComTipoFixadoEhComparavel(t *testing.T) {
	sc := compile(t)
	f, ok := sc.FieldByName([]byte("livre"))
	if !ok {
		t.Fatal("campo livre não compilado")
	}
	if err := sc.FixAutoType(f, types.TypeInt64); err != nil {
		t.Fatal(err)
	}

	st, err := parser.Parse("SELECT id FROM radius_sessions WHERE livre = 42")
	if err != nil {
		t.Fatal(err)
	}
	bq, err := binder.Bind(st, sc, nil)
	if err != nil {
		t.Fatalf("campo auto fixado deveria ser comparável: %v", err)
	}
	cmp := bq.Where.(*binder.BoundComparison)
	if cmp.Kind != binder.CompareInt || cmp.Val.I64 != 42 {
		t.Errorf("comparação = %+v", cmp)
	}

	// E a célula escolhida é a do tipo FIXADO: um campo fixado como double
	// coage o literal int para bits de float.
	sc2 := compile(t)
	f2, _ := sc2.FieldByName([]byte("livre"))
	if err := sc2.FixAutoType(f2, types.TypeDouble); err != nil {
		t.Fatal(err)
	}
	st2, _ := parser.Parse("SELECT id FROM radius_sessions WHERE livre = 5")
	bq2, err := binder.Bind(st2, sc2, nil)
	if err != nil {
		t.Fatal(err)
	}
	cmp2 := bq2.Where.(*binder.BoundComparison)
	if cmp2.Kind != binder.CompareFloatBits {
		t.Errorf("Kind = %v, want float-bits (o tipo fixado manda)", cmp2.Kind)
	}
	if got := math.Float64frombits(uint64(cmp2.Val.I64)); got != 5.0 {
		t.Errorf("literal = %v, want 5.0", got)
	}
}

// IS NULL sobre campo auto sem tipo continua funcionando: presença não
// depende de tipo.
func TestAutoSemTipoAceitaIsNull(t *testing.T) {
	bq := bindOK(t, "SELECT id FROM radius_sessions WHERE livre IS NOT NULL")
	if _, ok := bq.Where.(*binder.BoundNullCheck); !ok {
		t.Errorf("Where = %#v", bq.Where)
	}
}

// O binder não pode registrar coluna: uma query com erro de digitação não
// cria campo no dicionário.
func TestBindNaoRegistraColunaNova(t *testing.T) {
	sc := compile(t)
	before := len(sc.FieldsByID())

	st, _ := parser.Parse("SELECT naoexiste FROM radius_sessions")
	if _, err := binder.Bind(st, sc, nil); err == nil {
		t.Fatal("coluna inexistente deveria falhar")
	}
	if after := len(sc.FieldsByID()); after != before {
		t.Errorf("o bind registrou campo novo: %d → %d", before, after)
	}
}
