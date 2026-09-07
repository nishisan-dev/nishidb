package planner_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/query/planner"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/typeparse"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Fixture: o YAML REAL da spec §12, já commitado. Usar o arquivo de produção
// (e não um YAML de brinquedo) faz os casos de alias e de campo quoted serem
// rastreáveis já no plano.
const fixtureYAML = "../../../testdata/radius.materialization.yaml"

func compilar(tb testing.TB) *schema.Compiled {
	tb.Helper()
	spec, err := schema.Load(fixtureYAML, schema.DefaultsPadrao)
	if err != nil {
		tb.Fatal(err)
	}
	c, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		tb.Fatal(err)
	}
	return c
}

// campo resolve pelo nome ORIGINAL ou pelo alias — o binder já não distingue,
// e o planner nunca vê nome nenhum.
func campo(tb testing.TB, c *schema.Compiled, nome string) *schema.CompiledField {
	tb.Helper()
	f, ok := c.FieldByName([]byte(nome))
	if !ok {
		tb.Fatalf("campo %q não existe no fixture", nome)
	}
	return f
}

func fid(tb testing.TB, c *schema.Compiled, nomes ...string) []types.FieldID {
	tb.Helper()
	out := make([]types.FieldID, len(nomes))
	for i, n := range nomes {
		out[i] = campo(tb, c, n).ID
	}
	return out
}

// ---------------------------------------------------------------- valores

func str(s string) types.Value {
	return types.Value{Type: types.TypeString, Bytes: []byte(s)}
}

func i64(n int64) types.Value { return types.Value{Type: types.TypeInt64, I64: n} }

func ipv4(tb testing.TB, s string) types.Value {
	tb.Helper()
	a, b, c, d, ok := typeparse.IPv4([]byte(s))
	if !ok {
		tb.Fatalf("ipv4 inválido no teste: %q", s)
	}
	return types.Value{Type: types.TypeIPv4, Bytes: []byte{a, b, c, d}}
}

// ---------------------------------------------------------------- AST

func eq(f *schema.CompiledField, v types.Value) *binder.BoundComparison {
	return &binder.BoundComparison{Field: f, Op: parser.CmpEq, Val: v}
}

func cmp(f *schema.CompiledField, op parser.CmpOp, v types.Value) *binder.BoundComparison {
	return &binder.BoundComparison{Field: f, Op: op, Val: v}
}

func and(a, b binder.BoundExpr) *binder.BoundLogical {
	return &binder.BoundLogical{Op: parser.LogicalAnd, Left: a, Right: b}
}

func or(a, b binder.BoundExpr) *binder.BoundLogical {
	return &binder.BoundLogical{Op: parser.LogicalOr, Left: a, Right: b}
}

// consulta monta o BoundQuery à mão: o planner só lê Where, e montar o bound
// AST direto deixa cada caso legível sem acoplar o teste ao lexer/parser.
func consulta(where binder.BoundExpr) *binder.BoundQuery {
	return &binder.BoundQuery{Dataset: "radius_sessions", Where: where}
}

// ---------------------------------------------------------------- stores

// todosOsIndices é o store que atende tudo o que o fixture declara.
//
// A lista era passada em Bitmap E em Hash enquanto o dublê tinha dois
// conjuntos, porque o Store REAL respondia true nos dois para QUALQUER campo
// indexado e um dublê que só marcasse um divergiria dele. Com um conjunto só a
// paridade é estrutural: não existe mais como declarar um campo que só uma das
// capabilities enxerga, nem aqui nem no real.
func todosOsIndices(tb testing.TB, c *schema.Compiled) *fakestorage.Store {
	tb.Helper()
	todos := fid(tb, c, "Acct-Status-Type", "Framed-Protocol", "Acct-Terminate-Cause",
		"Filter-Id", "NAS-Port-Type", "Service-Type", "Acct-Authentic",
		"NAS-IP-Address", "NAS-Identifier", "Framed-IP-Address")
	return fakestorage.New(fakestorage.Indexes{Campos: todos})
}

// espiao falha o teste se o planner tocar em DADOS. Nenhum dos métodos de dado
// é alcançável de fato: a interface Store do planner tem apenas Indexado — mas
// o dublê os carrega para que uma ampliação futura da interface acuse aqui.
type espiao struct {
	t     *testing.T
	inner *fakestorage.Store
}

func (s *espiao) Indexed(f types.FieldID) bool { return s.inner.Indexed(f) }

func (s *espiao) Lookup(f types.FieldID, v types.Value) (types.RowIDIterator, error) {
	s.t.Fatalf("o planner consultou DADOS no plan-time: Lookup(%d)", f)
	return nil, nil
}

func (s *espiao) ScanLive() types.RowIDIterator {
	s.t.Fatal("o planner consultou DADOS no plan-time: ScanLive")
	return nil
}

// ---------------------------------------------------------------- asserções

func planeja(t *testing.T, p *planner.Planner, where binder.BoundExpr) planner.Plan {
	t.Helper()
	pl := p.Plan(consulta(where))
	if pl == nil {
		t.Fatal("Plan devolveu nil: toda incerteza vira FullScan, nunca nil")
	}
	return pl
}

// querString compara o plano pela sua forma canônica: pega tipo, campo, valor
// e ORDEM dos termos de uma vez.
func querString(t *testing.T, got planner.Plan, want string) {
	t.Helper()
	if s := got.String(); s != want {
		t.Errorf("plano = %s, want %s", s, want)
	}
}

func novo(t *testing.T) (*planner.Planner, *schema.Compiled, *planner.Metrics) {
	t.Helper()
	c := compilar(t)
	m := &planner.Metrics{}
	return planner.New(todosOsIndices(t, c), m), c, m
}

// ---------------------------------------------------------------- 1-4: key

func TestPlanSemWhereFullScan(t *testing.T) {
	p, _, m := novo(t)
	pl := p.Plan(&binder.BoundQuery{Dataset: "radius_sessions"})
	if _, ok := pl.(*planner.FullScanPlan); !ok {
		t.Fatalf("plano = %T, want *FullScanPlan", pl)
	}
	if got := m.FullScan.Load(); got != 1 {
		t.Errorf("query.full_scan = %d, want 1 (§64: é o sinal de índice faltando)", got)
	}
}

// SELECT ... WHERE id = 'CMP-1' — a primary vence tudo (§63).
func TestPlanKeyLiteralPointLookup(t *testing.T) {
	p, c, m := novo(t)
	pl := planeja(t, p, eq(campo(t, c, "id"), str("CMP-1")))
	if _, ok := pl.(*planner.PointLookupPlan); !ok {
		t.Fatalf("plano = %T, want *PointLookupPlan", pl)
	}
	querString(t, pl, `PointLookup(id="CMP-1")`)
	if m.FullScan.Load() != 0 {
		t.Error("query.full_scan contou num plano indexado")
	}
}

// `id = ?`: o parâmetro já vem resolvido do bind (a v1 não tem prepared
// statements), então o point lookup casa no plan-time.
func TestPlanKeyParamPointLookup(t *testing.T) {
	p, c, _ := novo(t)
	termo := eq(campo(t, c, "id"), str("CMP-PARAM"))
	termo.IsParam, termo.ParamIdx = true, 0

	pl := planeja(t, p, termo)
	pt, ok := pl.(*planner.PointLookupPlan)
	if !ok {
		t.Fatalf("plano = %T, want *PointLookupPlan", pl)
	}
	if string(pt.Key.Bytes) != "CMP-PARAM" {
		t.Errorf("Key = %q, want o valor do parâmetro", pt.Key.Bytes)
	}
}

// `status='Stop' AND (protocol='PPPoE' AND service_id='CMP-1')`: a key vence
// os bitmaps mesmo aninhada, e resolve pelo ALIAS.
func TestPlanKeyEmANDProfundoPointLookup(t *testing.T) {
	p, c, _ := novo(t)
	where := and(
		eq(campo(t, c, "status"), str("Stop")),
		and(eq(campo(t, c, "protocol"), str("PPPoE")), eq(campo(t, c, "service_id"), str("CMP-1"))),
	)
	querString(t, planeja(t, p, where), `PointLookup(id="CMP-1")`)
}

// A prioridade da §63 é da REGRA (R1 acima de R2), não da POSIÇÃO no
// AST: a key vence o índice secundário mesmo aparecendo depois dele. Sem este
// caso, trocar a ordem das regras não quebra teste nenhum — e o plano passa a
// ler o índice quando existe um lookup de no máximo uma row disponível.
//
// nas_ip é o campo que, antes da D5, tinha `index: hash` — o nome da variável
// ficou porque a query em si (id AND nas_ip, nas duas ordens) não muda: o que
// mudou foi só QUAL regra atende nas_ip — hoje a R2 única —, e R1 continua
// vencendo.
func TestPlanKeyVenceIndiceIndependenteDaOrdem(t *testing.T) {
	p, c, _ := novo(t)
	chave, nasIP := campo(t, c, "id"), campo(t, c, "nas_ip")
	ip := ipv4(t, "10.0.0.1")

	querString(t, planeja(t, p, and(eq(nasIP, ip), eq(chave, str("CMP-1")))), `PointLookup(id="CMP-1")`)
	querString(t, planeja(t, p, and(eq(chave, str("CMP-1")), eq(nasIP, ip))), `PointLookup(id="CMP-1")`)
}

// --------------------------------------------------- 5-8: a assimetria extinta
//
// As quatro regras 5-8 do §63 original testavam a ASSIMETRIA entre R2 (hash:
// devolve um plano e para) e R3/R4 (bitmap: intersecta): TestPlanHashVenceBitmap
// (R2 vence quando os dois se aplicam), TestPlanSemCapabilityHashDegradaPara-
// Bitmap e TestPlanSemCapabilityHashSemOutroIndiceFullScan (degradação da
// capability opcional HashLookuper) e TestPlanHasHashFalsoDegrada (o capability
// check por campo, HashCapable). A D5 tirou a grafia (`index: hash`) que
// alimentava a R2, e as quatro passaram a provar um caminho que este binário não
// alcançava mais por configuração nenhuma; esta tarefa apagou a própria R2 —
// junto com HashIndexPlan, HashLookuper e HashCapable —, e com ela a segunda
// regra que poderia atender um termo.
//
// TestPlanHashVenceBitmap sobrevive REPROPOSITADO: a mesma query (nas_ip AND
// status, os dois indexados) agora prova a intersecção que a D5 introduz — a
// consequência "para melhor" que a spec já registrava.

// TestDoisCamposIndexadosAgoraIntersectam: antes da D5, `nas_ip` era
// `index: hash` e a regra R2 devolvia UM plano (HashIndexPlan) e parava —
// `status` nunca entrava, e o executor revalidava `status` linha a linha sobre o
// superset do hash. Com `index: true` como grafia única os dois campos compilam
// para o mesmo estado (era IndexKind, hoje um bool) e caem na MESMA regra, que
// junta todos os termos elegíveis antes de decidir.
func TestDoisCamposIndexadosAgoraIntersectam(t *testing.T) {
	p, c, _ := novo(t)
	where := and(eq(campo(t, c, "nas_ip"), ipv4(t, "10.0.0.1")), eq(campo(t, c, "status"), str("Stop")))
	pl := planeja(t, p, where)
	bi, ok := pl.(*planner.IndexIntersectPlan)
	if !ok {
		t.Fatalf("plano = %T, want *IndexIntersectPlan", pl)
	}
	if len(bi.Terms) != 2 {
		t.Fatalf("termos = %d, want 2", len(bi.Terms))
	}
	querString(t, pl, `IndexIntersect(NAS-IP-Address=0x0a000001, Acct-Status-Type="Stop")`)
}

// ---------------------------------------------------------------- 9-14: índice

func TestPlanDoisIndicesIntersect(t *testing.T) {
	p, c, _ := novo(t)
	where := and(eq(campo(t, c, "status"), str("Stop")), eq(campo(t, c, "protocol"), str("PPPoE")))
	pl := planeja(t, p, where)
	bi, ok := pl.(*planner.IndexIntersectPlan)
	if !ok {
		t.Fatalf("plano = %T, want *IndexIntersectPlan", pl)
	}
	if len(bi.Terms) != 2 {
		t.Fatalf("termos = %d, want 2", len(bi.Terms))
	}
	querString(t, pl, `IndexIntersect(Acct-Status-Type="Stop", Framed-Protocol="PPPoE")`)
}

func TestPlanTresIndicesIntersect(t *testing.T) {
	p, c, _ := novo(t)
	where := and(
		and(eq(campo(t, c, "status"), str("Stop")), eq(campo(t, c, "protocol"), str("PPPoE"))),
		eq(campo(t, c, "terminate_cause"), str("NAS-Reboot")),
	)
	querString(t, planeja(t, p, where),
		`IndexIntersect(Acct-Status-Type="Stop", Framed-Protocol="PPPoE", Acct-Terminate-Cause="NAS-Reboot")`)
}

func TestPlanUmIndexScan(t *testing.T) {
	p, c, _ := novo(t)
	pl := planeja(t, p, eq(campo(t, c, "status"), str("Stop")))
	if _, ok := pl.(*planner.IndexScanPlan); !ok {
		t.Fatalf("plano = %T, want *IndexScanPlan", pl)
	}
}

// O conjunto indexado é o que a ABERTURA do dataset declarou, e ele pode não
// cobrir o que o YAML pede. Sem o capability check, o executor chamaria
// Lookup num campo sem índice e a query FALHARIA em vez de degradar.
func TestPlanBitmapSemCapabilityFullScan(t *testing.T) {
	c := compilar(t)
	semEsseBitmap := fakestorage.New(fakestorage.Indexes{Campos: fid(t, c, "Framed-Protocol")})
	p := planner.New(semEsseBitmap, nil)
	if pl := planeja(t, p, eq(campo(t, c, "status"), str("Stop"))); pl != planner.FullScan() {
		t.Fatalf("plano = %T, want FullScan", pl)
	}
}

func TestPlanBitmapDuplicadoDedupParaScan(t *testing.T) {
	p, c, _ := novo(t)
	where := and(eq(campo(t, c, "status"), str("Stop")), eq(campo(t, c, "status"), str("Stop")))
	pl := planeja(t, p, where)
	if _, ok := pl.(*planner.IndexScanPlan); !ok {
		t.Fatalf("plano = %T, want *IndexScanPlan (termo idêntico colapsa)", pl)
	}
}

// Mesmo campo com valores DIFERENTES não colapsa: são dois termos, a
// interseção é vazia, e isso é o resultado correto.
func TestPlanBitmapMesmoCampoValoresDiferentes(t *testing.T) {
	p, c, _ := novo(t)
	where := and(eq(campo(t, c, "status"), str("Stop")), eq(campo(t, c, "status"), str("Start")))
	pl := planeja(t, p, where)
	bi, ok := pl.(*planner.IndexIntersectPlan)
	if !ok || len(bi.Terms) != 2 {
		t.Fatalf("plano = %T, want *IndexIntersectPlan com 2 termos", pl)
	}
}

// ---------------------------------------------------------------- 15-23: OR

func TestPlanOrTodosIndexaveisUnion(t *testing.T) {
	p, c, _ := novo(t)
	where := or(eq(campo(t, c, "status"), str("Stop")), eq(campo(t, c, "protocol"), str("PPPoE")))
	pl := planeja(t, p, where)
	u, ok := pl.(*planner.IndexUnionPlan)
	if !ok || len(u.Terms) != 2 {
		t.Fatalf("plano = %T, want *IndexUnionPlan com 2 termos", pl)
	}
	// A conferência do Kind de cada termo saiu com o campo: o termo não nomeia
	// mais um motor, porque não há dois a nomear.
}

// Antes da D5 este caso misturava um termo bitmap com um hash NUM MESMO
// IndexUnionPlan, e a string do plano prefixava cada termo com o seu motor —
// prefixo que saiu junto com o campo Kind. A cobertura de "união com dois campos
// diferentes" continua existindo (TestPlanOrTodosIndexaveisUnion, com status e
// protocol); o que este caso acrescenta é nas_ip especificamente, que é o
// campo que a spec do piloto usava como exemplo de índice de alta
// cardinalidade.
func TestPlanOrDeDoisCamposIndexadosUniao(t *testing.T) {
	p, c, _ := novo(t)
	where := or(eq(campo(t, c, "status"), str("Stop")), eq(campo(t, c, "nas_ip"), ipv4(t, "10.0.0.1")))
	querString(t, planeja(t, p, where),
		`IndexUnion(Acct-Status-Type="Stop", NAS-IP-Address=0x0a000001)`)
}

// Risco #4: união PARCIAL perderia candidatos e devolveria resultado
// silenciosamente incompleto — o pior defeito possível nesta engine. É tudo ou
// nada.
func TestPlanOrComNaoIndexadoFullScan(t *testing.T) {
	p, c, m := novo(t)
	where := or(eq(campo(t, c, "status"), str("Stop")), eq(campo(t, c, "username"), str("ze")))
	if pl := planeja(t, p, where); pl != planner.FullScan() {
		t.Fatalf("plano = %T, want FullScan", pl)
	}
	if m.FullScan.Load() != 1 {
		t.Error("query.full_scan não contou")
	}
}

func TestPlanOrComComparadorNaoEqFullScan(t *testing.T) {
	p, c, _ := novo(t)
	where := or(eq(campo(t, c, "status"), str("Stop")), cmp(campo(t, c, "connect_info"), parser.CmpGt, i64(10)))
	if pl := planeja(t, p, where); pl != planner.FullScan() {
		t.Fatalf("plano = %T, want FullScan", pl)
	}
}

func TestPlanOrAninhadoEmOrFlatten(t *testing.T) {
	p, c, _ := novo(t)
	where := or(
		eq(campo(t, c, "status"), str("Stop")),
		or(eq(campo(t, c, "protocol"), str("PPPoE")), eq(campo(t, c, "terminate_cause"), str("NAS-Reboot"))),
	)
	pl := planeja(t, p, where)
	u, ok := pl.(*planner.IndexUnionPlan)
	if !ok || len(u.Terms) != 3 {
		t.Fatalf("plano = %T (%s), want *IndexUnionPlan com 3 termos", pl, pl)
	}
}

func TestPlanOrDuplicadoColapsaParaScan(t *testing.T) {
	p, c, _ := novo(t)
	where := or(eq(campo(t, c, "status"), str("Stop")), eq(campo(t, c, "status"), str("Stop")))
	pl := planeja(t, p, where)
	if _, ok := pl.(*planner.IndexScanPlan); !ok {
		t.Fatalf("plano = %T, want *IndexScanPlan (união de 1 termo colapsa)", pl)
	}
}

// O dedup da união é por (campo, VALOR), e o valor é comparado byte a byte.
//
// O caso que morde tem os dois valores com o MESMO COMPRIMENTO: um dedup que
// comparasse só o tamanho colapsaria a união num BitmapScan do primeiro termo
// e as linhas "Star" SUMIRIAM do resultado — falso negativo puro, silencioso,
// na query mais banal do domínio. O teste de valores diferentes que já existia
// usa 'Stop' (4) × 'Start' (5) e vive no caminho AND, onde o colapso seria
// benigno (superset mais largo); nenhum dos dois protege isto.
func TestPlanOrMesmoCampoValoresDeMesmoComprimento(t *testing.T) {
	p, c, _ := novo(t)
	status := campo(t, c, "Acct-Status-Type")
	pl := planeja(t, p, or(eq(status, str("Stop")), eq(status, str("Star"))))

	querString(t, pl, `IndexUnion(Acct-Status-Type="Stop", Acct-Status-Type="Star")`)
	u, ok := pl.(*planner.IndexUnionPlan)
	if !ok || len(u.Terms) != 2 {
		t.Fatalf("plano = %T, want IndexUnion com 2 termos", pl)
	}
}

// O colapso da união de um termo só. Antes da D5 este caso (chamado
// TestPlanOrDuplicadoDeHashColapsaParaHash) provava que ele respeitava o índice
// do termo, colapsando para HashIndex e não para BitmapScan. Com uma grafia só
// ele ficou idêntico, campo por campo, a TestPlanOrDuplicadoColapsaParaScan
// (união de 1 termo, mesmo campo, mesmo valor): a mesma asserção, sobre
// `status` em vez de `nas_ip`. Retirado por duplicar cobertura, não por perder
// alguma.

// OR aninhado DENTRO de AND: o conjunct indexado é um superset válido, e o OR
// inteiro é resolvido na revalidação do executor.
func TestPlanOrAninhadoDentroDeAndUsaOutroConjunct(t *testing.T) {
	p, c, _ := novo(t)
	where := and(
		eq(campo(t, c, "status"), str("Stop")),
		or(eq(campo(t, c, "protocol"), str("PPPoE")), eq(campo(t, c, "username"), str("ze"))),
	)
	querString(t, planeja(t, p, where), `IndexScan(Acct-Status-Type="Stop")`)
}

// Limitação registrada: grupo OR indexável aninhado em AND NÃO vira união na
// v1 (a regra 5 vale só na raiz). Seria correto usar um dos grupos como
// superset; simplicidade > performance (§127).
func TestPlanGrupoOrIndexavelDentroDeAndNaoUniao(t *testing.T) {
	p, c, _ := novo(t)
	where := and(
		or(eq(campo(t, c, "status"), str("Stop")), eq(campo(t, c, "protocol"), str("X"))),
		or(eq(campo(t, c, "terminate_cause"), str("Y")), eq(campo(t, c, "protocol"), str("Z"))),
	)
	if pl := planeja(t, p, where); pl != planner.FullScan() {
		t.Fatalf("plano = %T, want FullScan (limitação registrada)", pl)
	}
}

// Limitação registrada: a primary não é índice secundário, e a regra 5 exige
// bitmap/hash. `id='a' OR id='b'` não vira união de point lookups na v1.
func TestPlanKeyOrKeyFullScan(t *testing.T) {
	p, c, _ := novo(t)
	where := or(eq(campo(t, c, "id"), str("a")), eq(campo(t, c, "id"), str("b")))
	if pl := planeja(t, p, where); pl != planner.FullScan() {
		t.Fatalf("plano = %T, want FullScan (limitação registrada)", pl)
	}
}

// ---------------------------------------------------------------- 24-29

func TestPlanNaoEqFullScan(t *testing.T) {
	p, c, _ := novo(t)
	where := cmp(campo(t, c, "status"), parser.CmpNe, str("Stop"))
	if pl := planeja(t, p, where); pl != planner.FullScan() {
		t.Fatalf("plano = %T, want FullScan: != nunca usa índice na v1", pl)
	}
}

func TestPlanRangeFullScan(t *testing.T) {
	p, c, _ := novo(t)
	for _, op := range []parser.CmpOp{parser.CmpGt, parser.CmpGte, parser.CmpLt, parser.CmpLte} {
		where := cmp(campo(t, c, "connect_info"), op, i64(10))
		if pl := planeja(t, p, where); pl != planner.FullScan() {
			t.Errorf("op %v: plano = %T, want FullScan", op, pl)
		}
	}
}

// NULL é AUSÊNCIA: não existe posting para campo ausente.
func TestPlanIsNullFullScan(t *testing.T) {
	p, c, _ := novo(t)
	for _, not := range []bool{false, true} {
		where := &binder.BoundNullCheck{Field: campo(t, c, "status"), Not: not}
		if pl := planeja(t, p, where); pl != planner.FullScan() {
			t.Errorf("Not=%v: plano = %T, want FullScan", not, pl)
		}
	}
}

// O teste de correção mais importante da tarefa: a chave do índice inclui o
// BYTE DE TIPO. Um Value{TypeDouble} jamais casa com postings gravados como
// TypeInt64 — o lookup devolveria vazio e a query MENTIRIA. Por isso o termo
// só é indexável quando Val.Type == Field.Type.
func TestPlanTipoPromovidoNaoUsaIndice(t *testing.T) {
	spec, err := schema.Parse([]byte(vlanYAML), schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	c, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	vlan := campo(t, c, "vlan")
	store := fakestorage.New(fakestorage.Indexes{Campos: []types.FieldID{vlan.ID}})
	p := planner.New(store, nil)

	// Literal int: casa o tipo da coluna, usa o índice.
	if pl := planeja(t, p, eq(vlan, i64(10))); pl.String() != "IndexScan(vlan=10)" {
		t.Errorf("literal int: plano = %s, want IndexScan(vlan=10)", pl)
	}
	// Literal float sobre coluna int64: o binder promoveu os dois para double,
	// e esse valor NÃO existe no índice.
	promovido := types.Value{Type: types.TypeDouble, I64: 0x4024000000000000} // 10.0
	if pl := planeja(t, p, eq(vlan, promovido)); pl != planner.FullScan() {
		t.Fatalf("literal float: plano = %s, want FullScan", pl)
	}
}

const vlanYAML = `
version: 1
name: redes
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
    vlan:
      type: int64
      index: true
`

// Campo auto não tem índice (o YAML proíbe `index:` em auto), então mesmo com
// tipo fixado a comparação sobre ele degrada — não há índice a consultar.
func TestPlanCampoAutoNaoUsaIndice(t *testing.T) {
	p, c, _ := novo(t)
	auto, err := c.ResolveUnknown([]byte("X-Vendor-Attr"))
	if err != nil || auto == nil {
		t.Fatalf("ResolveUnknown: %v (%v)", auto, err)
	}
	if err := c.FixAutoType(auto, types.TypeString); err != nil {
		t.Fatal(err)
	}
	if pl := planeja(t, p, eq(auto, str("x"))); pl != planner.FullScan() {
		t.Fatalf("plano = %T, want FullScan", pl)
	}

	// O teste acima NÃO prova a guarda de IsAuto: pelo caminho normal um campo
	// auto tem Type == TypeNull, e é essa condição que o recusa — remover
	// `f.IsAuto` da classificação não quebra nada.
	//
	// O estado que a guarda protege não é produzível pelo Compile (campo auto
	// com tipo DECLARADO concreto e índice), e por isso é montado à mão: ela
	// existe para o dia em que alguém decidir gravar o tipo fixado em Type em
	// vez de autoType. Sem esta asserção, a guarda seria decoração.
	sintetico := &schema.CompiledField{
		ID: 999, Name: "sintetico", Type: types.TypeString,
		IsAuto: true, Indexado: true,
	}
	comIndice := planner.New(fakestorage.New(fakestorage.Indexes{
		Campos: []types.FieldID{sintetico.ID},
	}), nil)
	if pl := planeja(t, comIndice, eq(sintetico, str("x"))); pl != planner.FullScan() {
		t.Errorf("plano = %s, want FullScan: campo auto não é elegível a índice "+
			"mesmo com tipo declarado e índice disponível", pl)
	}
}

// Determinismo: com dois eq sobre a key, vence o PRIMEIRO da ordem léxica. O
// segundo conjunct derruba a row na revalidação e o resultado é vazio —
// correto.
func TestPlanDuasKeysPrimeiraVence(t *testing.T) {
	p, c, _ := novo(t)
	where := and(eq(campo(t, c, "id"), str("a")), eq(campo(t, c, "id"), str("b")))
	querString(t, planeja(t, p, where), `PointLookup(id="a")`)
}

// O budget de profundidade do flatten TEM consequência observável: um termo
// indexável abaixo do teto não é alcançado, e a query degrada. É o preço
// escolhido — recursão sem budget derruba o processo com uma AST patológica, e
// FullScan é sempre correto, só mais lento.
//
// Sem este caso o budget é invisível para os testes: uma AST de 200 níveis com
// folhas não indexáveis degrada de qualquer jeito.
func TestPlanProfundidadeAcimaDoBudgetDegrada(t *testing.T) {
	p, c, _ := novo(t)
	indexavel := eq(campo(t, c, "status"), str("Stop"))
	ruido := eq(campo(t, c, "username"), str("ze"))

	// Termo indexável a 10 níveis: dentro do budget, o flatten o alcança.
	var raso binder.BoundExpr = indexavel
	for i := 0; i < 10; i++ {
		raso = and(raso, ruido)
	}
	querString(t, planeja(t, p, raso), `IndexScan(Acct-Status-Type="Stop")`)

	// O MESMO termo a 200 níveis: a subárvore vira conjunct opaco.
	var fundo binder.BoundExpr = indexavel
	for i := 0; i < 200; i++ {
		fundo = and(fundo, ruido)
	}
	if pl := planeja(t, p, fundo); pl != planner.FullScan() {
		t.Errorf("plano = %s, want FullScan: acima do budget a subárvore é opaca", pl)
	}
}

// O penhasco do budget é EXATO e depende da POSIÇÃO do termo, não do tamanho
// da query: a mesma consulta com os termos reordenados muda de IndexScan para
// FullScan. O teste mede a fronteira em vez de afirmá-la, para que ela conste
// do código e não de um relatório.
func TestPenhascoDoBudgetDeFlatten(t *testing.T) {
	p, c, _ := novo(t)
	indexavel := eq(campo(t, c, "status"), str("Stop"))
	ruido := eq(campo(t, c, "username"), str("ze"))

	// Termo indexável no INÍCIO = o mais fundo da espinha left-leaning.
	noInicio := func(n int) binder.BoundExpr {
		var e binder.BoundExpr = indexavel
		for i := 1; i < n; i++ {
			e = and(e, ruido)
		}
		return e
	}
	achaPenhasco := func() int {
		for n := 2; n <= 512; n++ {
			if p.Plan(consulta(noInicio(n))) == planner.FullScan() {
				return n
			}
		}
		return -1
	}
	penhasco := achaPenhasco()
	if penhasco != 67 {
		t.Errorf("penhasco em %d conjuncts, want 67 (budget %d + a raiz + o off-by-one do depth)",
			penhasco, 64)
	}

	// Com o MESMO número de termos e o indexável no FIM (raso), não há
	// penhasco: é a prova de que a fronteira é posicional.
	var noFim binder.BoundExpr = ruido
	for i := 1; i < 500; i++ {
		noFim = and(noFim, ruido)
	}
	noFim = and(noFim, indexavel)
	if pl := planeja(t, p, noFim); pl == planner.FullScan() {
		t.Error("com o termo indexável raso, 500 conjuncts deveriam continuar indexados")
	}
}

// ---------------------------------------------------------------- 30-33

// O planner escolhe a fonte de candidatos; quem lê dado é o executor.
func TestPlanNaoTocaEmDados(t *testing.T) {
	c := compilar(t)
	sp := &espiao{t: t, inner: todosOsIndices(t, c)}
	p := planner.New(sp, nil)

	casos := []binder.BoundExpr{
		nil,
		eq(campo(t, c, "id"), str("CMP-1")),
		eq(campo(t, c, "nas_ip"), ipv4(t, "10.0.0.1")),
		eq(campo(t, c, "status"), str("Stop")),
		and(eq(campo(t, c, "status"), str("Stop")), eq(campo(t, c, "protocol"), str("PPPoE"))),
		or(eq(campo(t, c, "status"), str("Stop")), eq(campo(t, c, "nas_ip"), ipv4(t, "10.0.0.1"))),
		eq(campo(t, c, "username"), str("ze")),
	}
	for _, where := range casos {
		if pl := p.Plan(consulta(where)); pl == nil {
			t.Fatal("Plan devolveu nil")
		}
	}
}

func TestPlanDefensivo(t *testing.T) {
	c := compilar(t)
	m := &planner.Metrics{}
	p := planner.New(todosOsIndices(t, c), m)

	t.Run("bq nil", func(t *testing.T) {
		if pl := p.Plan(nil); pl != planner.FullScan() {
			t.Errorf("plano = %v", pl)
		}
	})
	t.Run("where nil", func(t *testing.T) {
		if pl := p.Plan(consulta(nil)); pl != planner.FullScan() {
			t.Errorf("plano = %v", pl)
		}
	})
	t.Run("store nil", func(t *testing.T) {
		semStore := planner.New(nil, m)
		if pl := planeja(t, semStore, eq(campo(t, c, "status"), str("Stop"))); pl != planner.FullScan() {
			t.Errorf("plano = %v", pl)
		}
	})
	t.Run("campo nil", func(t *testing.T) {
		where := &binder.BoundComparison{Op: parser.CmpEq, Val: str("Stop")}
		if pl := planeja(t, p, where); pl != planner.FullScan() {
			t.Errorf("plano = %v", pl)
		}
	})
	t.Run("null check com campo nil", func(t *testing.T) {
		if pl := planeja(t, p, &binder.BoundNullCheck{}); pl != planner.FullScan() {
			t.Errorf("plano = %v", pl)
		}
	})
	// Recursão sem budget derrubaria o processo com uma query patológica.
	t.Run("200 níveis de AND", func(t *testing.T) {
		var where binder.BoundExpr = eq(campo(t, c, "username"), str("ze"))
		for i := 0; i < 200; i++ {
			where = and(where, eq(campo(t, c, "username"), str("ze")))
		}
		if pl := planeja(t, p, where); pl != planner.FullScan() {
			t.Errorf("plano = %v", pl)
		}
	})
	t.Run("200 níveis de OR", func(t *testing.T) {
		var where binder.BoundExpr = eq(campo(t, c, "status"), str("Stop"))
		for i := 0; i < 200; i++ {
			where = or(where, eq(campo(t, c, "status"), str("Stop")))
		}
		if pl := planeja(t, p, where); pl == nil {
			t.Error("Plan devolveu nil")
		}
	})
	t.Run("metrics nil", func(t *testing.T) {
		semMetrics := planner.New(todosOsIndices(t, c), nil)
		if pl := semMetrics.Plan(nil); pl != planner.FullScan() {
			t.Errorf("plano = %v", pl)
		}
	})
}

// Cada plano incrementa o SEU contador e só o seu. A métrica de full scan é o
// sinal operacional de índice faltando: contá-la a mais ou a menos dispara (ou
// esconde) a investigação errada.
func TestPlanMetricas(t *testing.T) {
	c := compilar(t)
	casos := []struct {
		nome  string
		where binder.BoundExpr
		conta func(m *planner.Metrics) int64
	}{
		{"full scan", eq(campo(t, c, "username"), str("ze")),
			func(m *planner.Metrics) int64 { return m.FullScan.Load() }},
		{"point lookup", eq(campo(t, c, "id"), str("a")),
			func(m *planner.Metrics) int64 { return m.PointLookup.Load() }},
		// O caso "hash" saiu daqui na D5, quando nenhuma config passou a compilar
		// um campo para IndexKind hash e, portanto, a produzir um HashIndexPlan. O
		// `total` abaixo continuou somando m.HashIndex.Load() DE PROPÓSITO, para
		// que o dia em que o contador fosse apagado este teste acusasse por falha
		// de compilação em vez de silenciar. Foi o que aconteceu: a parcela saiu
		// junto com o contador, e a soma abaixo é DELIBERADAMENTE de cinco
		// parcelas em vez de seis.
		{"index scan", eq(campo(t, c, "status"), str("Stop")),
			func(m *planner.Metrics) int64 { return m.IndexScan.Load() }},
		{"intersect", and(eq(campo(t, c, "status"), str("Stop")), eq(campo(t, c, "protocol"), str("P"))),
			func(m *planner.Metrics) int64 { return m.IndexIntersect.Load() }},
		{"union", or(eq(campo(t, c, "status"), str("Stop")), eq(campo(t, c, "protocol"), str("P"))),
			func(m *planner.Metrics) int64 { return m.IndexUnion.Load() }},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			m := &planner.Metrics{}
			p := planner.New(todosOsIndices(t, c), m)
			p.Plan(consulta(tc.where))

			if got := tc.conta(m); got != 1 {
				t.Errorf("contador do plano = %d, want 1", got)
			}
			total := m.FullScan.Load() + m.PointLookup.Load() +
				m.IndexScan.Load() + m.IndexIntersect.Load() + m.IndexUnion.Load()
			if total != 1 {
				t.Errorf("total de contadores = %d, want 1: só o plano escolhido conta", total)
			}
		})
	}
}

func TestPlanNuncaDevolveNil(t *testing.T) {
	c := compilar(t)
	p := planner.New(todosOsIndices(t, c), &planner.Metrics{})
	casos := []binder.BoundExpr{
		nil,
		eq(campo(t, c, "id"), str("a")),
		eq(campo(t, c, "nas_ip"), ipv4(t, "10.0.0.1")),
		eq(campo(t, c, "status"), str("Stop")),
		and(eq(campo(t, c, "status"), str("S")), eq(campo(t, c, "protocol"), str("P"))),
		or(eq(campo(t, c, "status"), str("S")), eq(campo(t, c, "protocol"), str("P"))),
		or(eq(campo(t, c, "status"), str("S")), eq(campo(t, c, "username"), str("z"))),
		cmp(campo(t, c, "status"), parser.CmpNe, str("S")),
		&binder.BoundNullCheck{Field: campo(t, c, "status")},
		&binder.BoundComparison{Op: parser.CmpEq, Val: str("x")},
		and(nil, nil),
		or(nil, nil),
	}
	for i, where := range casos {
		if pl := p.Plan(consulta(where)); pl == nil {
			t.Errorf("caso %d: Plan devolveu nil", i)
		}
	}
}

// Um Planner é criado uma vez por dataset e usado por todas as queries: só lê
// campos imutáveis e incrementa contadores atômicos.
func TestPlanConcorrente(t *testing.T) {
	c := compilar(t)
	m := &planner.Metrics{}
	p := planner.New(todosOsIndices(t, c), m)
	where := and(eq(campo(t, c, "status"), str("Stop")), eq(campo(t, c, "protocol"), str("PPPoE")))

	const goroutines, porGoroutine = 8, 100
	pronto := make(chan struct{})
	fim := make(chan string, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			<-pronto
			var último string
			for j := 0; j < porGoroutine; j++ {
				último = p.Plan(consulta(where)).String()
			}
			fim <- último
		}()
	}
	close(pronto)
	for i := 0; i < goroutines; i++ {
		if got := <-fim; got != `IndexIntersect(Acct-Status-Type="Stop", Framed-Protocol="PPPoE")` {
			t.Errorf("plano concorrente = %s", got)
		}
	}
	if got := m.IndexIntersect.Load(); got != goroutines*porGoroutine {
		t.Errorf("contador = %d, want %d", got, goroutines*porGoroutine)
	}
}
