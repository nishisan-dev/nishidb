package planner

import (
	"bytes"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Store é a ÚNICA superfície do storage que o planner enxerga: CAPABILITY,
// nunca dado. *storage.Store e *fakestorage.Store a satisfazem.
type Store interface {
	HasBitmap(field types.FieldID) bool
}

// HashLookuper é a capability do índice hash (Fase 8, M4). NÃO existe no
// *storage.Store do M1; o dublê de teste implementa. A detecção é por type
// assertion, feita uma única vez no New.
type HashLookuper interface {
	LookupHash(field types.FieldID, v types.Value) (types.RowIDIterator, error)
}

// HashCapable é o capability check POR CAMPO do índice hash, análogo do
// HasBitmap. É opcional: um store que não a implementa é tratado como "atende
// todo campo declarado com index: hash".
type HashCapable interface {
	HasHash(field types.FieldID) bool
}

// maxFlattenDepth limita a recursão do achatamento. Estourá-lo não é erro: a
// subárvore vira um conjunct OPACO e a query degrada para FullScan — correto,
// só mais lento.
//
// NÃO é defesa em profundidade: o teto de PARÊNTESES do parser
// (parser.MaxExprDepth) não alcança a espinha de AND/OR, que é iterativa lá.
// Este budget é o ÚNICO limite dela, e é alcançável por SQL perfeitamente
// válido — o penhasco medido está em 67 conjuncts com o termo indexável no
// INÍCIO da cláusula (TestPenhascoDoBudgetDeFlatten).
//
// A consequência a conhecer: a fronteira é POSICIONAL, não de tamanho. A mesma
// query com os termos reordenados pode mudar de BitmapScan para FullScan,
// porque o que conta é a profundidade do termo indexável na espinha. É um
// penhasco de performance silencioso; nunca de correção, porque degradar para
// full scan não perde row nenhuma.
const maxFlattenDepth = 64

// Planner é criado UMA VEZ por dataset (o Engine o guarda junto do schema
// compilado). Plan é seguro para uso concorrente: só lê campos imutáveis e
// incrementa contadores atômicos.
type Planner struct {
	store   Store
	hash    HashLookuper // nil quando o store não tem a capability
	hashCap HashCapable  // nil quando o store não expõe HasHash
	metrics *Metrics     // pode ser nil
}

// New resolve as capabilities opcionais UMA vez, fora do caminho de query.
// store == nil é aceito e degrada tudo para FullScan.
func New(store Store, m *Metrics) *Planner {
	p := &Planner{store: store, metrics: m}
	if store != nil {
		p.hash, _ = store.(HashLookuper)
		p.hashCap, _ = store.(HashCapable)
	}
	return p
}

// Plan escolhe a fonte de candidatos. NUNCA devolve nil, NUNCA devolve erro,
// NUNCA entra em panic: toda incerteza vira FullScan, que é sempre correto —
// só mais lento — porque o executor revalida o WHERE inteiro (§44).
//
// A ordem das regras é a prioridade da spec §63. O empate "preferir hash a
// bitmap" já está codificado por ela; não existe lógica extra de desempate.
func (p *Planner) Plan(bq *binder.BoundQuery) Plan {
	if bq == nil || bq.Where == nil {
		return p.conta(FullScan())
	}

	conjuncts := flattenAnd(bq.Where, nil, 0)
	eqs := make([]eqTerm, 0, len(conjuncts))
	for _, c := range conjuncts {
		if t, ok := eqTermOf(c); ok {
			eqs = append(eqs, t)
		}
	}

	// R1 — primary. O campo key nunca tem índice secundário (o YAML recusa
	// `index:` nele), então não há disputa. Dois eq sobre a key: vence o
	// PRIMEIRO; o segundo conjunct derruba a row na revalidação e o resultado
	// vazio é o correto.
	for _, t := range eqs {
		if t.field.IsKey {
			return p.conta(&PointLookupPlan{Field: t.field, Key: t.val})
		}
	}

	// R2 — hash. Varredura separada da R1 de propósito: a prioridade é da
	// REGRA, não da posição no AST. `nas_ip=x AND id=y` é point lookup.
	for _, t := range eqs {
		if p.hashEligible(t.field) {
			return p.conta(&HashIndexPlan{Field: t.field, Val: t.val})
		}
	}

	// R3/R4 — bitmap. Termos na ordem do AST, deduplicados por (Field.ID,
	// Val): `status='a' AND status='a'` colapsa, mas `status='a' AND
	// status='b'` NÃO — são dois termos, a interseção é vazia, e é o
	// resultado correto.
	var terms []IndexTerm
	for _, t := range eqs {
		if !p.bitmapEligible(t.field) {
			continue
		}
		terms = appendDedup(terms, IndexTerm{Field: t.field, Val: t.val, Kind: schema.IndexBitmap})
	}
	switch {
	case len(terms) >= 2:
		return p.conta(&BitmapIntersectPlan{Terms: terms})
	case len(terms) == 1:
		return p.conta(&BitmapScanPlan{Field: terms[0].Field, Val: terms[0].Val})
	}

	// R5 — OR na raiz. Só se aplica quando o flatten de AND produziu um único
	// conjunct e ele é um nó OR: com AND no topo haveria >= 2 conjuncts.
	if len(conjuncts) == 1 {
		if l, ok := conjuncts[0].(*binder.BoundLogical); ok && l.Op == parser.LogicalOr {
			return p.uniao(l)
		}
	}

	return p.conta(FullScan())
}

// uniao aplica a regra 5. REGRA DURA: se QUALQUER disjunct não for indexável,
// o OR inteiro degrada. Uma união parcial perderia candidatos e produziria
// resultado silenciosamente incompleto — o pior defeito possível nesta engine.
// Não existe meia união.
func (p *Planner) uniao(raiz *binder.BoundLogical) Plan {
	disjuncts := flattenOr(raiz, nil, 0)
	terms := make([]IndexTerm, 0, len(disjuncts))
	for _, d := range disjuncts {
		t, ok := eqTermOf(d)
		if !ok {
			return p.conta(FullScan())
		}
		switch {
		case p.hashEligible(t.field):
			terms = appendDedup(terms, IndexTerm{Field: t.field, Val: t.val, Kind: schema.IndexHash})
		case p.bitmapEligible(t.field):
			terms = appendDedup(terms, IndexTerm{Field: t.field, Val: t.val, Kind: schema.IndexBitmap})
		default:
			return p.conta(FullScan())
		}
	}
	// Um termo só depois do dedup não é união: colapsa para o plano simples do
	// índice que o atende.
	if len(terms) == 1 {
		if terms[0].Kind == schema.IndexHash {
			return p.conta(&HashIndexPlan{Field: terms[0].Field, Val: terms[0].Val})
		}
		return p.conta(&BitmapScanPlan{Field: terms[0].Field, Val: terms[0].Val})
	}
	return p.conta(&IndexUnionPlan{Terms: terms})
}

// eqTerm é um `campo = valor` que serve de chave de índice.
type eqTerm struct {
	field *schema.CompiledField
	val   types.Value
}

// eqTermOf classifica um conjunct. As condições são cumulativas e cada uma tem
// uma razão:
//
//  1. só *binder.BoundComparison — IS NULL nunca é termo de igualdade, porque
//     não existe posting para valor AUSENTE (NULL = ausência);
//  2. só `=`. Range e `!=` não usam índice na v1;
//  3. Field != nil (defesa; o binder não produz isso);
//  4. Field.ID != 0 — o 0 é reservado no dicionário;
//  5. campo auto nunca é elegível: o YAML proíbe `index:` em auto, então
//     nenhum campo auto tem índice. Depois da QP o binder ACEITA comparação
//     sobre campo auto com tipo fixado, e ela chega aqui — degradando, que é
//     correto: não há índice a consultar;
//  6. Val.Type == Field.Type — a condição de CORREÇÃO mais importante do
//     pacote. A chave do índice inclui o byte de TIPO, então um valor
//     promovido (coluna int64 comparada a literal float vira double dos dois
//     lados) jamais casa com os postings gravados: o lookup devolveria vazio e
//     a query mentiria com resultado incompleto;
//  7. Val.Type != TypeNull — não há posting para "sem valor".
func eqTermOf(e binder.BoundExpr) (eqTerm, bool) {
	c, ok := e.(*binder.BoundComparison)
	if !ok || c.Op != parser.CmpEq {
		return eqTerm{}, false
	}
	f := c.Field
	if f == nil || f.ID == 0 || f.IsAuto || f.Type == types.TypeNull {
		return eqTerm{}, false
	}
	if c.Val.Type != f.Type || c.Val.Type == types.TypeNull {
		return eqTerm{}, false
	}
	return eqTerm{field: f, val: c.Val}, true
}

// bitmapEligible: o schema declara E o store atende.
//
// O HasBitmap é obrigatório mesmo com o YAML dizendo `index: bitmap` porque o
// conjunto indexado é o que a ABERTURA do dataset declarou, e nada garante que
// ele cubra o que o YAML pede — um Store aberto sem o campo é legítimo. Sem o
// check, o executor chamaria LookupBitmap num campo sem índice, receberia
// ErrInvalidArgument e a query FALHARIA em vez de degradar.
func (p *Planner) bitmapEligible(f *schema.CompiledField) bool {
	return f != nil && f.Index == schema.IndexBitmap && p.store != nil && p.store.HasBitmap(f.ID)
}

// hashEligible: o schema declara E o store tem a capability da Fase 8 E, se
// expuser o check por campo, ele confirma.
func (p *Planner) hashEligible(f *schema.CompiledField) bool {
	if f == nil || f.Index != schema.IndexHash || p.hash == nil {
		return false
	}
	return p.hashCap == nil || p.hashCap.HasHash(f.ID)
}

// flattenAnd colhe as folhas do spine de AND no topo da árvore. Um nó OR (ou
// qualquer outro nó) entra na lista INTEIRO, como conjunct OPACO — o que é
// correto: qualquer conjunct AND indexado é um superset válido, e o OR é
// resolvido na revalidação.
//
// A ordem léxica (esquerda→direita) é preservada: é ela que decide qual termo
// vence os empates, e é o que torna o plano determinístico.
func flattenAnd(e binder.BoundExpr, dst []binder.BoundExpr, depth int) []binder.BoundExpr {
	return flatten(e, parser.LogicalAnd, dst, depth)
}

func flattenOr(e binder.BoundExpr, dst []binder.BoundExpr, depth int) []binder.BoundExpr {
	return flatten(e, parser.LogicalOr, dst, depth)
}

func flatten(e binder.BoundExpr, op parser.LogicalOp, dst []binder.BoundExpr, depth int) []binder.BoundExpr {
	l, ok := e.(*binder.BoundLogical)
	if !ok || l.Op != op || depth > maxFlattenDepth {
		return append(dst, e)
	}
	dst = flatten(l.Left, op, dst, depth+1)
	return flatten(l.Right, op, dst, depth+1)
}

// appendDedup acrescenta o termo se ele já não estiver na lista. A varredura é
// O(n²) de propósito: a lista tem meia dúzia de elementos, e um map alocaria
// por consulta e ainda daria ordem não-determinística.
func appendDedup(ts []IndexTerm, novo IndexTerm) []IndexTerm {
	for _, t := range ts {
		if t.Field.ID == novo.Field.ID && sameValue(t.Val, novo.Val) {
			return ts
		}
	}
	return append(ts, novo)
}

// sameValue é igualdade EXATA de valor, incluindo o tipo — a mesma chave que o
// índice usa.
func sameValue(a, b types.Value) bool {
	return a.Type == b.Type && a.I64 == b.I64 && bytes.Equal(a.Bytes, b.Bytes)
}

// conta incrementa o contador do plano ESCOLHIDO e devolve o plano. É o único
// ponto de contagem do pacote: com um incremento espalhado por regra, a
// primeira regra nova esqueceria o seu, e `query.full_scan` — que é o sinal
// operacional de índice faltando — deixaria de ser confiável.
func (p *Planner) conta(pl Plan) Plan {
	if p.metrics == nil {
		return pl
	}
	switch pl.(type) {
	case *FullScanPlan:
		p.metrics.FullScan.Add(1)
	case *PointLookupPlan:
		p.metrics.PointLookup.Add(1)
	case *HashIndexPlan:
		p.metrics.HashIndex.Add(1)
	case *BitmapScanPlan:
		p.metrics.BitmapScan.Add(1)
	case *BitmapIntersectPlan:
		p.metrics.BitmapIntersect.Add(1)
	case *IndexUnionPlan:
		p.metrics.IndexUnion.Add(1)
	}
	return pl
}
