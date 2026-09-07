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
//
// UM método, e não mais o HasBitmap somado às capabilities OPCIONAIS
// HashLookuper e HashCapable, detectadas por type assertion no New: as três
// perguntavam a mesma coisa a um Store que tem um motor de índice só (spec
// §3.1). As opcionais eram um risco próprio — um Store que não as implementasse
// era tratado como "atende tudo", e a diferença entre "não sei responder" e
// "sim" só aparecia como erro em runtime no executor.
type Store interface {
	Indexed(field types.FieldID) bool
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
// query com os termos reordenados pode mudar de IndexScan para FullScan,
// porque o que conta é a profundidade do termo indexável na espinha. É um
// penhasco de performance silencioso; nunca de correção, porque degradar para
// full scan não perde row nenhuma.
const maxFlattenDepth = 64

// Planner é criado UMA VEZ por dataset (o Engine o guarda junto do schema
// compilado). Plan é seguro para uso concorrente: só lê campos imutáveis e
// incrementa contadores atômicos.
type Planner struct {
	store   Store
	metrics *Metrics // pode ser nil
}

// New guarda o store. store == nil é aceito e degrada tudo para FullScan.
func New(store Store, m *Metrics) *Planner {
	return &Planner{store: store, metrics: m}
}

// Plan escolhe a fonte de candidatos. NUNCA devolve nil, NUNCA devolve erro,
// NUNCA entra em panic: toda incerteza vira FullScan, que é sempre correto —
// só mais lento — porque o executor revalida o WHERE inteiro (§44).
//
// A ordem é a prioridade da spec §63, e ela é FIXA: primary, índice, união de
// OR na raiz, full scan. Não existe desempate — com um motor de índice só, não
// há "qual dos dois" a decidir.
//
// Os rótulos R1/R2/R5 que marcam os blocos abaixo são HISTÓRICOS, e não são a
// numeração do §63: eles vêm da tabela de regras do
// planning/m1-design-materializer-query.md §3.5, cuja ordem era 1 primary,
// 2 hash, 3 interseção de bitmaps, 4 bitmap scan e 5 união — a coluna de lá
// chama-se "Ordem" e não traz o prefixo R; o full scan era
// a linha "Caso contrário", sem rótulo. A R2 daqui é a fusão da R2 com a R3/R4
// de lá, e a R5 guardou o nome.
//
// Os números do §63 nunca coincidiram com esses rótulos (lá o bitmap era um
// passo só) e hoje coincidem menos ainda — a R5 é o passo 3 da lista de agora.
// Os rótulos ficam porque é por eles que a suíte e os comentários deste ciclo
// nomeiam as regras; quem for reconciliar as duas numerações mexe nos dois
// lados de uma vez, nunca só num.
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

	// R2 — índice. UMA regra, e é a colheita completa: antes eram duas, e a de
	// hash devolvia o PRIMEIRO termo elegível e parava — `node='x' AND type=2`
	// indexava node e revalidava type linha a linha. Varredura separada da R1
	// de propósito: a prioridade é da REGRA, não da posição no AST.
	// `nas_ip=x AND id=y` é point lookup.
	//
	// Termos na ordem do AST, deduplicados por (Field.ID, Val):
	// `status='a' AND status='a'` colapsa, mas `status='a' AND status='b'` NÃO
	// — são dois termos, a interseção é vazia, e é o resultado correto.
	var terms []IndexTerm
	for _, t := range eqs {
		if !p.indexavel(t.field) {
			continue
		}
		terms = appendDedup(terms, IndexTerm{Field: t.field, Val: t.val})
	}
	switch {
	case len(terms) >= 2:
		return p.conta(&IndexIntersectPlan{Terms: terms})
	case len(terms) == 1:
		return p.conta(&IndexScanPlan{Field: terms[0].Field, Val: terms[0].Val})
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
		if !p.indexavel(t.field) {
			return p.conta(FullScan())
		}
		terms = appendDedup(terms, IndexTerm{Field: t.field, Val: t.val})
	}
	// Um termo só depois do dedup não é união: colapsa para o scan.
	if len(terms) == 1 {
		return p.conta(&IndexScanPlan{Field: terms[0].Field, Val: terms[0].Val})
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

// indexavel: o schema declara E o store atende.
//
// O Indexado é obrigatório mesmo com o YAML dizendo `index: true` porque o
// conjunto indexado é o que a ABERTURA do dataset declarou, e nada garante que
// ele cubra o que o YAML pede — um Store aberto sem o campo é legítimo. Sem o
// check, o executor chamaria Lookup num campo sem índice, receberia
// ErrInvalidArgument e a query FALHARIA em vez de degradar.
func (p *Planner) indexavel(f *schema.CompiledField) bool {
	return f != nil && f.Indexado && p.store != nil && p.store.Indexed(f.ID)
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
	case *IndexScanPlan:
		p.metrics.IndexScan.Add(1)
	case *IndexIntersectPlan:
		p.metrics.IndexIntersect.Add(1)
	case *IndexUnionPlan:
		p.metrics.IndexUnion.Add(1)
	}
	return pl
}
