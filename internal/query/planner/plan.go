// Package planner escolhe a FONTE DE CANDIDATOS de uma query SQL v1.
//
// O princípio do qual tudo aqui deriva: o plano é apenas um SUPERSET do
// resultado. O executor SEMPRE reavalia o WHERE completo contra o estado
// corrente (spec §44) — inclusive no point lookup. Logo, toda dúvida degrada
// para FullScan e nunca para um plano indexado: falso positivo o executor
// filtra, falso negativo ninguém recupera.
//
// Consequências práticas dessa assimetria, todas materializadas em regra:
//
//   - o planner NÃO remove, reescreve nem simplifica a árvore WHERE. Ele a LÊ
//     e devolve um plano; a árvore inteira segue para o executor;
//   - um índice só é escolhido quando o schema o declara E o store confirma
//     que o atende (capability check). O conjunto de bitmaps é decidido na
//     criação do dataset, e um Open com AllowBitmapDrop abre o dataset sem um
//     índice que o YAML declara;
//   - a v1 não tem optimizer por custo (§63): nenhuma estimativa de
//     seletividade, nenhuma estatística. A prioridade é fixa —
//     primary → hash → bitmap → interseção/união → full scan.
//
// Três propriedades do binder das quais o planner DEPENDE:
//
//   - P1. BoundComparison.Val já está na representação binária DO TIPO DA
//     COLUNA — a mesma que o materializer gravou no payload e que o índice usa
//     como chave (a chave do values.dict inclui o byte de tipo). Quando o
//     binder promove tipos (coluna int64 × literal float → ambos double),
//     Val.Type != Field.Type e esse termo NÃO pode virar chave de índice;
//   - P2. parâmetros `?` já estão resolvidos em Val no bind (a v1 não tem
//     prepared statements), então `key = ?` casa a regra 1 no plan-time;
//   - P3. Field nunca é nil numa comparação bem-formada — o planner trata nil
//     defensivamente mesmo assim, degradando, nunca com panic.
package planner

import (
	"encoding/hex"
	"math"
	"strconv"
	"strings"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Plan é a FONTE DE CANDIDATOS escolhida para a query. É imutável e pode ser
// compartilhado entre goroutines — NUNCA guarde estado de execução (cursores,
// buffers, contadores) dentro de um plano.
type Plan interface {
	String() string
	isPlan()
}

// IndexTerm é um termo `campo = valor` resolvido para lookup indexado. Kind
// diz QUAL índice atende o termo.
type IndexTerm struct {
	Field *schema.CompiledField
	Val   types.Value
	Kind  schema.IndexKind
}

// PointLookupPlan: no máximo 1 candidato, via primary (Store.GetByKey).
// Key.Type é SEMPRE types.TypeString — a v1 só aceita key string, e o campo
// key nunca tem índice secundário, então não há disputa com as regras 2-4.
type PointLookupPlan struct {
	Field *schema.CompiledField
	Key   types.Value
}

// HashIndexPlan: Store.(HashLookuper).LookupHash(Field.ID, Val).
type HashIndexPlan struct {
	Field *schema.CompiledField
	Val   types.Value
}

// BitmapScanPlan: Store.LookupBitmap(Field.ID, Val).
type BitmapScanPlan struct {
	Field *schema.CompiledField
	Val   types.Value
}

// BitmapIntersectPlan: interseção dos iteradores. len(Terms) >= 2 sempre,
// todos com Kind == schema.IndexBitmap e sem duplicatas exatas.
type BitmapIntersectPlan struct {
	Terms []IndexTerm
}

// IndexUnionPlan: união COM DEDUP dos iteradores. len(Terms) >= 2 sempre;
// Kind pode misturar bitmap e hash.
type IndexUnionPlan struct {
	Terms []IndexTerm
}

// FullScanPlan: Store.ScanLive(). Struct VAZIA e SINGLETON — ver FullScan.
type FullScanPlan struct{}

// fullScan é compartilhado por todas as queries do processo. É seguro porque
// o plano não carrega estado; escrever nele seria corromper todas as outras.
var fullScan = &FullScanPlan{}

// FullScan devolve o singleton compartilhado. Existe para que o caminho sem
// WHERE — o mais comum em teste e no CLI — não aloque nada.
func FullScan() *FullScanPlan { return fullScan }

func (*PointLookupPlan) isPlan()     {}
func (*HashIndexPlan) isPlan()       {}
func (*BitmapScanPlan) isPlan()      {}
func (*BitmapIntersectPlan) isPlan() {}
func (*IndexUnionPlan) isPlan()      {}
func (*FullScanPlan) isPlan()        {}

// As formas abaixo são determinísticas: sem mapas, sem ordenação por endereço,
// e o nome do campo é SEMPRE Field.Name (nunca o alias), para que a mesma
// query escrita com alias ou com nome quoted produza a MESMA string.

func (*FullScanPlan) String() string { return "FullScan" }

func (p *PointLookupPlan) String() string {
	return "PointLookup(" + nomeCampo(p.Field) + "=" + formatValue(p.Key) + ")"
}

func (p *HashIndexPlan) String() string {
	return "HashIndex(" + nomeCampo(p.Field) + "=" + formatValue(p.Val) + ")"
}

func (p *BitmapScanPlan) String() string {
	return "BitmapScan(" + nomeCampo(p.Field) + "=" + formatValue(p.Val) + ")"
}

func (p *BitmapIntersectPlan) String() string {
	return "BitmapIntersect(" + termos(p.Terms, false) + ")"
}

// Só a UNIÃO prefixa o índice de cada termo: na interseção todos são bitmap
// por construção, e o prefixo seria ruído.
func (p *IndexUnionPlan) String() string {
	return "IndexUnion(" + termos(p.Terms, true) + ")"
}

func termos(ts []IndexTerm, comPrefixo bool) string {
	var b strings.Builder
	for i, t := range ts {
		if i > 0 {
			b.WriteString(", ")
		}
		if comPrefixo {
			switch t.Kind {
			case schema.IndexHash:
				b.WriteString("hash:")
			default:
				b.WriteString("bitmap:")
			}
		}
		b.WriteString(nomeCampo(t.Field))
		b.WriteByte('=')
		b.WriteString(formatValue(t.Val))
	}
	return b.String()
}

func nomeCampo(f *schema.CompiledField) string {
	if f == nil {
		return "?"
	}
	return f.Name
}

// formatValue renderiza o valor para diagnóstico. Os tipos de rede saem em
// HEX, e não formatados como IP, para não arrastar netip e a apresentação de
// tipos para dentro do planner — apresentação bonita é do encoder NDJSON.
func formatValue(v types.Value) string {
	switch v.Type {
	case types.TypeString:
		return strconv.Quote(string(v.Bytes))
	case types.TypeInt64, types.TypeTimestamp:
		return strconv.FormatInt(v.I64, 10)
	case types.TypeBool:
		if v.I64 != 0 {
			return "true"
		}
		return "false"
	case types.TypeDouble:
		return strconv.FormatFloat(math.Float64frombits(uint64(v.I64)), 'g', -1, 64)
	default:
		return "0x" + hex.EncodeToString(v.Bytes)
	}
}
