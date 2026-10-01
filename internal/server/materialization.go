package server

import (
	"strconv"

	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/schema"
)

// materializationStats traduz o snapshot do materializer para o documento da
// rota, casando cada contagem de regra com a regra que a produziu.
//
// A spec entra junto porque o snapshot só tem contagens por ÍNDICE: publicar o
// índice sem o `when` mandaria quem lê o painel abrir o YAML para saber o que
// casou, e a ordem das regras é justamente o que decide qual vence.
func materializationStats(spec *schema.DatasetSpec, snap materializer.MetricsSnapshot) protocol.MaterializationStats {
	out := protocol.MaterializationStats{
		FrozenCaptured:  snap.CongeladasCapturadas,
		FrozenPreserved: snap.CongeladasPreservadas,
		Summed:          snap.Acumuladas,
		SummedSaturated: snap.Saturadas,
		TTLUnmatched:    snap.TTLUnmatched,
	}
	regras := spec.Materialization.TTLRules
	if len(regras) == 0 {
		return out
	}
	out.TTLRules = make([]protocol.TTLRuleStats, 0, len(regras))
	for i, r := range regras {
		var casou int64
		if i < len(snap.TTLRuleMatches) {
			casou = snap.TTLRuleMatches[i]
		}
		out.TTLRules = append(out.TTLRules, protocol.TTLRuleStats{
			Index:            i,
			When:             r.When,
			ExpireAfterWrite: ruleDuration(r),
			Matched:          casou,
		})
	}
	return out
}

// ruleDuration rende a duração como o operador a escreveu: `never` sai como
// o literal, e não como "0s" — zero e imortal não são a mesma coisa, e um
// painel mostrando 0s diria o oposto do que a regra faz.
func ruleDuration(r schema.TTLRule) string {
	if r.Duration == 0 {
		return schema.DurationNever
	}
	return r.Duration.String()
}

// samplesOfMaterializacao publica as mesmas contagens como séries.
//
// Saem pela MESMA leitura do snapshot que alimenta o /v1/stats, e não de uma
// cópia própria: dois contadores para o mesmo fato divergem no dia em que um
// caminho de erro esquece de somar, e em produção alguém lê o errado.
func samplesOfMaterializacao(dataset string, spec *schema.DatasetSpec, snap materializer.MetricsSnapshot) []metrics.Sample {
	rotulo := []metrics.Label{{Name: metrics.LabelDataset, Value: dataset}}
	out := []metrics.Sample{
		{
			Family: metrics.MetFrozenCaptured, Name: metrics.MetFrozenCaptured,
			Help:   "valores capturados por coluna congelada na ingestão; sem preservação correspondente, a coluna nunca chegou a congelar",
			Type:   metrics.TypeCounter,
			Labels: rotulo, Value: float64(snap.CongeladasCapturadas),
		},
		{
			Family: metrics.MetFrozenPreserved, Name: metrics.MetFrozenPreserved,
			Help:   "valores que o merge preservou por serem de coluna congelada",
			Type:   metrics.TypeCounter,
			Labels: rotulo, Value: float64(snap.CongeladasPreservadas),
		},
		{
			Family: metrics.MetMergeSum, Name: metrics.MetMergeSum,
			Help:   "somas feitas pelo merge em coluna com merge: sum",
			Type:   metrics.TypeCounter,
			Labels: rotulo, Value: float64(snap.Acumuladas),
		},
		{
			Family: metrics.MetMergeSumSaturated, Name: metrics.MetMergeSumSaturated,
			Help:   "somas que bateram no teto do int64 e pararam ali; a partir da primeira, o valor da coluna deixa de crescer",
			Type:   metrics.TypeCounter,
			Labels: rotulo, Value: float64(snap.Saturadas),
		},
		{
			Family: metrics.MetIngestDecodeSerial, Name: metrics.MetIngestDecodeSerial,
			Help:   "eventos que o writer decodificou de novo, sozinho, porque o decode paralelo é somente-leitura e encontrou coluna a criar ou tipo a fixar; em regime é plana",
			Type:   metrics.TypeCounter,
			Labels: rotulo, Value: float64(snap.DecodeSerialTotal),
		},
		{
			Family: metrics.MetTTLUnmatched, Name: metrics.MetTTLUnmatched,
			Help:   "escritas que não casaram com regra de ttl nenhuma; só acontece em dataset sem regra de fundo",
			Type:   metrics.TypeCounter,
			Labels: rotulo, Value: float64(snap.TTLUnmatched),
		},
	}
	for i := range spec.Materialization.TTLRules {
		var casou int64
		if i < len(snap.TTLRuleMatches) {
			casou = snap.TTLRuleMatches[i]
		}
		out = append(out, metrics.Sample{
			Family: metrics.MetTTLRuleMatched, Name: metrics.MetTTLRuleMatched,
			Help:   "escritas decididas por cada regra de ttl; o rótulo é o ÍNDICE no YAML, porque é a ordem que decide qual vence",
			Type:   metrics.TypeCounter,
			Labels: append([]metrics.Label{{Name: "rule", Value: strconv.Itoa(i)}}, rotulo...),
			Value:  float64(casou),
		})
	}
	return out
}
