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
		FrozenCaptured:    snap.CongeladasCapturadas,
		FrozenPreserved:   snap.CongeladasPreservadas,
		Summed:            snap.Acumuladas,
		SummedSaturated:   snap.Saturadas,
		TTLUnmatched:      snap.TTLUnmatched,
		TTLAnchorInFuture: snap.TTLAnchorInFuture,
		TTLExpiredOnWrite: snap.TTLExpiredOnWrite,
	}
	rules := spec.Materialization.TTLRules
	if len(rules) == 0 {
		return out
	}
	out.TTLRules = make([]protocol.TTLRuleStats, 0, len(rules))
	for i, r := range rules {
		rs := protocol.TTLRuleStats{
			Index:   i,
			When:    r.When,
			Matched: at(snap.TTLRuleMatches, i),
		}
		if r.Anchor == "" {
			rs.ExpireAfterWrite = ruleDuration(r)
		} else {
			absent := at(snap.TTLAnchorAbsent, i)
			rs.Anchor, rs.ExpireAfter, rs.AnchorAbsent = r.Anchor, r.Duration.String(), &absent
		}
		out.TTLRules = append(out.TTLRules, rs)
	}
	return out
}

// at lê a contagem do índice, e zero quando o snapshot não a tem.
func at(counts []int64, i int) int64 {
	if i < len(counts) {
		return counts[i]
	}
	return 0
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

// samplesOfMaterialization publica as mesmas contagens como séries.
//
// Saem pela MESMA leitura do snapshot que alimenta o /v1/stats, e não de uma
// cópia própria: dois contadores para o mesmo fato divergem no dia em que um
// caminho de erro esquece de somar, e em produção alguém lê o errado.
//
// As séries de TTL nascem com zero: a de âncora ausente para cada regra
// ancorada, e as duas sem rótulo de regra em todo dataset — como a de
// escritas sem regra, que também existe em dataset sem regra nenhuma.
func samplesOfMaterialization(dataset string, spec *schema.DatasetSpec, snap materializer.MetricsSnapshot) []metrics.Sample {
	label := []metrics.Label{{Name: metrics.LabelDataset, Value: dataset}}
	out := []metrics.Sample{
		{
			Family: metrics.MetFrozenCaptured, Name: metrics.MetFrozenCaptured,
			Help:   "values captured by a frozen column at ingestion; without a matching preservation, the column never got to freeze",
			Type:   metrics.TypeCounter,
			Labels: label, Value: float64(snap.CongeladasCapturadas),
		},
		{
			Family: metrics.MetFrozenPreserved, Name: metrics.MetFrozenPreserved,
			Help:   "values the merge preserved because they belong to a frozen column",
			Type:   metrics.TypeCounter,
			Labels: label, Value: float64(snap.CongeladasPreservadas),
		},
		{
			Family: metrics.MetMergeSum, Name: metrics.MetMergeSum,
			Help:   "sums performed by the merge on columns with merge: sum",
			Type:   metrics.TypeCounter,
			Labels: label, Value: float64(snap.Acumuladas),
		},
		{
			Family: metrics.MetMergeSumSaturated, Name: metrics.MetMergeSumSaturated,
			Help:   "sums that hit the int64 ceiling and stopped there; from the first one on, the column value stops growing",
			Type:   metrics.TypeCounter,
			Labels: label, Value: float64(snap.Saturadas),
		},
		{
			Family: metrics.MetIngestDecodeSerial, Name: metrics.MetIngestDecodeSerial,
			Help:   "events the writer decoded again, alone, because the parallel decode is read-only and found a column to create or a type to fix; flat in steady state",
			Type:   metrics.TypeCounter,
			Labels: label, Value: float64(snap.DecodeSerialTotal),
		},
		{
			Family: metrics.MetTTLUnmatched, Name: metrics.MetTTLUnmatched,
			Help:   "writes no ttl rule decided — no condition matched, or an anchored rule's anchor was absent — and that therefore do not expire; only happens in a dataset without a fallback rule",
			Type:   metrics.TypeCounter,
			Labels: label, Value: float64(snap.TTLUnmatched),
		},
		{
			Family: metrics.MetTTLAnchorInFuture, Name: metrics.MetTTLAnchorInFuture,
			Help:   "anchors later than the write instant, clamped to it so the deadline is expireAfter counted from the write; a source with a clock ahead shows here",
			Type:   metrics.TypeCounter,
			Labels: label, Value: float64(snap.TTLAnchorInFuture),
		},
		{
			Family: metrics.MetTTLExpiredOnWrite, Name: metrics.MetTTLExpiredOnWrite,
			Help:   "documents written already expired (anchor plus expireAfter in the past); they leave queries right after the write and the collector tombstones them",
			Type:   metrics.TypeCounter,
			Labels: label, Value: float64(snap.TTLExpiredOnWrite),
		},
	}
	// Uma família de cada vez: as linhas de uma família saem juntas.
	rules := spec.Materialization.TTLRules
	for i := range rules {
		out = append(out, metrics.Sample{
			Family: metrics.MetTTLRuleMatched, Name: metrics.MetTTLRuleMatched,
			Help:   "writes decided by each ttl rule; the label is the INDEX in the YAML, because the order decides which one wins",
			Type:   metrics.TypeCounter,
			Labels: ruleLabel(i, label),
			Value:  float64(at(snap.TTLRuleMatches, i)),
		})
	}
	for i, r := range rules {
		if r.Anchor == "" {
			continue
		}
		out = append(out, metrics.Sample{
			Family: metrics.MetTTLAnchorAbsent, Name: metrics.MetTTLAnchorAbsent,
			Help:   "writes in which this anchored ttl rule would have decided and the anchor field was absent from the document, so the next rule was tried",
			Type:   metrics.TypeCounter,
			Labels: ruleLabel(i, label),
			Value:  float64(at(snap.TTLAnchorAbsent, i)),
		})
	}
	return out
}

// ruleLabel monta os rótulos de uma série por regra: o índice no YAML e o
// dataset.
func ruleLabel(i int, dataset []metrics.Label) []metrics.Label {
	return append([]metrics.Label{{Name: "rule", Value: strconv.Itoa(i)}}, dataset...)
}
