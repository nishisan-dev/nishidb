package server

import (
	"strconv"

	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocolo"
	"github.com/nishisan-dev/nishidb/internal/schema"
)

// materializacaoStats traduz o snapshot do materializer para o documento da
// rota, casando cada contagem de regra com a regra que a produziu.
//
// A spec entra junto porque o snapshot só tem contagens por ÍNDICE: publicar o
// índice sem o `when` mandaria quem lê o painel abrir o YAML para saber o que
// casou, e a ordem das regras é justamente o que decide qual vence.
func materializacaoStats(spec *schema.DatasetSpec, snap materializer.MetricsSnapshot) protocolo.MaterializacaoStats {
	out := protocolo.MaterializacaoStats{
		FrozenCaptured:  snap.CongeladasCapturadas,
		FrozenPreserved: snap.CongeladasPreservadas,
		Summed:          snap.Acumuladas,
		SummedSaturated: snap.Saturadas,
		TTLUnmatched:    snap.SemRegra,
	}
	regras := spec.Materialization.RegrasTTL
	if len(regras) == 0 {
		return out
	}
	out.TTLRules = make([]protocolo.RegraTTLStats, 0, len(regras))
	for i, r := range regras {
		var casou int64
		if i < len(snap.RegrasCasadas) {
			casou = snap.RegrasCasadas[i]
		}
		out.TTLRules = append(out.TTLRules, protocolo.RegraTTLStats{
			Index:            i,
			When:             r.When,
			ExpireAfterWrite: duracaoDaRegra(r),
			Matched:          casou,
		})
	}
	return out
}

// duracaoDaRegra rende a duração como o operador a escreveu: `never` sai como
// o literal, e não como "0s" — zero e imortal não são a mesma coisa, e um
// painel mostrando 0s diria o oposto do que a regra faz.
func duracaoDaRegra(r schema.RegraTTL) string {
	if r.Duracao == 0 {
		return schema.DuracaoNunca
	}
	return r.Duracao.String()
}

// amostrasDeMaterializacao publica as mesmas contagens como séries.
//
// Saem pela MESMA leitura do snapshot que alimenta o /v1/stats, e não de uma
// cópia própria: dois contadores para o mesmo fato divergem no dia em que um
// caminho de erro esquece de somar, e em produção alguém lê o errado.
func amostrasDeMaterializacao(dataset string, spec *schema.DatasetSpec, snap materializer.MetricsSnapshot) []metrics.Amostra {
	rotulo := []metrics.Label{{Nome: metrics.LabelDataset, Valor: dataset}}
	out := []metrics.Amostra{
		{
			Familia: metrics.MetFrozenCaptured, Nome: metrics.MetFrozenCaptured,
			Ajuda:  "valores capturados por coluna congelada na ingestão; sem preservação correspondente, a coluna nunca chegou a congelar",
			Tipo:   metrics.TipoContador,
			Labels: rotulo, Valor: float64(snap.CongeladasCapturadas),
		},
		{
			Familia: metrics.MetFrozenPreserved, Nome: metrics.MetFrozenPreserved,
			Ajuda:  "valores que o merge preservou por serem de coluna congelada",
			Tipo:   metrics.TipoContador,
			Labels: rotulo, Valor: float64(snap.CongeladasPreservadas),
		},
		{
			Familia: metrics.MetMergeSum, Nome: metrics.MetMergeSum,
			Ajuda:  "somas feitas pelo merge em coluna com merge: sum",
			Tipo:   metrics.TipoContador,
			Labels: rotulo, Valor: float64(snap.Acumuladas),
		},
		{
			Familia: metrics.MetMergeSumSaturated, Nome: metrics.MetMergeSumSaturated,
			Ajuda:  "somas que bateram no teto do int64 e pararam ali; a partir da primeira, o valor da coluna deixa de crescer",
			Tipo:   metrics.TipoContador,
			Labels: rotulo, Valor: float64(snap.Saturadas),
		},
		{
			Familia: metrics.MetIngestDecodeSerial, Nome: metrics.MetIngestDecodeSerial,
			Ajuda:  "eventos que o writer decodificou de novo, sozinho, porque o decode paralelo é somente-leitura e encontrou coluna a criar ou tipo a fixar; em regime é plana",
			Tipo:   metrics.TipoContador,
			Labels: rotulo, Valor: float64(snap.DecodeSerialTotal),
		},
		{
			Familia: metrics.MetTTLUnmatched, Nome: metrics.MetTTLUnmatched,
			Ajuda:  "escritas que não casaram com regra de ttl nenhuma; só acontece em dataset sem regra de fundo",
			Tipo:   metrics.TipoContador,
			Labels: rotulo, Valor: float64(snap.SemRegra),
		},
	}
	for i := range spec.Materialization.RegrasTTL {
		var casou int64
		if i < len(snap.RegrasCasadas) {
			casou = snap.RegrasCasadas[i]
		}
		out = append(out, metrics.Amostra{
			Familia: metrics.MetTTLRuleMatched, Nome: metrics.MetTTLRuleMatched,
			Ajuda:  "escritas decididas por cada regra de ttl; o rótulo é o ÍNDICE no YAML, porque é a ordem que decide qual vence",
			Tipo:   metrics.TipoContador,
			Labels: append([]metrics.Label{{Nome: "rule", Valor: strconv.Itoa(i)}}, rotulo...),
			Valor:  float64(casou),
		})
	}
	return out
}
