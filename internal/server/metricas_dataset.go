package server

import "github.com/nishisan-dev/nishidb/internal/metrics"

// registraMetricasDeDataset publica as séries do §92 que saem do Store.Stats().
//
// Elas eram nome sem alimentador desde o M2 (débito 16): o Store sempre soube
// quantas linhas vivas e quantos segmentos tem, e nada expunha. A rota
// /v1/stats passou a precisar exatamente desses números, e publicar os dois
// pela MESMA leitura é o que impede o JSON e o painel de discordarem.
//
// É coletor PULL: lê no scrape, não copia estado. A lista de datasets é fixada
// na subida e não muda com o processo de pé, então percorrê-la aqui é seguro
// sem lock.
//
// O que NÃO entra: nishidb_storage_bytes, _live_bytes e _garbage_bytes. Os três
// dependem do tally de liveness por segmento, que hoje é limite SUPERIOR e não
// contagem exata (débitos 38 e 45) — publicá-los como se fossem exatos seria
// uma métrica que mente, e quem a lesse dimensionaria compaction por ela.
func registraMetricasDeDataset(reg *metrics.Registry, s *Servidor) {
	reg.Coletor(metrics.ColetorFunc(func() []metrics.Amostra {
		out := make([]metrics.Amostra, 0, len(s.datasets)*2)
		for _, d := range s.datasets {
			st := d.st.Stats()
			rotulo := []metrics.Label{{Nome: metrics.LabelDataset, Valor: d.nome}}
			out = append(out,
				metrics.Amostra{
					Familia: metrics.MetMaterializedRows,
					Nome:    metrics.MetMaterializedRows,
					Ajuda:   "linhas vivas materializadas no dataset",
					Tipo:    metrics.TipoMedidor,
					Labels:  rotulo,
					Valor:   float64(st.RowCount),
				},
				metrics.Amostra{
					Familia: metrics.MetSegmentsTotal,
					Nome:    metrics.MetSegmentsTotal,
					Ajuda:   "segmentos do dataset",
					Tipo:    metrics.TipoMedidor,
					Labels:  rotulo,
					Valor:   float64(st.SegmentsTotal),
				})
		}
		return out
	}))
}
