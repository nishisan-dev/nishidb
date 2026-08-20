package server

import (
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

// lixoDeDisco é a conta de nishidb_storage_garbage_bytes: o que está em disco
// menos o que o tally reivindica como vivo, com piso em zero.
//
// Está fora do coletor por um motivo só, e ele é de prova: dentro do closure a
// conta só é alcançável com um servidor de pé, e um servidor de teste sobe de
// manifest já checkpointado e não recebe Put pela rota — ali o disco está
// SEMPRE à frente do vivo e o ramo do piso nunca roda. Aqui ela é tabela, e os
// dois lados da desigualdade entram.
//
// O piso não é defensividade vaga. StorageBytes é do último ListSegments, que
// só roda no checkpoint, enquanto o tally anda a cada Put: entre dois
// checkpoints o vivo passa do disco POR CONSTRUÇÃO. E a subtração é em uint64,
// que não tem negativo — sem piso, um vivo 64 bytes à frente não publicaria
// "-64" e sim 1,8e19, um medidor de bytes com 18 exabytes de lixo disparando
// compaction contra um dataset saudável. A guarda `> 0` é a mesma defesa um
// passo antes: ela não existe pelo número negativo, existe pela CONVERSÃO de
// int64 negativo para uint64, que também dá a volta.
//
// A direção do erro é o que se garante e vale sempre: como o vivo é limite
// SUPERIOR (débitos 38 e 45), a diferença é limite INFERIOR do lixo — "há pelo
// menos isto de garbage", que é o que a Ajuda da série diz ao operador.
func lixoDeDisco(st storage.Stats) uint64 {
	if st.StorageBytes > 0 && uint64(st.StorageBytes) > st.LiveBytesUpperBound {
		return uint64(st.StorageBytes) - st.LiveBytesUpperBound
	}
	return 0
}

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
// O que NÃO entra, e agora é UMA só: nishidb_storage_live_bytes. O M2 deixou
// as três séries de disco de fora com o mesmo argumento — o tally de liveness
// é limite SUPERIOR e não contagem exata (débitos 38 e 45), e publicá-lo como
// exato seria métrica que mente. O argumento sobrevive inteiro para
// _live_bytes, que é o tally cru sob um nome que promete exatidão, e ela só
// sai quando a compaction do §53-55 reconstruir o tally por varredura. Mas ele
// nunca alcançou as outras duas:
//
//   - nishidb_storage_bytes não depende do tally. É soma de tamanho de
//     arquivo, medida pelo ListSegments, e é a série que antecede o ENOSPC —
//     o §86 dá 2 a 3 dias de margem a 500 ev/s, curto demais para descobrir
//     pelo alerta do filesystem. A ressalva dela é de FRESCOR, não de
//     exatidão, e está escrita no campo Stats.StorageBytes.
//   - nishidb_storage_garbage_bytes é alcançada pela METADE, e a metade que
//     sobra é publicável: como o vivo é limite superior, `total - vivo` é
//     limite INFERIOR do lixo. "Há pelo menos isto de garbage" é afirmação
//     verdadeira e acionável, e a ajuda a carrega junto do número — é ela que
//     impede a próxima leitura de tratar o valor como exato.
func registraMetricasDeDataset(reg *metrics.Registry, s *Servidor) {
	reg.Coletor(metrics.ColetorFunc(func() []metrics.Amostra {
		out := make([]metrics.Amostra, 0, len(s.datasets)*4)
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
				},
				metrics.Amostra{
					Familia: metrics.MetStorageBytes,
					Nome:    metrics.MetStorageBytes,
					Ajuda:   "bytes ocupados pelos segmentos do dataset em disco, medidos no último checkpoint",
					Tipo:    metrics.TipoMedidor,
					Labels:  rotulo,
					Valor:   float64(st.StorageBytes),
				},
				metrics.Amostra{
					Familia: metrics.MetStorageGarbageBytes,
					Nome:    metrics.MetStorageGarbageBytes,
					Ajuda:   "bytes de garbage nos segmentos: há pelo menos isto de lixo (limite INFERIOR, porque a liveness é limite superior — débitos 38 e 45)",
					Tipo:    metrics.TipoMedidor,
					Labels:  rotulo,
					Valor:   float64(lixoDeDisco(st)),
				})
		}
		return out
	}))
}
