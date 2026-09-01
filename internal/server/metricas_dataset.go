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
//
// As três séries de compaction do §92 entram por aqui pelo mesmo motivo das
// outras: os números já existem no Store, que é quem varre e quem copia, e o
// coletor os LÊ no scrape em vez de manter cópia própria. Um contador mantido
// dos dois lados diverge no dia em que um caminho de erro esquece de somar, e
// em produção alguém lê o errado.
//
// Elas são publicadas SEMPRE, inclusive com storage.compaction.enabled: false
// e inclusive antes da primeira passada. Série que só aparece quando há o que
// mostrar vira lacuna no gráfico, e lacuna não se distingue de alvo fora do ar
// — zero é uma resposta, ausência não é.
func registraMetricasDeDataset(reg *metrics.Registry, s *Servidor) {
	reg.Coletor(metrics.ColetorFunc(func() []metrics.Amostra {
		out := make([]metrics.Amostra, 0, len(s.datasets)*16)
		for _, d := range s.datasets {
			st := d.st.Stats()
			// As políticas de escrita e as regras de ttl saem da MESMA leitura
			// do snapshot que alimenta o /v1/stats: dois contadores para o
			// mesmo fato divergem no dia em que um caminho de erro esquece de
			// somar, e em produção alguém lê o errado.
			// A tabela irmã não tem materializer — quem escreve nela é o do
			// dataset base. As séries de política de escrita saem no rótulo
			// de quem as aplicou.
			if d.mat != nil {
				out = append(out, amostrasDeMaterializacao(d.nome, d.spec, d.mat.Metrics())...)
			}
			out = append(out, amostrasDeHistorico(d)...)
			// As séries do índice por segmento saem para TODO dataset, e não só
			// para o histórico: o motor de índice é um só nos dois modos (D1), e
			// um materializado que responda por enumeração é a mesma degradação
			// pela mesma porta. Elas vêm de uma leitura própria porque a
			// EstatisticasDeRetencao não vale fora do histórico.
			out = append(out, amostrasDeIndice(d.nome, d.st.EstatisticasDeIndice())...)
			out = append(out, amostrasDeIsolamento(d.nome, d.st)...)
			rotulo := []metrics.Label{{Nome: metrics.LabelDataset, Valor: d.nome}}
			// O formato de exposição só tem número, e Go não converte bool
			// para float: o 0/1 do medidor se escreve aqui, uma vez, em vez de
			// dentro do literal da amostra.
			var rodando float64
			if st.CompactionRunning {
				rodando = 1
			}

			out = append(out,
				metrics.Amostra{
					Familia: metrics.MetDatasetFields,
					Nome:    metrics.MetDatasetFields,
					Ajuda:   "campos registrados no dicionário do dataset; o denominador é schema.maxCols, publicado em /v1/metadata",
					Tipo:    metrics.TipoMedidor,
					Labels:  rotulo,
					Valor:   float64(d.sch.CamposConhecidos()),
				},
				metrics.Amostra{
					Familia: metrics.MetDatasetFieldsDropped,
					Nome:    metrics.MetDatasetFieldsDropped,
					Ajuda:   "campos que o teto recusou; só sobe sob onMaxCols ignore, e é o que torna a perda contável em vez de silenciosa",
					Tipo:    metrics.TipoContador,
					Labels:  rotulo,
					Valor:   float64(d.sch.CamposDescartados()),
				},
				metrics.Amostra{
					Familia: metrics.MetNestedTypeMismatch,
					Nome:    metrics.MetNestedTypeMismatch,
					Ajuda:   "valores de coluna aninhada que não couberam no tipo fixado; o evento entrou e o documento guarda o valor, e a coluna foi REMOVIDA naquela linha — sem a remoção ela ficaria com o valor do evento anterior, no modo merge",
					Tipo:    metrics.TipoContador,
					Labels:  rotulo,
					Valor:   float64(d.sch.ValoresIncompativeis()),
				},
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
				},
				metrics.Amostra{
					Familia: metrics.MetCompactionRunning,
					Nome:    metrics.MetCompactionRunning,
					Ajuda:   "1 enquanto o laço de compaction do §53-55 está de pé para o dataset; 0 com a chave desligada ou depois de ele parar",
					Tipo:    metrics.TipoMedidor,
					Labels:  rotulo,
					Valor:   rodando,
				},
				metrics.Amostra{
					Familia: metrics.MetCompactionBytesRead,
					Nome:    metrics.MetCompactionBytesRead,
					Ajuda:   "bytes de record que as varreduras de compaction leram desde a abertura do dataset",
					Tipo:    metrics.TipoContador,
					Labels:  rotulo,
					Valor:   float64(st.CompactionBytesRead),
				},
				metrics.Amostra{
					Familia: metrics.MetCompactionBytesWritten,
					Nome:    metrics.MetCompactionBytesWritten,
					Ajuda:   "bytes que o copy-forward da compaction reescreveu; a distância para bytes_read é o disco devolvido (§86)",
					Tipo:    metrics.TipoContador,
					Labels:  rotulo,
					Valor:   float64(st.CompactionBytesWritten),
				},
				metrics.Amostra{
					Familia: metrics.MetExpiryRunning,
					Nome:    metrics.MetExpiryRunning,
					Ajuda:   "1 enquanto o coletor do idle timeout está de pé para o dataset; 0 sem expireAfterWrite declarado ou depois de ele parar",
					Tipo:    metrics.TipoMedidor,
					Labels:  rotulo,
					Valor:   umSeVerdadeiro(st.ExpiryRunning),
				},
				metrics.Amostra{
					Familia: metrics.MetExpiredTotal,
					Nome:    metrics.MetExpiredTotal,
					Ajuda:   "entidades que ganharam lápide por inatividade, pelas duas rotas: o coletor e a descoberta oportunista da compaction",
					Tipo:    metrics.TipoContador,
					Labels:  rotulo,
					Valor:   float64(st.Expiradas),
				},
				metrics.Amostra{
					Familia: metrics.MetExpiryGraceTotal,
					Nome:    metrics.MetExpiryGraceTotal,
					Ajuda:   "carências dadas a entidades sem instante de escrita; é o que sobe nos primeiros minutos depois de ligar a expiração num dataset já materializado, quando ainda não há nada a expirar",
					Tipo:    metrics.TipoContador,
					Labels:  rotulo,
					Valor:   float64(st.Carencias),
				},
				metrics.Amostra{
					Familia: metrics.MetExpiryScannedTotal,
					Nome:    metrics.MetExpiryScannedTotal,
					Ajuda:   "entradas do índice de escritas examinadas pelo coletor; comparada com o total de rows, diz se o cursor dá a volta ou empaca",
					Tipo:    metrics.TipoContador,
					Labels:  rotulo,
					Valor:   float64(st.ExpiryExaminadas),
				})
		}
		return out
	}))
}

// umSeVerdadeiro converte o booleano de estado para o 0/1 que um medidor
// Prometheus espera.
func umSeVerdadeiro(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

// amostrasDeIsolamento publica o que o snapshot de leitura está segurando
// neste dataset.
//
// As quatro nascem com ZERO na subida, como todas as que podem alarmar: uma
// série que só aparece quando há o que mostrar vira lacuna no gráfico, e
// lacuna não se distingue de alvo fora do ar.
//
// São lidas no SCRAPE, e não escritas na admissão, pela mesma razão da
// longest_running: a idade cresce sozinha com o tempo, e um medidor escrito
// quando a consulta abriu ficaria congelado mostrando a idade que ela tinha ao
// nascer — que é sempre zero.
func amostrasDeIsolamento(dataset string, st *storage.Store) []metrics.Amostra {
	rotulo := []metrics.Label{{Nome: metrics.LabelDataset, Valor: dataset}}
	return []metrics.Amostra{
		{
			Familia: metrics.MetQuerySnapshotBytes,
			Nome:    metrics.MetQuerySnapshotBytes,
			Ajuda:   "heap retido pelo journal de versões do isolamento de leitura",
			Tipo:    metrics.TipoMedidor,
			Valor:   float64(st.BytesRetidos()),
			Labels:  rotulo,
		},
		{
			Familia: metrics.MetQuerySnapshotOldestSeconds,
			Nome:    metrics.MetQuerySnapshotOldestSeconds,
			Ajuda:   "há quanto tempo a consulta mais antiga segura o passado; 0 quando não há nenhuma",
			Tipo:    metrics.TipoMedidor,
			Valor:   st.IdadeDoSnapshotMaisAntigo().Seconds(),
			Labels:  rotulo,
		},
		{
			Familia: metrics.MetQuerySnapshotExpiredTotal,
			Nome:    metrics.MetQuerySnapshotExpiredTotal,
			Ajuda:   "consultas invalidadas por o journal de versões estourar runtime.query.snapshotMaxBytes",
			Tipo:    metrics.TipoContador,
			Valor:   float64(st.SnapshotsExpirados()),
			Labels:  rotulo,
		},
		{
			Familia: metrics.MetCompactionSegmentosPinados,
			Nome:    metrics.MetCompactionSegmentosPinados,
			Ajuda:   "segmentos que a compaction não pode aposentar por guardarem versões de consultas em voo",
			Tipo:    metrics.TipoMedidor,
			Valor:   float64(st.SegmentosPinados()),
			Labels:  rotulo,
		},
	}
}
