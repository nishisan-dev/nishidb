package server

import (
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
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
	reg.Register(metrics.CollectorFunc(func() []metrics.Sample {
		out := make([]metrics.Sample, 0, len(s.datasets)*16)
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
			out = append(out, amostrasDeContadores(d.nome, d.st.EstatisticasDeContadores())...)
			rotulo := []metrics.Label{{Name: metrics.LabelDataset, Value: d.nome}}
			// O formato de exposição só tem número, e Go não converte bool
			// para float: o 0/1 do medidor se escreve aqui, uma vez, em vez de
			// dentro do literal da amostra.
			var rodando float64
			if st.CompactionRunning {
				rodando = 1
			}

			out = append(out,
				metrics.Sample{
					Family: metrics.MetDatasetFields,
					Name:   metrics.MetDatasetFields,
					Help:   "campos registrados no dicionário do dataset; o denominador é schema.maxCols, publicado em /v1/metadata",
					Type:   metrics.TypeGauge,
					Labels: rotulo,
					Value:  float64(d.sch.KnownFieldCount()),
				},
				metrics.Sample{
					Family: metrics.MetDatasetFieldsDropped,
					Name:   metrics.MetDatasetFieldsDropped,
					Help:   "campos que o teto recusou; só sobe sob onMaxCols ignore, e é o que torna a perda contável em vez de silenciosa",
					Type:   metrics.TypeCounter,
					Labels: rotulo,
					Value:  float64(d.sch.DiscardedFieldCount()),
				},
				metrics.Sample{
					Family: metrics.MetNestedTypeMismatch,
					Name:   metrics.MetNestedTypeMismatch,
					Help:   "valores de coluna aninhada que não couberam no tipo fixado; o evento entrou e o documento guarda o valor, e a coluna foi REMOVIDA naquela linha — sem a remoção ela ficaria com o valor do evento anterior, no modo merge",
					Type:   metrics.TypeCounter,
					Labels: rotulo,
					Value:  float64(d.sch.IncompatibleValueCount()),
				},
				metrics.Sample{
					Family: metrics.MetMaterializedRows,
					Name:   metrics.MetMaterializedRows,
					Help:   "linhas vivas materializadas no dataset",
					Type:   metrics.TypeGauge,
					Labels: rotulo,
					Value:  float64(st.RowCount),
				},
				metrics.Sample{
					Family: metrics.MetSegmentsTotal,
					Name:   metrics.MetSegmentsTotal,
					Help:   "segmentos do dataset",
					Type:   metrics.TypeGauge,
					Labels: rotulo,
					Value:  float64(st.SegmentsTotal),
				},
				metrics.Sample{
					Family: metrics.MetStorageBytes,
					Name:   metrics.MetStorageBytes,
					Help:   "bytes ocupados pelos segmentos do dataset em disco, medidos no último checkpoint",
					Type:   metrics.TypeGauge,
					Labels: rotulo,
					Value:  float64(st.StorageBytes),
				},
				metrics.Sample{
					Family: metrics.MetStorageGarbageBytes,
					Name:   metrics.MetStorageGarbageBytes,
					Help:   "bytes de garbage nos segmentos: há pelo menos isto de lixo (limite INFERIOR, porque a liveness é limite superior — débitos 38 e 45)",
					Type:   metrics.TypeGauge,
					Labels: rotulo,
					Value:  float64(lixoDeDisco(st)),
				},
				metrics.Sample{
					Family: metrics.MetCompactionRunning,
					Name:   metrics.MetCompactionRunning,
					Help:   "1 enquanto o laço de compaction do §53-55 está de pé para o dataset; 0 com a chave desligada ou depois de ele parar",
					Type:   metrics.TypeGauge,
					Labels: rotulo,
					Value:  rodando,
				},
				metrics.Sample{
					Family: metrics.MetCompactionBytesRead,
					Name:   metrics.MetCompactionBytesRead,
					Help:   "bytes de record que as varreduras de compaction leram desde a abertura do dataset",
					Type:   metrics.TypeCounter,
					Labels: rotulo,
					Value:  float64(st.CompactionBytesRead),
				},
				metrics.Sample{
					Family: metrics.MetCompactionBytesWritten,
					Name:   metrics.MetCompactionBytesWritten,
					Help:   "bytes que o copy-forward da compaction reescreveu; a distância para bytes_read é o disco devolvido (§86)",
					Type:   metrics.TypeCounter,
					Labels: rotulo,
					Value:  float64(st.CompactionBytesWritten),
				},
				metrics.Sample{
					Family: metrics.MetExpiryRunning,
					Name:   metrics.MetExpiryRunning,
					Help:   "1 enquanto o coletor do idle timeout está de pé para o dataset; 0 sem expireAfterWrite declarado ou depois de ele parar",
					Type:   metrics.TypeGauge,
					Labels: rotulo,
					Value:  umSeVerdadeiro(st.ExpiryRunning),
				},
				metrics.Sample{
					Family: metrics.MetExpiredTotal,
					Name:   metrics.MetExpiredTotal,
					Help:   "entidades que ganharam lápide por inatividade, pelas duas rotas: o coletor e a descoberta oportunista da compaction",
					Type:   metrics.TypeCounter,
					Labels: rotulo,
					Value:  float64(st.ExpiredCount),
				},
				metrics.Sample{
					Family: metrics.MetExpiryGraceTotal,
					Name:   metrics.MetExpiryGraceTotal,
					Help:   "carências dadas a entidades sem instante de escrita; é o que sobe nos primeiros minutos depois de ligar a expiração num dataset já materializado, quando ainda não há nada a expirar",
					Type:   metrics.TypeCounter,
					Labels: rotulo,
					Value:  float64(st.PendingCount),
				},
				metrics.Sample{
					Family: metrics.MetExpiryScannedTotal,
					Name:   metrics.MetExpiryScannedTotal,
					Help:   "entradas do índice de escritas examinadas pelo coletor; comparada com o total de rows, diz se o cursor dá a volta ou empaca",
					Type:   metrics.TypeCounter,
					Labels: rotulo,
					Value:  float64(st.ExpiredExamined),
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

// amostrasDeContadores publica as dez séries de cada contador mantido na
// escrita, da MESMA leitura (EstatisticasDeContadores) que o /v1/stats faz.
//
// Nascem com zero na subida, como todas as que podem alarmar: um dataset com
// contador e nenhuma linha emite as dez, zeradas — lacuna não se distingue de
// alvo fora do ar. O rótulo `counter` vem do YAML, conjunto fechado.
func amostrasDeContadores(dataset string, ests []counter.Stats) []metrics.Sample {
	out := make([]metrics.Sample, 0, len(ests)*10)
	for _, e := range ests {
		rotulo := []metrics.Label{
			{Name: metrics.LabelDataset, Value: dataset},
			{Name: metrics.LabelCounter, Value: e.Name},
		}
		out = append(out,
			metrics.Sample{
				Family: metrics.MetCounterGroups,
				Name:   metrics.MetCounterGroups,
				Help:   "grupos vivos do contador (inclui os com total zero; exclui os aposentados)",
				Type:   metrics.TypeGauge,
				Labels: rotulo,
				Value:  float64(e.Groups),
			},
			metrics.Sample{
				Family: metrics.MetCounterGroupsMax,
				Name:   metrics.MetCounterGroupsMax,
				Help:   "teto de grupos vivos (maxGroups) do contador",
				Type:   metrics.TypeGauge,
				Labels: rotulo,
				Value:  float64(e.Ceiling),
			},
			metrics.Sample{
				Family: metrics.MetCounterGroupsRefusedTotal,
				Name:   metrics.MetCounterGroupsRefusedTotal,
				Help:   "grupos novos recusados pelo teto nesta abertura; a linha entrou e NÃO é contada — diferente de zero é total errado para baixo",
				Type:   metrics.TypeCounter,
				Labels: rotulo,
				Value:  float64(e.Refused),
			},
			metrics.Sample{
				Family: metrics.MetCounterGroupsRetiredTotal,
				Name:   metrics.MetCounterGroupsRetiredTotal,
				Help:   "grupos aposentados por ficarem sem linha há mais que retainEmptyGroups, desde a criação do contador",
				Type:   metrics.TypeCounter,
				Labels: rotulo,
				Value:  float64(e.Retired),
			},
			metrics.Sample{
				Family: metrics.MetCounterOrphanEntries,
				Name:   metrics.MetCounterOrphanEntries,
				Help:   "entradas do vetor apontando para grupo aposentado ou inexistente, zeradas na dobra da abertura; não é esperado fora de uma janela de crash",
				Type:   metrics.TypeGauge,
				Labels: rotulo,
				Value:  float64(e.Orphans),
			},
			metrics.Sample{
				Family: metrics.MetCounterBytes,
				Name:   metrics.MetCounterBytes,
				Help:   "bytes em disco do contador: vetor de grupos mais dicionário",
				Type:   metrics.TypeGauge,
				Labels: rotulo,
				Value:  float64(e.Bytes),
			},
			metrics.Sample{
				Family: metrics.MetCounterRows,
				Name:   metrics.MetCounterRows,
				Help:   "linhas vivas que apontam para algum grupo deste contador; abaixo do rows do dataset são as linhas sem alguma coluna do groupBy, que o contador não conta por desenho",
				Type:   metrics.TypeGauge,
				Labels: rotulo,
				Value:  float64(e.Rows),
			},
			metrics.Sample{
				Family: metrics.MetCounterCountedRows,
				Name:   metrics.MetCounterCountedRows,
				Help:   "linhas que satisfazem a condição deste contador no dataset inteiro; sem `where`, igual ao rows do contador",
				Type:   metrics.TypeGauge,
				Labels: rotulo,
				Value:  float64(e.Total),
			},
			metrics.Sample{
				Family: metrics.MetCounterAuditDivergentGroups,
				Name:   metrics.MetCounterAuditDivergentGroups,
				Help:   "divergências na última auditoria conclusiva — grupos, linhas sem grupo internado e os dois agregados; 0 até a primeira (ver o timestamp ao lado)",
				Type:   metrics.TypeGauge,
				Labels: rotulo,
				Value:  float64(e.AuditDivergences),
			},
			metrics.Sample{
				Family: metrics.MetCounterAuditLastSuccessTimestamp,
				Name:   metrics.MetCounterAuditLastSuccessTimestamp,
				Help:   "Unix timestamp da última auditoria conclusiva do contador; 0 até a primeira",
				Type:   metrics.TypeGauge,
				Labels: rotulo,
				Value:  float64(e.AuditLastUnix),
			},
		)
	}
	return out
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
func amostrasDeIsolamento(dataset string, st *storage.Store) []metrics.Sample {
	rotulo := []metrics.Label{{Name: metrics.LabelDataset, Value: dataset}}
	return []metrics.Sample{
		{
			Family: metrics.MetQuerySnapshotBytes,
			Name:   metrics.MetQuerySnapshotBytes,
			Help:   "heap retido pelo journal de versões do isolamento de leitura",
			Type:   metrics.TypeGauge,
			Value:  float64(st.BytesRetidos()),
			Labels: rotulo,
		},
		{
			Family: metrics.MetQuerySnapshotOldestSeconds,
			Name:   metrics.MetQuerySnapshotOldestSeconds,
			Help:   "há quanto tempo a consulta mais antiga segura o passado; 0 quando não há nenhuma",
			Type:   metrics.TypeGauge,
			Value:  st.IdadeDoSnapshotMaisAntigo().Seconds(),
			Labels: rotulo,
		},
		{
			Family: metrics.MetQuerySnapshotExpiredTotal,
			Name:   metrics.MetQuerySnapshotExpiredTotal,
			Help:   "consultas invalidadas por o journal de versões estourar runtime.query.snapshotMaxBytes",
			Type:   metrics.TypeCounter,
			Value:  float64(st.SnapshotsExpirados()),
			Labels: rotulo,
		},
		{
			Family: metrics.MetCompactionPinnedSegments,
			Name:   metrics.MetCompactionPinnedSegments,
			Help:   "segmentos que a compaction não pode aposentar por guardarem versões de consultas em voo",
			Type:   metrics.TypeGauge,
			Value:  float64(st.SegmentosPinados()),
			Labels: rotulo,
		},
	}
}
