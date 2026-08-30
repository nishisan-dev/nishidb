package server

import (
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocolo"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

// amostrasDeIndice publica as seis séries do índice por segmento (§6.6) de UM
// dataset.
//
// Elas saem para TODO dataset, materializado e histórico, porque o índice por
// segmento é um motor só nos dois modos (decisão D1) — e é por isso que a
// família não pertence ao bloco `history`. A janela efetiva, que é política do
// histórico e não mecânica de índice, continua sendo publicada por
// nishidb_history_index_window_seconds, no rótulo do dataset BASE; ver o
// comentário da família em internal/metrics/nomes.go.
//
// TODAS AS COMBINAÇÕES DE RÓTULO SAEM SEMPRE, inclusive as zeradas, e isso é a
// regra que a função existe para cumprir. Uma série que só aparece quando o
// primeiro segmento é enumerado faz `absent()` e `rate()` mentirem exatamente na
// janela em que o operador mais precisa deles — logo depois de uma subida. Zero é
// uma resposta; ausência não é, e é indistinguível de alvo fora do ar.
//
// O ÚNICO valor que NÃO nasce em zero é o de reconstrucao_ativa, e ele está
// certo: o §92 manda o medidor subir antes da goroutine, então num dataset com
// campo indexado o valor verdadeiro na subida é 1. Um medidor que reportasse zero
// para caber numa regra sobre contadores estaria mentindo sobre o estado.
func amostrasDeIndice(nome string, e storage.EstatisticasDeIndice) []metrics.Amostra {
	rotulo := []metrics.Label{{Nome: metrics.LabelDataset, Valor: nome}}
	// A ordem é a de metrics.EstadosDeIndice, e a contagem é indexada por ela:
	// escrever os quatro pares à mão aqui seria uma segunda lista de estados ao
	// lado da do pacote de nomes, e a segunda lista é a que esquece o valor novo.
	porEstado := map[string]int64{
		metrics.EstadoSelado:   e.Estados.Selado,
		metrics.EstadoCorrente: e.Estados.Corrente,
		metrics.EstadoPendente: e.Estados.Pendente,
		metrics.EstadoFora:     e.Estados.Fora,
	}
	out := make([]metrics.Amostra, 0, len(metrics.EstadosDeIndice)+len(metrics.ModosDeConsultaDeIndice)+4)
	for _, estado := range metrics.EstadosDeIndice {
		out = append(out, metrics.Amostra{
			Familia: metrics.MetIndexSegmentos,
			Nome:    metrics.MetIndexSegmentos,
			Ajuda: "segmentos por estado de índice: selado (o .sidx cobre o conjunto indexado), corrente (o índice é o WIP), " +
				"pendente (dentro da janela e na fila de reconstrução) e fora (a janela não o cobre, e ele responde por enumeração de propósito)",
			Tipo:   metrics.TipoMedidor,
			Labels: comRotulo(rotulo, metrics.LabelEstado, estado),
			Valor:  float64(porEstado[estado]),
		})
	}
	porModo := map[string]int64{
		metrics.ModoIndice:     e.PorIndice,
		metrics.ModoEnumeracao: e.PorEnumeracao,
	}
	for _, modo := range metrics.ModosDeConsultaDeIndice {
		out = append(out, metrics.Amostra{
			Familia: metrics.MetIndexSegmentosConsultadosTotal,
			Nome:    metrics.MetIndexSegmentosConsultadosTotal,
			Ajuda: "segmentos que os Lookups resolveram por cada caminho; é o único observável da degradação, " +
				"porque para o planner e o executor os dois são o mesmo iterador ascendente",
			Tipo:   metrics.TipoContador,
			Labels: comRotulo(rotulo, metrics.LabelModo, modo),
			Valor:  float64(porModo[modo]),
		})
	}
	return append(out,
		metrics.Amostra{
			Familia: metrics.MetIndexBytes,
			Nome:    metrics.MetIndexBytes,
			Ajuda: "blocos ocupados pelo índice por segmento (.sidx, temporários e .widx do corrente) DESTE dataset. " +
				"Num histórico ele é a parcela por segmento de nishidb_history_index_bytes — que soma também indexes/ e " +
				"checkpoint/ e sai no rótulo do dataset BASE. As duas vivem em rótulos DIFERENTES: a comparação é entre " +
				"{dataset=\"X_history\"} aqui e {dataset=\"X\"} lá, nunca no mesmo rótulo",
			Tipo:   metrics.TipoMedidor,
			Labels: rotulo,
			Valor:  float64(e.Bytes),
		},
		metrics.Amostra{
			Familia: metrics.MetIndexReconstrucoesTotal,
			Nome:    metrics.MetIndexReconstrucoesTotal,
			Ajuda:   "segmentos cujo índice a reconstrução publicou desde a abertura do dataset; a taxa diz se a fila está drenando",
			Tipo:    metrics.TipoContador,
			Labels:  rotulo,
			Valor:   float64(e.Reconstrucoes),
		},
		metrics.Amostra{
			Familia: metrics.MetIndexReconstrucaoAtiva,
			Nome:    metrics.MetIndexReconstrucaoAtiva,
			Ajuda: "1 enquanto o laço de reconstrução do índice por segmento está de pé; " +
				"0 sem campo indexado ou depois de ele parar, e ele para de vez na primeira falha",
			Tipo:   metrics.TipoMedidor,
			Labels: rotulo,
			Valor:  umSeVerdadeiro(e.ReconstrucaoAtiva),
		},
		metrics.Amostra{
			Familia: metrics.MetIndexCrcFalhasTotal,
			Nome:    metrics.MetIndexCrcFalhasTotal,
			Ajuda: "índices selados reprovados no crc32c. É DEGRAU, não taxa: o mapeamento reprovado fica em cache, " +
				"então cada arquivo podre soma UMA vez por processo e um rate() sobre isto se auto-resolve enquanto o " +
				"segmento segue enumerando. Para saber se há índice podre AGORA, olhe " +
				"nishidb_index_segmentos_consultados_total{modo=\"enumeracao\"} subindo de forma persistente num dataset " +
				"que deveria estar indexado",
			Tipo:   metrics.TipoContador,
			Labels: rotulo,
			Valor:  float64(e.CRCFalhas),
		})
}

// comRotulo devolve o rótulo do dataset acrescido de UM par, sem tocar na fatia
// de origem.
//
// A cópia não é zelo: `append` sobre uma fatia compartilhada com capacidade
// sobrando faria a segunda série sobrescrever o rótulo da primeira, e as duas
// sairiam com o mesmo valor de `estado`. O bug seria invisível no código e óbvio
// no painel — quatro séries iguais.
func comRotulo(base []metrics.Label, nome, valor string) []metrics.Label {
	out := make([]metrics.Label, len(base), len(base)+1)
	copy(out, base)
	return append(out, metrics.Label{Nome: nome, Valor: valor})
}

// indiceStats monta o bloco `index` do /v1/stats a partir da MESMA estrutura que
// o /metrics lê.
//
// Nenhum número é recalculado aqui, pela regra do handleStats: duas fontes para
// o mesmo fato divergem, e a divergência aparece como um painel que discorda do
// JSON sem nada estar quebrado.
func indiceStats(e storage.EstatisticasDeIndice) protocolo.IndiceStats {
	return protocolo.IndiceStats{
		Sealed:          e.Estados.Selado,
		Current:         e.Estados.Corrente,
		Pending:         e.Estados.Pendente,
		OutOfWindow:     e.Estados.Fora,
		Bytes:           e.Bytes,
		Rebuilds:        e.Reconstrucoes,
		RebuildRunning:  e.ReconstrucaoAtiva,
		CRCFailures:     e.CRCFalhas,
		SegmentsByIndex: e.PorIndice,
		SegmentsByScan:  e.PorEnumeracao,
	}
}
