package server

import (
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

// samplesOfIndex publica as seis séries do índice por segmento (§6.6) de UM
// dataset.
//
// Elas saem para TODO dataset, materializado e histórico, porque o índice por
// segmento é um motor só nos dois modos (decisão D1) — e é por isso que a
// família não pertence ao bloco `history`. A janela efetiva, que é política do
// histórico e não mecânica de índice, continua sendo publicada por
// nishidb_history_index_window_seconds, no rótulo do dataset BASE; ver o
// comentário da família em internal/metrics/names.go.
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
func samplesOfIndex(name string, e storage.EstatisticasOfIndex) []metrics.Sample {
	rotulo := []metrics.Label{{Name: metrics.LabelDataset, Value: name}}
	// A ordem é a de metrics.IndexStates, e a contagem é indexada por ela:
	// escrever os quatro pares à mão aqui seria uma segunda lista de estados ao
	// lado da do pacote de nomes, e a segunda lista é a que esquece o valor novo.
	byState := map[string]int64{
		metrics.StateSealed:  e.States.Sealed,
		metrics.StateCurrent: e.States.Current,
		metrics.StatePending: e.States.Pendente,
		metrics.StateOutside: e.States.Fora,
	}
	out := make([]metrics.Sample, 0, len(metrics.IndexStates)+len(metrics.IndexLookupModes)+4)
	for _, state := range metrics.IndexStates {
		out = append(out, metrics.Sample{
			Family: metrics.MetIndexSegments,
			Name:   metrics.MetIndexSegments,
			Help: "segmentos por estado de índice: selado (o .sidx cobre o conjunto indexado), corrente (o índice é o WIP), " +
				"pendente (dentro da janela e na fila de reconstrução) e fora (a janela não o cobre, e ele responde por enumeração de propósito)",
			Type:   metrics.TypeGauge,
			Labels: withLabel(rotulo, metrics.LabelState, state),
			Value:  float64(byState[state]),
		})
	}
	byModo := map[string]int64{
		metrics.ModeIndex:       e.ByIndex,
		metrics.ModeEnumeration: e.ByEnumeration,
	}
	for _, modo := range metrics.IndexLookupModes {
		out = append(out, metrics.Sample{
			Family: metrics.MetIndexSegmentsLookedUpTotal,
			Name:   metrics.MetIndexSegmentsLookedUpTotal,
			Help: "segmentos que os Lookups resolveram por cada caminho; é o único observável da degradação, " +
				"porque para o planner e o executor os dois são o mesmo iterador ascendente",
			Type:   metrics.TypeCounter,
			Labels: withLabel(rotulo, metrics.LabelMode, modo),
			Value:  float64(byModo[modo]),
		})
	}
	return append(out,
		metrics.Sample{
			Family: metrics.MetIndexBytes,
			Name:   metrics.MetIndexBytes,
			Help: "blocos ocupados pelo índice por segmento (.sidx, temporários e .widx do corrente) DESTE dataset. " +
				"Num histórico ele é a parcela por segmento de nishidb_history_index_bytes — que soma também indexes/ e " +
				"checkpoint/ e sai no rótulo do dataset BASE. As duas vivem em rótulos DIFERENTES: a comparação é entre " +
				"{dataset=\"X_history\"} aqui e {dataset=\"X\"} lá, nunca no mesmo rótulo",
			Type:   metrics.TypeGauge,
			Labels: rotulo,
			Value:  float64(e.Bytes),
		},
		metrics.Sample{
			Family: metrics.MetIndexRebuildsTotal,
			Name:   metrics.MetIndexRebuildsTotal,
			Help:   "segmentos cujo índice a reconstrução publicou desde a abertura do dataset; a taxa diz se a fila está drenando",
			Type:   metrics.TypeCounter,
			Labels: rotulo,
			Value:  float64(e.Reconstrucoes),
		},
		metrics.Sample{
			Family: metrics.MetIndexRebuildActive,
			Name:   metrics.MetIndexRebuildActive,
			Help: "1 enquanto o laço de reconstrução do índice por segmento está de pé; " +
				"0 sem campo indexado ou depois de ele parar, e ele para de vez na primeira falha",
			Type:   metrics.TypeGauge,
			Labels: rotulo,
			Value:  umSeVerdadeiro(e.RebuildActive),
		},
		metrics.Sample{
			Family: metrics.MetIndexCRCFailuresTotal,
			Name:   metrics.MetIndexCRCFailuresTotal,
			Help: "índices selados reprovados no crc32c desde a subida. É DEGRAU e não taxa: o mapeamento reprovado " +
				"fica em cache, então cada arquivo podre soma UMA vez por processo e um rate() sobre isto se auto-resolve " +
				"enquanto o segmento segue enumerando. Nenhuma série responde \"há corrupção AGORA\": o gauge de estados é " +
				"cego a ela (o arquivo reprovado conta como selado, porque a fila não confere crc) e a enumeração sobe " +
				"sem atribuir causa, no mesmo balde de um segmento fora da janela. O sinal é o WARN " +
				"indice.selado_corrompido no log. É o débito 103",
			Type:   metrics.TypeCounter,
			Labels: rotulo,
			Value:  float64(e.CRCFalhas),
		})
}

// comRotulo devolve o rótulo do dataset acrescido de UM par, sem tocar na fatia
// de origem.
//
// A cópia não é zelo: `append` sobre uma fatia compartilhada com capacidade
// sobrando faria a segunda série sobrescrever o rótulo da primeira, e as duas
// sairiam com o mesmo valor de `estado`. O bug seria invisível no código e óbvio
// no painel — quatro séries iguais.
func withLabel(base []metrics.Label, name, value string) []metrics.Label {
	out := make([]metrics.Label, len(base), len(base)+1)
	copy(out, base)
	return append(out, metrics.Label{Name: name, Value: value})
}

// indiceStats monta o bloco `index` do /v1/stats a partir da MESMA estrutura que
// o /metrics lê.
//
// Nenhum número é recalculado aqui, pela regra do handleStats: duas fontes para
// o mesmo fato divergem, e a divergência aparece como um painel que discorda do
// JSON sem nada estar quebrado.
func indexStats(e storage.EstatisticasOfIndex) protocol.IndexStats {
	return protocol.IndexStats{
		Sealed:          e.States.Sealed,
		Current:         e.States.Current,
		Pending:         e.States.Pendente,
		OutOfWindow:     e.States.Fora,
		Bytes:           e.Bytes,
		Rebuilds:        e.Reconstrucoes,
		RebuildRunning:  e.RebuildActive,
		CRCFailures:     e.CRCFalhas,
		SegmentsByIndex: e.ByIndex,
		SegmentsByScan:  e.ByEnumeration,
	}
}
