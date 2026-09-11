package main

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ConfigColeta reúne os parâmetros de uma rodada de medição.
type ConfigColeta struct {
	Brokers []string
	Topic   string
	Field   string
	// ReportEvery é o intervalo, em mensagens, entre chamadas a Progresso.
	// Zero desliga o progresso (só o Progresso final ainda é chamado).
	ReportEvery int64
}

// Progresso é chamado a cada ConfigColeta.ReportEvery mensagens lidas, e uma
// última vez ao final da rodada.
type Progresso func(lidos, alvo int64, decorrido time.Duration)

// Coleta consome o tópico do earliest até o high-watermark capturado NA
// PARTIDA — sem consumer group, sem commit de offset — e devolve o
// histograma de comprimentos do campo configurado.
//
// "Capturado na partida" é a decisão que faz a medição TERMINAR: o tópico do
// piloto recebe tráfego contínuo (~452 msg/s, docs/piloto-events.md) e não
// vai parar sozinho, então perseguir o tail faria esta ferramenta nunca
// sair. O watermark é lido uma única vez, antes do primeiro fetch, com um
// cliente à parte que nunca chega a consumir; a rodada fecha nele mesmo que
// o tópico continue recebendo durante a leitura.
//
// Não há consumer group em nenhum momento: kgo.ConsumePartitions atribui
// partições diretamente, sem join nem rebalance, e sem group não há posição
// nenhuma para commitar — a leitura é estritamente somativa, sem side
// effect no cluster.
func Coleta(ctx context.Context, cfg ConfigColeta, prog Progresso) (*Histograma, error) {
	targets, err := capturaAlvos(ctx, cfg.Brokers, cfg.Topic)
	if err != nil {
		return nil, err
	}

	partOffsets := make(map[int32]kgo.Offset, len(targets))
	restante := make(map[int32]int64, len(targets))
	var targetTotal int64
	for p, a := range targets {
		partOffsets[p] = kgo.NewOffset().At(a.start)
		restante[p] = a.end - a.start
		targetTotal += restante[p]
	}

	hist := NewHistograma()
	if targetTotal <= 0 {
		if prog != nil {
			prog(0, 0, 0)
		}
		return hist, nil
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{cfg.Topic: partOffsets}),
	)
	if err != nil {
		return nil, fmt.Errorf("identifier-len: cliente de consumo: %w", err)
	}
	defer cl.Close()

	startRodada := time.Now()
	var lidos, desdeOUltimoReport int64
	for !tudoConsumido(restante) {
		if ctx.Err() != nil {
			return hist, ctx.Err()
		}
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() {
			break
		}
		var errFetch error
		fetches.EachError(func(topic string, partition int32, err error) {
			if errFetch == nil {
				errFetch = fmt.Errorf("identifier-len: fetch %s/%d: %w", topic, partition, err)
			}
		})
		if errFetch != nil {
			return hist, errFetch
		}

		fetches.EachRecord(func(r *kgo.Record) {
			if restante[r.Partition] <= 0 {
				// O watermark já foi alcançado nesta partição: o broker
				// pode entregar mensagens mais novas, produzidas depois da
				// captura em capturaAlvos, mas a rodada fecha no que foi
				// capturado — é essa borda que impede perseguir o tail.
				return
			}
			value, result := ExtractField(r.Value, cfg.Field)
			hist.Register(result, len(value))
			restante[r.Partition]--
			lidos++
			desdeOUltimoReport++
			if prog != nil && cfg.ReportEvery > 0 && desdeOUltimoReport >= cfg.ReportEvery {
				prog(lidos, targetTotal, time.Since(startRodada))
				desdeOUltimoReport = 0
			}
		})
	}
	if prog != nil {
		prog(lidos, targetTotal, time.Since(startRodada))
	}
	return hist, nil
}

// tudoConsumido reporta se todas as partições já alcançaram o watermark
// capturado na partida (nenhuma tem registros restantes). O nome antigo,
// faltaConsumir, devolvia exatamente este valor — true quando NADA falta —,
// o inverso do que o nome prometia.
func tudoConsumido(restante map[int32]int64) bool {
	for _, r := range restante {
		if r > 0 {
			return false
		}
	}
	return true
}

// window é o par [earliest, high-watermark) de uma partição no instante da
// captura.
type window struct {
	start int64
	end   int64
}

// capturaAlvos lê earliest e high-watermark de cada partição do tópico com
// um cliente administrativo que NUNCA consome — só metadados —, e fecha esse
// cliente antes de devolver. É a separação que torna "capturado na partida"
// verdade: o consumo real, aberto depois por Coleta, começa de um alvo já
// fixo, e nada entre a captura e o primeiro fetch pode movê-lo.
func capturaAlvos(ctx context.Context, brokers []string, topic string) (map[int32]window, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("identifier-len: cliente administrativo: %w", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	start, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("identifier-len: offsets de início: %w", err)
	}
	if err := start.Error(); err != nil {
		return nil, fmt.Errorf("identifier-len: offsets de início: %w", err)
	}
	end, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("identifier-len: high-watermark: %w", err)
	}
	if err := end.Error(); err != nil {
		return nil, fmt.Errorf("identifier-len: high-watermark: %w", err)
	}

	byStart, ok := start[topic]
	if !ok || len(byStart) == 0 {
		return nil, fmt.Errorf("identifier-len: tópico %q sem partições", topic)
	}
	byEnd := end[topic]

	targets := make(map[int32]window, len(byStart))
	for p, ini := range byStart {
		f, ok := byEnd[p]
		if !ok {
			return nil, fmt.Errorf("identifier-len: partição %d sem high-watermark", p)
		}
		targets[p] = window{start: ini.Offset, end: f.Offset}
	}
	return targets, nil
}
