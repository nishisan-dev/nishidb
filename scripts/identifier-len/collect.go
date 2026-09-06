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
	Topico  string
	Campo   string
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
	alvos, err := capturaAlvos(ctx, cfg.Brokers, cfg.Topico)
	if err != nil {
		return nil, err
	}

	partOffsets := make(map[int32]kgo.Offset, len(alvos))
	restante := make(map[int32]int64, len(alvos))
	var alvoTotal int64
	for p, a := range alvos {
		partOffsets[p] = kgo.NewOffset().At(a.inicio)
		restante[p] = a.fim - a.inicio
		alvoTotal += restante[p]
	}

	hist := NovoHistograma()
	if alvoTotal <= 0 {
		if prog != nil {
			prog(0, 0, 0)
		}
		return hist, nil
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{cfg.Topico: partOffsets}),
	)
	if err != nil {
		return nil, fmt.Errorf("identifier-len: cliente de consumo: %w", err)
	}
	defer cl.Close()

	inicioRodada := time.Now()
	var lidos, desdeOUltimoReport int64
	for !tudoConsumido(restante) {
		if ctx.Err() != nil {
			return hist, ctx.Err()
		}
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() {
			break
		}
		var erroFetch error
		fetches.EachError(func(topico string, particao int32, err error) {
			if erroFetch == nil {
				erroFetch = fmt.Errorf("identifier-len: fetch %s/%d: %w", topico, particao, err)
			}
		})
		if erroFetch != nil {
			return hist, erroFetch
		}

		fetches.EachRecord(func(r *kgo.Record) {
			if restante[r.Partition] <= 0 {
				// O watermark já foi alcançado nesta partição: o broker
				// pode entregar mensagens mais novas, produzidas depois da
				// captura em capturaAlvos, mas a rodada fecha no que foi
				// capturado — é essa borda que impede perseguir o tail.
				return
			}
			valor, resultado := ExtraiCampo(r.Value, cfg.Campo)
			hist.Registra(resultado, len(valor))
			restante[r.Partition]--
			lidos++
			desdeOUltimoReport++
			if prog != nil && cfg.ReportEvery > 0 && desdeOUltimoReport >= cfg.ReportEvery {
				prog(lidos, alvoTotal, time.Since(inicioRodada))
				desdeOUltimoReport = 0
			}
		})
	}
	if prog != nil {
		prog(lidos, alvoTotal, time.Since(inicioRodada))
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

// janela é o par [earliest, high-watermark) de uma partição no instante da
// captura.
type janela struct {
	inicio int64
	fim    int64
}

// capturaAlvos lê earliest e high-watermark de cada partição do tópico com
// um cliente administrativo que NUNCA consome — só metadados —, e fecha esse
// cliente antes de devolver. É a separação que torna "capturado na partida"
// verdade: o consumo real, aberto depois por Coleta, começa de um alvo já
// fixo, e nada entre a captura e o primeiro fetch pode movê-lo.
func capturaAlvos(ctx context.Context, brokers []string, topico string) (map[int32]janela, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("identifier-len: cliente administrativo: %w", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	inicio, err := adm.ListStartOffsets(ctx, topico)
	if err != nil {
		return nil, fmt.Errorf("identifier-len: offsets de início: %w", err)
	}
	if err := inicio.Error(); err != nil {
		return nil, fmt.Errorf("identifier-len: offsets de início: %w", err)
	}
	fim, err := adm.ListEndOffsets(ctx, topico)
	if err != nil {
		return nil, fmt.Errorf("identifier-len: high-watermark: %w", err)
	}
	if err := fim.Error(); err != nil {
		return nil, fmt.Errorf("identifier-len: high-watermark: %w", err)
	}

	porInicio, ok := inicio[topico]
	if !ok || len(porInicio) == 0 {
		return nil, fmt.Errorf("identifier-len: tópico %q sem partições", topico)
	}
	porFim := fim[topico]

	alvos := make(map[int32]janela, len(porInicio))
	for p, ini := range porInicio {
		f, ok := porFim[p]
		if !ok {
			return nil, fmt.Errorf("identifier-len: partição %d sem high-watermark", p)
		}
		alvos[p] = janela{inicio: ini.Offset, fim: f.Offset}
	}
	return alvos, nil
}
