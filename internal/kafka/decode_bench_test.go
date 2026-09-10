package kafka

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nishisan-dev/nishidb/internal/schema"
)

// corpusDeRegime é um lote de eventos SEM descoberta: todas as colunas já
// existem quando a medição começa.
//
// É o corpus certo para medir vazão porque é o regime do piloto — ~1.700
// colunas, nenhuma nova — e porque a descoberta paga um parse dobrado que
// mediria a Fase 4.2, não o paralelismo. O documento tem doze chaves aninhadas,
// que é o que faz o decode custar o que ele custa.
func corpusDeRegime(n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		var sb strings.Builder
		fmt.Fprintf(&sb, `{"id":"e%d","sev":%d,"doc":{"probe":"10.0.%d.%d"`,
			i%5000, i%100, (i/250)%250, i%250)
		for k := 0; k < 12; k++ {
			fmt.Fprintf(&sb, `,"chave%d":"valor-%d-%d"`, k, i, k)
		}
		sb.WriteString(`}}`)
		out[i] = []byte(sb.String())
	}
	return out
}

// BenchmarkDecodeWorkers mede o pipeline inteiro — fila, decodificadores,
// esteira e writer — com 1, 2, 4 e 8 decodificadores.
//
// POR QUE O PIPELINE INTEIRO, e não só o decode. O ganho do paralelismo é
// limitado pela fração que continua serial (aplicar, indexar, escrever), e
// medir só a parte paralelizável reportaria um speedup que a ingestão nunca
// entrega. Este número é comparável com o teto de um core que o diagnóstico da
// Fase 4 mediu no piloto: 902 µs/evento, ~1.109 ev/s.
//
// O baseline vive em benchmark/baselines/, e é ele — não a intuição — que
// justifica subir `decodeWorkers` num dataset. O default é 1.
func BenchmarkDecodeWorkers(b *testing.B) {
	for _, workers := range []int{1, 2, 4, 8} {
		b.Run("workers="+strconv.Itoa(workers), func(b *testing.B) {
			mat := matDoGolden(b)
			cfg := &Config{
				Dataset: "eventos", Topic: "eventos", GroupID: "g",
				Brokers:        []string{"127.0.0.1:9092"},
				MaxPollRecords: 1000, QueueSize: 5000, QueueBytes: 64 << 20,
				DecodeWorkers:      workers,
				InvalidEventPolicy: schema.InvalidEventLogAndSkip,
			}
			fila := NewQueue(cfg.QueueSize, cfg.QueueBytes)
			met := registerMetrics(nil, "eventos")
			w := NewWriter(cfg, mat, storeGolden{}, fila, nil, met,
				map[int32]int64{0: 0}, Cadencia{Eventos: 1 << 30, Intervalo: time.Hour}, nil)

			ctx, cancela := context.WithCancel(context.Background())
			defer cancela()
			pool := newDecodePool(workers, fila, w.est, mat, met, w.limiarDeEncolhimento)
			esperaDecode := pool.Sobe(ctx)
			pronto := make(chan error, 1)
			go func() { pronto <- w.Roda(ctx) }()

			// Aquece fora da medição: a primeira passagem cria as colunas, e
			// medir a descoberta junto mediria a Fase 4.2 no lugar desta.
			corpus := corpusDeRegime(4096)
			for i := 0; i < 256; i++ {
				if err := fila.Envia(ctx, &kgo.Record{Value: corpus[i]}); err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				rec := &kgo.Record{Partition: 0, Offset: int64(i), Value: corpus[i%len(corpus)]}
				if err := fila.Envia(ctx, rec); err != nil {
					b.Fatal(err)
				}
			}
			fila.Fecha()
			esperaDecode()
			if err := <-pronto; err != nil {
				b.Fatal(err)
			}
			b.StopTimer()

			// ev/s é o número do aceite da fase, e ele sai daqui em vez de uma
			// conta na cabeça de quem lê ns/op.
			if b.Elapsed() > 0 {
				b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ev/s")
			}
			b.ReportMetric(float64(w.est.bytesRetidos())/1024, "KiB_esteira")
		})
	}
}
