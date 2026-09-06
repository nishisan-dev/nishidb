package metakafka_test

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/kafka"
	"github.com/nishisan-dev/nishidb/internal/metakafka"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

const dataset = "servicos"

// consumidorFalso é o dublê de Consumido: devolve o Instantaneo que o teste
// montou, sem broker nenhum. Conta as chamadas, que é o observável de "uma ida
// por consulta".
type consumidorFalso struct {
	inst     kafka.Instantaneo
	chamadas int
}

func (c *consumidorFalso) Instantaneo(context.Context) kafka.Instantaneo {
	c.chamadas++
	return c.inst
}

// declarada é a identidade do fluxo que os testes usam.
func declarada() metakafka.Declarada {
	return metakafka.DoYAML(&schema.KafkaSpec{
		Enabled:          true,
		Topic:            "service-view",
		BootstrapServers: []string{"kafka02:9092", "kafka01:9092"},
		Consumer:         schema.ConsumerSpec{GroupID: "piloto-7"},
	})
}

// motorComKafka registra o dataset de mentira e a `_meta.kafka` sobre a fonte
// dada — a mesma costura do Registra, sem servidor.
func motorComKafka(t *testing.T, f metakafka.Fonte) *query.Engine {
	t.Helper()
	spec, err := schema.Parse([]byte("version: 1\nname: "+dataset+"\nmaterialization:\n  key:\n    field: id\nschema:\n  fields:\n    id:\n      type: string\n"), schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	sc, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	eng := query.NewEngine()
	if err := eng.Register(dataset, sc, fakestorage.New(fakestorage.Indexes{})); err != nil {
		t.Fatal(err)
	}
	if err := metakafka.Registra(eng, dataset, f); err != nil {
		t.Fatal(err)
	}
	return eng
}

// fonteCom monta a Fonte com o dublê e os closures que o teste quiser.
func fonteCom(c metakafka.Consumido) metakafka.Fonte {
	f := metakafka.Fonte{Declarada: declarada()}
	if c != nil {
		f.Consumidor = func() metakafka.Consumido { return c }
	}
	return f
}

type linha map[string]string

func consulta(t *testing.T, eng *query.Engine, sql string) []linha {
	t.Helper()
	it, err := eng.Execute(context.Background(), dataset, sql, nil)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	defer func() { _ = it.Close() }()
	var out []linha
	for it.Next() {
		l := make(linha, it.Row().Len())
		for _, c := range it.Row().Cols() {
			l[c.Name] = render(c)
		}
		out = append(out, l)
	}
	if err := it.Err(); err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return out
}

func render(c executor.ColView) string {
	if c.Null {
		return "<null>"
	}
	switch c.Val.Type {
	case types.TypeString:
		return string(c.Val.Bytes)
	case types.TypeInt64:
		return strconv.FormatInt(c.Val.I64, 10)
	case types.TypeBool:
		if c.Val.I64 != 0 {
			return "true"
		}
		return "false"
	case types.TypeTimestamp:
		return strconv.FormatInt(c.Val.I64, 10)
	default:
		return "?"
	}
}

func coluna(rows []linha, col string) string {
	out := make([]string, 0, len(rows))
	for _, l := range rows {
		out = append(out, l[col])
	}
	return strings.Join(out, ",")
}

// instantaneoCom monta um retrato com as partições dadas, todas com fim de log
// válido e o carimbo agora.
func instantaneoCom(parts ...kafka.PosicaoDaParticao) kafka.Instantaneo {
	return kafka.Instantaneo{
		Particoes:           parts,
		MarcaEm:             time.Unix(1_757_000_000, 0),
		FilaRegistros:       3,
		FilaBytes:           4096,
		UltimoLote:          time.Unix(1_757_000_001, 0),
		Desfecho:            kafka.Desfecho{Eventos: 100, Aplicados: 97, Stale: 2, Descartados: 1, Falhas: 0},
		ParticoesAtribuidas: int64(len(parts)),
		ParticoesDoTopico:   int64(len(parts)),
	}
}

func pos(p int32, aplicado, fim int64) kafka.PosicaoDaParticao {
	atraso := fim - aplicado
	if atraso < 0 {
		atraso = 0
	}
	return kafka.PosicaoDaParticao{Particao: p, Aplicado: aplicado, FimDoLog: fim, Atraso: atraso, TemFim: true}
}

// duraveisCom devolve o closure do tri-estado com os offsets dados.
func duraveisCom(offs map[int32]int64) func() (checkpoint.SourceOffsets, bool, error) {
	return func() (checkpoint.SourceOffsets, bool, error) {
		return checkpoint.SourceOffsets{Offsets: offs}, true, nil
	}
}

func inteiro(t *testing.T, s string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		t.Fatalf("valor não numérico: %q", s)
	}
	return n
}

func inteiroTexto(n int64) string { return strconv.FormatInt(n, 10) }

func juntar(cols []string) string { return strings.Join(cols, ", ") }

func contem(s, sub string) bool { return strings.Contains(s, sub) }

// snapshotDeEpoca monta um types.Snapshot com uma época que a fonte não abriu.
func snapshotDeEpoca(epoca uint64, limite types.RowID) types.Snapshot {
	return types.Snapshot{Epoch: epoca, LimitRowID: limite}
}
