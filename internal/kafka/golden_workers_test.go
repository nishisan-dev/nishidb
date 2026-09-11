package kafka

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// goldenYAML é um dataset de descoberta pura com documento aninhado — a forma
// do piloto —, mais uma coluna declarada e uma congelada.
//
// Descoberta é o que importa aqui: é ela que cria coluna em runtime, e portanto
// a única coisa no pipeline capaz de atribuir FieldID diferente conforme quem
// decodificou primeiro.
const goldenYAML = `version: 1
name: eventos
materialization:
  key:
    field: id
  ttl:
    - expireAfterWrite: 24h
schema:
  include: ["*"]
  unknownFields: include
  fields:
    id:
      type: string
    doc:
      type: json
      unknownFields: include
      extract:
        - path: probe
          as: probe_ip
          type: string
    sev:
      type: int64
    sevOriginal:
      frozenFrom: sev
`

// storeGolden é o dublê de checkpoint. O golden não mede durabilidade: o que
// ele compara é o estado materializado, e o checkpoint só precisa não atrapalhar.
type storeGolden struct{}

func (storeGolden) Checkpoint() error                               { return nil }
func (storeGolden) SetSourceOffsets(checkpoint.SourceOffsets) error { return nil }
func (storeGolden) SourceOffsets() (checkpoint.SourceOffsets, bool, error) {
	return checkpoint.SourceOffsets{}, false, nil
}
func (storeGolden) RowCount() uint64 { return 0 }

// corpusGolden é determinístico e cobre as rotas que poderiam divergir entre
// um decodificador e oito.
//
// Cada família existe por um motivo:
//   - chave aninhada NOVA em posições variadas: é a que cria coluna, e o FieldID
//     que ela recebe é o contrato do driver JDBC;
//   - a MESMA chave nova em eventos vizinhos: com 8 workers os dois decodes
//     abortam juntos e o segundo reprocesso não cria nada, que é o caminho em
//     que uma contagem mal adiada dobraria;
//   - tipo divergente e coerção: alimentam os contadores por coluna que o
//     `_meta.fields` publica, e é neles que uma contagem em dobro apareceria;
//   - documento que vira array: enumera todas as colunas do pai, o caminho mais
//     caro e o mais fácil de fazer alocar;
//   - evento inválido: o balde do §101 tem de bater nos dois lados.
func corpusGolden() []string {
	var ev []string
	for i := 0; i < 120; i++ {
		id := "e" + strconv.Itoa(i%17)
		switch i % 8 {
		case 0:
			ev = append(ev, fmt.Sprintf(`{"id":%q,"sev":%d,"doc":{"probe":"10.0.0.%d","k%d":%d}}`, id, i, i%250, i, i))
		case 1:
			// A mesma chave nova que o vizinho: dois decodes abortam juntos.
			ev = append(ev, fmt.Sprintf(`{"id":%q,"sev":%d,"doc":{"probe":"10.0.1.%d","k%d":%d}}`, id, i, i%250, i-1, i))
		case 2:
			ev = append(ev, fmt.Sprintf(`{"id":%q,"sev":%d,"doc":{"comum":"texto-%d"}}`, id, i, i))
		case 3:
			// Coerção: `comum` foi fixada como string e recebe número.
			ev = append(ev, fmt.Sprintf(`{"id":%q,"sev":%d,"doc":{"comum":%d}}`, id, i, i))
		case 4:
			// Divergência: `numerico` fixa int64 e depois recebe objeto.
			ev = append(ev, fmt.Sprintf(`{"id":%q,"sev":%d,"doc":{"numerico":%d}}`, id, i, i))
		case 5:
			ev = append(ev, fmt.Sprintf(`{"id":%q,"sev":%d,"doc":{"numerico":{"nao":"cabe"}}}`, id, i))
		case 6:
			// Pai com forma divergente: remove todas as colunas do documento.
			ev = append(ev, fmt.Sprintf(`{"id":%q,"sev":%d,"doc":[1,2,3]}`, id, i))
		default:
			ev = append(ev, fmt.Sprintf(`{"id":%q,"sev":%d,"topo%d":"v"}`, id, i, i%5))
		}
	}
	// Evento ruim no meio, para o balde do §101 entrar na comparação.
	ev = append(ev, `{"id":"e1", isso não é json`)
	return ev
}

// runPipeline monta o pipeline REAL — fila, pool de decodificadores, esteira e
// writer — e devolve o retrato do dataset que sobrou.
//
// Usa as peças de produção, e não uma imitação: um harness que chamasse
// mat.Apply em laço passaria com a esteira quebrada.
func runPipeline(t *testing.T, workers int, eventos []string) retratoOfDataset {
	t.Helper()
	spec, err := schema.Parse([]byte(goldenYAML), schema.BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	sch, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	st := fakestorage.New(fakestorage.Indexes{})
	mat, err := materializer.New(sch, st)
	if err != nil {
		t.Fatal(err)
	}
	// Relógio fixo: o `_writtenAt` entra no SELECT * pelo nome, e um instante
	// real faria as duas rodadas divergirem por milissegundo em vez de por
	// defeito.
	mat.UsaRelogio(func() time.Time { return time.UnixMilli(1_700_000_000_000) })

	eng := query.NewEngine()
	if err := eng.Register("eventos", sch, st); err != nil {
		t.Fatal(err)
	}

	cfg := &Config{
		Dataset: "eventos", Topic: "eventos", GroupID: "g",
		Brokers: []string{"127.0.0.1:9092"}, MaxPollRecords: 32,
		QueueSize: 64, QueueBytes: 1 << 20, DecodeWorkers: workers,
		InvalidEventPolicy: schema.InvalidEventLogAndSkip,
	}
	fila := NewQueue(cfg.QueueSize, cfg.QueueBytes)
	met := registerMetrics(nil, "eventos")
	w := NewWriter(cfg, mat, storeGolden{}, fila, nil, met,
		map[int32]int64{0: 0}, Cadencia{Eventos: 1 << 30, Intervalo: time.Hour}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := newDecodePool(workers, fila, w.est, mat, met, w.limiarOfEncolhimento)
	waitDecode := pool.Rises(ctx)
	writerPronto := make(chan error, 1)
	go func() { writerPronto <- w.Run(ctx) }()

	for i, ev := range eventos {
		r := &kgo.Record{Partition: 0, Offset: int64(i), Value: []byte(ev)}
		if err := fila.Envia(ctx, r); err != nil {
			t.Fatalf("Envia(%d): %v", i, err)
		}
	}
	fila.Close()
	waitDecode()
	if err := <-writerPronto; err != nil {
		t.Fatalf("writer: %v", err)
	}

	return retratoOfDataset{
		rows:     consulta(t, eng, "SELECT * FROM eventos ORDER BY id"),
		fields:   consulta(t, eng, "SELECT name, type, auto, occurrences, typeMismatches, coercions FROM _meta.fields"),
		desfecho: w.Desfecho(),
		serial:   mat.Metrics().DecodeSerialTotal,
	}
}

// retratoOfDataset é o que se compara entre as duas rodadas.
type retratoOfDataset struct {
	rows     string
	fields   string
	desfecho Desfecho
	serial   int64
}

// consulta serializa o resultado com NOME, POSIÇÃO e valor de cada coluna.
//
// A posição entra de propósito: o contrato do driver JDBC é posicional, e uma
// divergência de FieldID reordena as colunas sem mudar nenhum valor. Comparar
// só os valores deixaria passar exatamente o defeito que este teste existe para
// pegar.
func consulta(t *testing.T, eng *query.Engine, sql string) string {
	t.Helper()
	it, err := eng.Execute(context.Background(), "eventos", sql, nil)
	if err != nil {
		t.Fatalf("Execute(%q): %v", sql, err)
	}
	defer func() {
		if err := it.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	var sb strings.Builder
	for it.Next() {
		for i, col := range it.Row().Cols() {
			fmt.Fprintf(&sb, "%d:%s=", i, col.Name)
			if col.Null {
				sb.WriteString("<null>")
			} else {
				sb.WriteString(textOfValue(col.Val))
			}
			sb.WriteByte('\t')
		}
		sb.WriteByte('\n')
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iteração de %q: %v", sql, err)
	}
	return sb.String()
}

func textOfValue(v types.Value) string {
	switch v.Type {
	case types.TypeString, types.TypeJSON:
		return string(v.Bytes)
	case types.TypeBool:
		return strconv.FormatBool(v.I64 != 0)
	default:
		return strconv.FormatInt(v.I64, 10)
	}
}

// O GOLDEN DA FASE 4: oito decodificadores produzem o MESMO dataset que um.
//
// São duas comparações e as duas são necessárias. O `SELECT *` pega divergência
// de VALOR e de POSIÇÃO — a linha errada, o campo no lugar do outro. O
// `_meta.fields` pega divergência de FieldID e de CONTADOR, que o `SELECT *` não
// enxerga: dois datasets com as mesmas colunas em ordens diferentes só divergem
// no `SELECT *` se alguma linha tiver valores distintos naquelas colunas, e um
// contador em dobro não aparece em linha nenhuma.
//
// O que poderia divergir, e por isso é comparado:
//   - a ordem de aplicação, se a esteira reordenar errado;
//   - o FieldID, se um decodificador criar coluna (o que a Fase 4.2 proíbe);
//   - o tipo fixado, se ele sair de quem decodificou primeiro em vez de quem
//     tem o menor offset;
//   - as ocorrências e coerções por coluna, se o evento reprocessado contar
//     duas vezes.
func TestGoldenUmWorkerContraOitoProduzOMesmoDataset(t *testing.T) {
	corpus := corpusGolden()
	um := runPipeline(t, 1, corpus)
	oito := runPipeline(t, 8, corpus)

	if um.rows != oito.rows {
		t.Errorf("SELECT * divergiu entre 1 e 8 decodificadores:\n%s",
			primeiraDiferenca(um.rows, oito.rows))
	}
	if um.fields != oito.fields {
		t.Errorf("_meta.fields divergiu entre 1 e 8 decodificadores:\n%s",
			primeiraDiferenca(um.fields, oito.fields))
	}
	if um.desfecho != oito.desfecho {
		t.Errorf("desfecho divergiu: 1 worker = %+v, 8 workers = %+v", um.desfecho, oito.desfecho)
	}
	// Sanidade do próprio teste: um golden sobre dataset vazio compararia duas
	// strings vazias e passaria por vacuidade.
	if len(um.rows) == 0 || len(um.fields) == 0 {
		t.Fatal("o golden comparou saída VAZIA; o corpus não materializou nada")
	}
	if um.desfecho.Aplicados == 0 {
		t.Fatalf("nenhum evento aplicado: %+v", um.desfecho)
	}
	if um.serial == 0 {
		t.Fatal("nenhum reprocesso: o corpus não exercitou a descoberta, que é o que pode divergir")
	}
}

// primeiraDiferenca localiza a linha em que dois retratos divergem, porque
// despejar dois blocos de milhares de linhas não ajuda ninguém a achar o
// defeito.
func primeiraDiferenca(a, b string) string {
	la, lb := strings.Split(a, "\n"), strings.Split(b, "\n")
	for i := 0; i < len(la) && i < len(lb); i++ {
		if la[i] != lb[i] {
			return fmt.Sprintf("linha %d:\n  1 worker : %s\n  8 workers: %s", i+1, la[i], lb[i])
		}
	}
	return fmt.Sprintf("mesmo prefixo, contagens diferentes: %d contra %d linhas", len(la), len(lb))
}
