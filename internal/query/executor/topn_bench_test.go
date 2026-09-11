package executor_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/query/planner"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// A bancada do top-N mede o OPERADOR, não o storage.
//
// Por isso a fonte é um dublê sem alocação nenhuma — nem por row, nem por
// query. O fakestorage não serve aqui: o ScanLive dele monta e ordena um slice
// com todos os rowIds a cada abertura, e sobre 100 mil linhas esse custo é da
// mesma ordem do sort que se quer medir. Ele apareceria dos DOIS lados, e é
// justamente isso que estragaria a comparação — ruído comum não se cancela numa
// razão, ele a achata em direção a 1 e faz o ganho parecer menor do que é.
type sourceOfBench struct {
	ids      []types.RowID
	payloads [][]byte
	it       iteradorOfBench
}

func (f *sourceOfBench) GetByRowID(_ types.Snapshot, id types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	buf = append(buf[:0], f.payloads[id]...)
	return types.Record{RowID: id, Payload: buf}, buf, true, nil
}

func (f *sourceOfBench) GetByKey(_ types.Snapshot, _, buf []byte) (types.Record, []byte, bool, error) {
	return types.Record{}, buf, false, nil
}

// ScanLive REUSA o iterador: a bancada roda serialmente, e devolver um novo a
// cada query poria uma alocação por query na medida do lado que abre mais
// queries — que são os dois, igualmente, mas o ponto é medir o operador.
func (f *sourceOfBench) OpenSnapshot() types.Snapshot { return types.Snapshot{} }

func (f *sourceOfBench) CloseSnapshot(types.Snapshot) {}

func (f *sourceOfBench) ScanLive(types.Snapshot) types.RowIDIterator {
	f.it.f, f.it.pos = f, 0
	return &f.it
}

func (f *sourceOfBench) Lookup(types.FieldID, types.Value) (types.RowIDIterator, error) {
	return nil, fmt.Errorf("%w: bancada sem bitmap", types.ErrInvalidArgument)
}

func (f *sourceOfBench) Indexed(types.FieldID) bool { return false }

type iteradorOfBench struct {
	f   *sourceOfBench
	pos int
}

func (i *iteradorOfBench) Next() bool {
	if i.pos >= len(i.f.ids) {
		return false
	}
	i.pos++
	return true
}

func (i *iteradorOfBench) RowID() types.RowID { return i.f.ids[i.pos-1] }
func (i *iteradorOfBench) Err() error         { return nil }
func (i *iteradorOfBench) Close() error       { return nil }

// buildSource serializa o corpus uma vez, fora do cronômetro.
//
// O schema vem POR ARGUMENTO, e não compilado aqui: os payloads precisam usar
// exatamente os FieldIDs que a query ligada endereça. Um segundo Compile daria
// um segundo dicionário, e bastaria ele numerar diferente para a bancada medir
// uma consulta sobre colunas ausentes — rápida, silenciosa e sem significado.
func buildSource(tb testing.TB, c *schema.Compiled, rows []rowTopN) *sourceOfBench {
	tb.Helper()
	id, name, num := field(tb, c, "id"), field(tb, c, "nome"), field(tb, c, "n")
	// Os três campos vão na ordem de FieldID, que neste schema é id, n, nome.
	ordem := []struct {
		fid types.FieldID
		val func(l rowTopN) (types.Value, bool)
	}{
		{id.ID, func(l rowTopN) (types.Value, bool) {
			return types.Value{Type: types.TypeString, Bytes: []byte(l.id)}, true
		}},
		{num.ID, func(l rowTopN) (types.Value, bool) {
			return types.Value{Type: types.TypeInt64, I64: l.n}, true
		}},
		{name.ID, func(l rowTopN) (types.Value, bool) {
			return types.Value{Type: types.TypeString, Bytes: []byte(l.name)}, l.hasName
		}},
	}
	for i := 1; i < len(ordem); i++ {
		if ordem[i].fid < ordem[i-1].fid {
			tb.Fatalf("a ordem de FieldID do schema mudou: %d antes de %d", ordem[i-1].fid, ordem[i].fid)
		}
	}

	f := &sourceOfBench{
		ids:      make([]types.RowID, len(rows)),
		payloads: make([][]byte, len(rows)),
	}
	for i, l := range rows {
		var p []byte
		var err error
		for _, o := range ordem {
			v, presente := o.val(l)
			if !presente {
				continue
			}
			if p, err = encoding.AppendField(p, o.fid, v); err != nil {
				tb.Fatal(err)
			}
		}
		f.ids[i], f.payloads[i] = types.RowID(i), p
	}
	return f
}

// corpusOfBench é o corpus de 100 mil linhas, em três FORMAS.
//
// A forma importa porque ela decide quantas linhas o heap ACEITA, que é o único
// trabalho que o top-N faz além de comparar:
//
//   - "aleatorio" é o caso realista: uma permutação pseudo-aleatória, em que o
//     número de aceitações cresce com o logaritmo do varrido.
//   - "pior" é `nome` decrescente na ordem de varredura: TODA linha nova é
//     melhor que a raiz, então toda linha é aceita, reescreve um buffer e paga
//     um sift. É o teto de custo do modo.
//   - "melhor" é `nome` crescente: depois das N primeiras nenhuma entra, e o
//     modo vira uma comparação por linha.
//
// Medir só o "aleatorio" daria um ganho bonito sem dizer o que acontece no dia
// ruim; medir só o "pior" faria o modo parecer caro num caso que quase não
// ocorre. O teto de N sai dos três.
func corpusOfBench(forma string, rows int) []rowTopN {
	out := make([]rowTopN, 0, rows)
	// LCG determinístico: o corpus tem de ser o mesmo entre rodadas, senão a
	// comparação com -count=6 mistura variação de máquina com variação de dado.
	semente := uint64(0x9E3779B97F4A7C15)
	for i := 0; i < rows; i++ {
		var key int
		switch forma {
		case "pior":
			key = rows - i
		case "melhor":
			key = i
		default:
			semente = semente*6364136223846793005 + 1442695040888963407
			key = int(semente >> 40)
		}
		out = append(out, rowTopN{
			id:      fmt.Sprintf("k%07d", i),
			name:    fmt.Sprintf("g%08d", key),
			hasName: i%13 != 0,
			n:       int64(key % 1024),
		})
	}
	return out
}

// confereModo roda a query UMA vez, fora do cronômetro, e falha se o modo
// medido não for o pedido.
//
// Bancada que mede o caminho errado não dá erro: dá um número, e o número entra
// num documento. Esta função é o que impede que a comparação vire "o caminho
// completo contra ele mesmo" sem ninguém perceber.
func confereModo(b *testing.B, bq *binder.BoundQuery, opts executor.Options, querTopN bool) {
	b.Helper()
	var m executor.Metrics
	opts.Metrics = &m
	it, err := executor.Open(context.Background(), bq, planner.FullScan(), opts)
	if err != nil {
		b.Fatal(err)
	}
	for it.Next() {
	}
	if err := it.Err(); err != nil {
		b.Fatal(err)
	}
	if err := it.Close(); err != nil {
		b.Fatal(err)
	}
	if usou := m.TopN.Load() == 1; usou != querTopN {
		b.Fatalf("modo medido: topN=%v, quer %v (TopNMax=%d)", usou, querTopN, opts.TopNMax)
	}
	if d := m.TopNDegradado.Load(); d != 0 {
		b.Fatalf("a query DEGRADOU (%d): a bancada mediria o caminho completo com um heap "+
			"de aquecimento por cima", d)
	}
}

// BenchmarkOrdena compara os DOIS caminhos sobre o MESMO corpus.
//
// Os nomes das sub-bancadas são `chave=valor` de propósito: é o que permite
// `benchstat -col /caminho` pôr top-N e completo lado a lado com o delta e o
// intervalo, em vez de obrigar quem lê a parear as linhas na mão.
//
// Reproduzir:
//
//	go test ./internal/query/executor/ -run '^$' -bench BenchmarkOrdena \
//	  -benchmem -benchtime=20x -count=6 | tee novo.txt
//	benchstat -col /caminho novo.txt
func BenchmarkOrdena(b *testing.B) {
	medeOsDoisCaminhos(b, 100_000, []string{"aleatorio", "pior", "melhor"},
		[]int64{10, 100, 1_000, 4_096, 16_384})
}

// BenchmarkOrdenaCorpusCurto é a bancada que de fato FIXA o teto de N.
//
// Sobre 100 mil linhas o heap ganha em toda faixa medida, então a pergunta
// "onde o teto fica" não se responde lá. Ela se responde aqui: o custo do heap
// é O(varrido × log N) mais uma reescrita por linha ACEITA, e o do sort é
// O(varrido × log varrido) mais uma cópia por linha. Quando N se aproxima do
// número de linhas varridas, os dois expoentes se encontram — e o heap ainda
// paga o sift.
//
// O número de linhas varridas é DESCONHECIDO no Open, que é quando o modo é
// escolhido. O teto é a única defesa contra o caso em que ele é pequeno.
func BenchmarkOrdenaCorpusCurto(b *testing.B) {
	medeOsDoisCaminhos(b, 20_000, []string{"pior", "aleatorio"},
		[]int64{1_024, 4_096, 16_384})
}

func medeOsDoisCaminhos(b *testing.B, rows int, formas []string, tamanhos []int64) {
	b.Helper()
	c := compileSchemaTopN(b)
	chaves := []binder.BoundOrderKey{keyName(b, c, false, false), keyN(b, c, true)}

	for _, forma := range formas {
		source := buildSource(b, c, corpusOfBench(forma, rows))
		for _, n := range tamanhos {
			bq := bqTopN(b, c, chaves, n, 0)
			// bqTopN corta o limite efetivo em maxRows; a bancada precisa do N
			// pedido inteiro para medir o heap desse tamanho.
			bq.MaxRows, bq.EffectiveLimit = 1_000_000, n
			for _, path := range []string{"topn", "completo"} {
				// O lado rápido leva TopNMax = n, e não o teto padrão: sem
				// isso o n=16384 cairia no caminho completo dos DOIS lados e a
				// bancada compararia o caminho completo consigo mesmo — foi o
				// que aconteceu na primeira rodada, e é exatamente o número que
				// falta para justificar onde o teto fica.
				opts := executor.Options{Store: source, TopNMax: n}
				if path == "completo" {
					opts.TopNMax = -1
				}
				b.Run(fmt.Sprintf("forma=%s/n=%d/caminho=%s", forma, n, path), func(b *testing.B) {
					confereModo(b, bq, opts, path == "topn")
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						it, err := executor.Open(context.Background(), bq, planner.FullScan(), opts)
						if err != nil {
							b.Fatal(err)
						}
						for it.Next() {
						}
						if err := it.Err(); err != nil {
							b.Fatal(err)
						}
						if err := it.Close(); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
