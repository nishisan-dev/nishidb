package storage

import (
	"context"
	"strconv"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// BenchmarkReconstroiContadores1M mede a reconstrução em operação — a
// varredura das linhas vivas sob o wmu que /v1/counters/rebuild paga, e que a
// abertura paga quando os parâmetros de um contador mudaram — sobre um milhão
// de linhas vivas e dois contadores por OLT (2.048 grupos, um terço das linhas
// fora da condição). O número que interessa é o custo por linha: é a régua de
// "ordem do full scan" que o plano fixou para a reconstrução, e é o tempo em
// que o writer fica parado num rebuild.
func BenchmarkReconstroiContadores1M(b *testing.B) {
	const linhas = 1 << 20
	s := openStore(b, Options{
		Dir:           b.TempDir(),
		SegmentSize:   64 << 20,
		EstimatedRows: linhas,
		CompilaContadores: callbackDeContadores(
			contadorDeTeste{nome: "aut", groupBy: []string{"olt"}, where: true, teto: 4096},
			contadorDeTeste{nome: "serv", groupBy: []string{"olt"}, teto: 4096},
		),
	})
	dict := s.FieldDict()
	idID, err := dict.LookupOrCreate([]byte("id"))
	if err != nil {
		b.Fatal(err)
	}
	oltID, err := dict.LookupOrCreate([]byte("olt"))
	if err != nil {
		b.Fatal(err)
	}
	authID, err := dict.LookupOrCreate([]byte("authenticated"))
	if err != nil {
		b.Fatal(err)
	}
	// O payload é montado em ordem crescente de FieldID; os três ids saem do
	// dicionário na ordem em que foram internados, e o callback interna olt e
	// authenticated antes de id.
	ordem := []types.FieldID{idID, oltID, authID}
	for i := 1; i < len(ordem); i++ {
		for j := i; j > 0 && ordem[j] < ordem[j-1]; j-- {
			ordem[j], ordem[j-1] = ordem[j-1], ordem[j]
		}
	}
	var buf, chave, olt []byte
	var autenticadosNaOLT0 int64
	for i := 0; i < linhas; i++ {
		chave = strconv.AppendInt(chave[:0], int64(i), 10)
		olt = append(olt[:0], "OLT"...)
		olt = strconv.AppendInt(olt, int64(i%2048), 10)
		auth := i%3 != 0
		if i%2048 == 0 && auth {
			autenticadosNaOLT0++
		}
		buf = buf[:0]
		for _, id := range ordem {
			var v types.Value
			switch id {
			case idID:
				v = types.Value{Type: types.TypeString, Bytes: chave}
			case oltID:
				v = types.Value{Type: types.TypeString, Bytes: olt}
			default:
				v = types.Value{Type: types.TypeBool}
				if auth {
					v.I64 = 1
				}
			}
			buf = mustAppendField(b, buf, id, v)
		}
		if _, _, err := s.Put(chave, 0, 0, buf, nil); err != nil {
			b.Fatal(err)
		}
	}
	if err := s.Checkpoint(); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.ReconstroiContadores(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	// A reconstrução tem de reproduzir os totais — senão o número medido é o
	// de uma varredura que não fez o trabalho.
	for _, c := range s.Contadores() {
		id, ok := c.PorTexto([]byte("OLT0"))
		if !ok {
			b.Fatalf("%s: OLT0 não está no dicionário depois da reconstrução", c.Def().Nome)
		}
		_, _, total, refs, _ := c.Grupo(id)
		quer := int64(linhas / 2048)
		if c.Def().Nome == "aut" {
			quer = autenticadosNaOLT0
		}
		if total != quer || refs != linhas/2048 {
			b.Fatalf("%s[OLT0]: total %d refs %d, quer %d e %d", c.Def().Nome, total, refs, quer, linhas/2048)
		}
	}
}
