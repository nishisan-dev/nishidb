package segindex_test

import (
	"errors"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// marcador é o bit que fonteFalsa e fonteComErro usam para sinalizar, DENTRO
// do próprio slice, que um rowId já foi emitido.
//
// Existe porque o literal `fonteFalsa{10, 20, 30}` — usado sem `&` em todos os
// testes deste arquivo — só compila se Next/RowID/etc. tiverem receiver por
// VALOR (o conjunto de métodos de um tipo valor nunca inclui método com
// receiver ponteiro, mesmo quando o valor é endereçável). Um receiver por
// valor não pode manter um campo de posição comum: a mutação vale só para a
// cópia local do método e se perde ao retornar. O que SOBREVIVE entre
// chamadas por essa mesma via é a mutação de um ELEMENTO do slice — todas as
// cópias do header, inclusive a guardada dentro da interface Fonte, apontam
// para o MESMO array de apoio. O bit 63 nunca colide com um rowId de teste (o
// maior usado aqui é a centena do gerador aleatório).
const marcador = types.RowID(1) << 63

// fonteFalsa é o dublê de Fonte sobre um slice já ORDENADO ascendente:
// MinRowID/MaxRowID são o primeiro e o último elemento, sem marca.
type fonteFalsa []types.RowID

// Next marca o menor elemento ainda sem marca — o próximo da ordem ascendente,
// já que a entrada chega ordenada e sem repetição.
func (f fonteFalsa) Next() bool {
	for i := range f {
		if f[i]&marcador == 0 {
			f[i] |= marcador
			return true
		}
	}
	return false
}

// RowID devolve o MAIOR valor já marcado. Como Next() sempre marca o menor
// ainda sem marca e a entrada é ascendente, o maior marcado é exatamente o
// que a última chamada de Next() acabou de marcar — sem precisar de um campo
// de posição à parte, que o receiver por valor não permitiria manter.
func (f fonteFalsa) RowID() types.RowID {
	var maior types.RowID
	achou := false
	for _, v := range f {
		if v&marcador != 0 {
			if valor := v &^ marcador; !achou || valor > maior {
				maior, achou = valor, true
			}
		}
	}
	return maior
}

func (f fonteFalsa) Err() error   { return nil }
func (f fonteFalsa) Close() error { return nil }

// MinRowID devolve math.MaxUint64 quando o slice é vazio — o elemento neutro
// que o contrato da Fonte reserva para "não vou emitir nada" (o gerador
// aleatório do teste de propriedade produz slices vazios de vez em quando).
func (f fonteFalsa) MinRowID() types.RowID {
	if len(f) == 0 {
		return types.RowID(math.MaxUint64)
	}
	return f[0] &^ marcador
}

func (f fonteFalsa) MaxRowID() types.RowID {
	if len(f) == 0 {
		return 0
	}
	return f[len(f)-1] &^ marcador
}

// fonteVaziaDeTeste é uma Fonte que já nasce vazia por declaração: Min(1) >
// Max(0). É o caso central de TestUneDescartaFontesVazias — provar que Une a
// descarta ANTES de decidir entre concatenação e merge, sem consumi-la.
type fonteVaziaDeTeste struct{}

func (fonteVaziaDeTeste) Next() bool            { return false }
func (fonteVaziaDeTeste) RowID() types.RowID    { return 0 }
func (fonteVaziaDeTeste) Err() error            { return nil }
func (fonteVaziaDeTeste) Close() error          { return nil }
func (fonteVaziaDeTeste) MinRowID() types.RowID { return 1 }
func (fonteVaziaDeTeste) MaxRowID() types.RowID { return 0 }

// fonteComErro emite as linhas de rows e, só depois de esgotá-las, reporta
// err — simula uma fonte que falha DEPOIS de entregar algo, não antes. Usa a
// mesma marcação por bit alto de fonteFalsa, pelo mesmo motivo: receiver por
// valor exigido pelo literal sem `&`.
type fonteComErro struct {
	rows []types.RowID
	err  error
}

func (f fonteComErro) Next() bool {
	for i := range f.rows {
		if f.rows[i]&marcador == 0 {
			f.rows[i] |= marcador
			return true
		}
	}
	return false
}

func (f fonteComErro) RowID() types.RowID {
	var maior types.RowID
	achou := false
	for _, v := range f.rows {
		if v&marcador != 0 {
			if valor := v &^ marcador; !achou || valor > maior {
				maior, achou = valor, true
			}
		}
	}
	return maior
}

// Err devolve o defeito configurado incondicionalmente. Os combinadores só
// consultam Err() depois que Next() devolve false — e nesse ponto todas as
// linhas boas já saíram, que é exatamente o "falha no meio" que o teste quer.
func (f fonteComErro) Err() error   { return f.err }
func (f fonteComErro) Close() error { return nil }

func (f fonteComErro) MinRowID() types.RowID {
	if len(f.rows) == 0 {
		return types.RowID(math.MaxUint64)
	}
	return f.rows[0] &^ marcador
}

func (f fonteComErro) MaxRowID() types.RowID {
	if len(f.rows) == 0 {
		return 0
	}
	return f.rows[len(f.rows)-1] &^ marcador
}

// drena consome o iterador até o fim e falha o teste se ele terminar em erro
// — os testes que usam drena não esperam falha, então um erro aqui é sempre
// quebra de teste, não asserção.
func drena(t *testing.T, it types.RowIDIterator) []types.RowID {
	t.Helper()
	var out []types.RowID
	for it.Next() {
		out = append(out, it.RowID())
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iteração: %v", err)
	}
	return out
}

// TestUneConcatenaFaixasDisjuntas: é o caso do histórico, onde o rowId é
// monotônico e os segmentos particionam o espaço. Concatenar custa ZERO
// comparações, contra ~9 por linha de um heap de 500 fontes.
func TestUneConcatenaFaixasDisjuntas(t *testing.T) {
	it := segindex.Une([]segindex.Fonte{
		fonteFalsa{10, 20, 30},
		fonteFalsa{40, 50},
		fonteFalsa{60},
	})
	if got := segindex.EstrategiaDe(it); got != "concatenacao" {
		t.Errorf("estratégia = %q, quero \"concatenacao\"", got)
	}
	if got := drena(t, it); !slices.Equal(got, []types.RowID{10, 20, 30, 40, 50, 60}) {
		t.Errorf("drena = %v", got)
	}
}

// TestUneMergeiaFaixasSobrepostasEDeduplica: é o caso do materializado, onde a
// mesma linha pode ter posting em dois segmentos — gravada com o valor x no
// segmento 5 e regravada com o MESMO x no segmento 9, ao atualizar outro
// campo. Emiti-la duas vezes faria o unionIter do executor devolver a mesma
// linha duplicada num OR.
func TestUneMergeiaFaixasSobrepostasEDeduplica(t *testing.T) {
	it := segindex.Une([]segindex.Fonte{
		fonteFalsa{10, 30, 50},
		fonteFalsa{20, 30, 40},
	})
	if got := segindex.EstrategiaDe(it); got != "merge" {
		t.Errorf("estratégia = %q, quero \"merge\"", got)
	}
	if got := drena(t, it); !slices.Equal(got, []types.RowID{10, 20, 30, 40, 50}) {
		t.Errorf("drena = %v, quero sem repetir o 30", got)
	}
}

// TestUneDescartaFontesVazias: uma fonte vazia reporta Min > Max justamente
// para não participar do teste de disjunção. Sem o descarte, uma fonte vazia
// com Min=0,Max=0 empurraria o histórico inteiro para o caminho de merge.
func TestUneDescartaFontesVazias(t *testing.T) {
	it := segindex.Une([]segindex.Fonte{
		fonteFalsa{10, 20},
		fonteVaziaDeTeste{},
		fonteFalsa{30, 40},
	})
	if got := segindex.EstrategiaDe(it); got != "concatenacao" {
		t.Errorf("estratégia = %q, quero \"concatenacao\" — a vazia não pode forçar o merge", got)
	}
	if got := drena(t, it); !slices.Equal(got, []types.RowID{10, 20, 30, 40}) {
		t.Errorf("drena = %v", got)
	}
}

// TestUneNaoConcatenaQuandoAsFaixasSeTocam trava a fronteira exata: faixas
// [10,20] e [20,30] compartilham o 20 e NÃO são disjuntas. Um `<=` no lugar de
// `<` aqui produziria a mesma linha duas vezes, silenciosamente.
func TestUneNaoConcatenaQuandoAsFaixasSeTocam(t *testing.T) {
	it := segindex.Une([]segindex.Fonte{fonteFalsa{10, 20}, fonteFalsa{20, 30}})
	if got := segindex.EstrategiaDe(it); got != "merge" {
		t.Errorf("estratégia = %q, quero \"merge\"", got)
	}
	if got := drena(t, it); !slices.Equal(got, []types.RowID{10, 20, 30}) {
		t.Errorf("drena = %v, quero o 20 uma vez só", got)
	}
}

// TestUnePropagaOPrimeiroErro: uma fonte que falha no meio não pode virar
// "resultado menor". Falso negativo é o único desfecho que este produto não
// tolera, e resultado truncado sem erro é exatamente isso.
func TestUnePropagaOPrimeiroErro(t *testing.T) {
	quebrada := fonteComErro{rows: []types.RowID{10}, err: errors.New("mmap sumiu")}
	it := segindex.Une([]segindex.Fonte{fonteFalsa{1, 2}, quebrada})
	for it.Next() {
	}
	if it.Err() == nil {
		t.Fatal("Err() = nil, quero o erro da fonte quebrada")
	}
}

// TestUneEhEquivalenteAUniaoOrdenada compara contra um oráculo trivial sobre
// entrada aleatória: junta tudo, ordena, compacta. É este teste que reprova a
// mutação M3 (concatenar sempre) e a M2 (merge sem dedup) da spec.
func TestUneEhEquivalenteAUniaoOrdenada(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for range 200 {
		var fontes []segindex.Fonte
		var esperado []types.RowID
		for range 1 + rng.IntN(6) {
			var rows []types.RowID
			for range rng.IntN(30) {
				rows = append(rows, types.RowID(rng.IntN(100)))
			}
			slices.Sort(rows)
			rows = slices.Compact(rows)
			esperado = append(esperado, rows...)
			fontes = append(fontes, fonteFalsa(rows))
		}
		slices.Sort(esperado)
		esperado = slices.Compact(esperado)
		if got := drena(t, segindex.Une(fontes)); !slices.Equal(got, esperado) {
			t.Fatalf("Une = %v, quero %v", got, esperado)
		}
	}
}
