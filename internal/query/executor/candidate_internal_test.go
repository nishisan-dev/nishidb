package executor

import (
	"errors"
	"reflect"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// listaIter é um iterador ascendente de teste. A ordem crescente é
// PRÉ-CONDIÇÃO do storage; um dublê desordenado quebraria a interseção de um
// jeito que aparece como "resultado quase certo".
type listaIter struct {
	ids     []types.RowID
	pos     int
	err     error
	erroEm  int // devolve err ao chegar neste índice (0 = nunca)
	fechado int
}

func (l *listaIter) Next() bool {
	if l.erroEm > 0 && l.pos >= l.erroEm {
		l.err = errors.New("iterador falhou")
		return false
	}
	if l.pos >= len(l.ids) {
		return false
	}
	l.pos++
	return true
}

func (l *listaIter) RowID() types.RowID { return l.ids[l.pos-1] }
func (l *listaIter) Err() error         { return l.err }
func (l *listaIter) Close() error       { l.fechado++; return nil }

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

func lista(ids ...types.RowID) *listaIter { return &listaIter{ids: ids} }

func TestIntersecaoKWay(t *testing.T) {
	casos := []struct {
		nome string
		its  []types.RowIDIterator
		want []types.RowID
	}{
		{"sobreposição parcial", []types.RowIDIterator{
			lista(1, 3, 5, 7, 9), lista(3, 5, 6, 9), lista(2, 3, 9, 10)},
			[]types.RowID{3, 9}},
		{"idênticos", []types.RowIDIterator{lista(1, 2, 3), lista(1, 2, 3)},
			[]types.RowID{1, 2, 3}},
		{"disjuntos", []types.RowIDIterator{lista(1, 3), lista(2, 4)}, nil},
		// Uma fonte vazia zera a interseção — e é o caso que um laço mal
		// escrito trata como "ignora esta fonte", devolvendo linhas a mais.
		{"uma fonte vazia", []types.RowIDIterator{lista(1, 2, 3), lista()}, nil},
		{"salto grande", []types.RowIDIterator{lista(1, 1000), lista(1000)},
			[]types.RowID{1000}},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			it := &intersectIter{its: tc.its, cur: make([]types.RowID, len(tc.its))}
			if got := drena(t, it); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("interseção = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestUniaoComDedupKWay(t *testing.T) {
	casos := []struct {
		nome string
		its  []types.RowIDIterator
		want []types.RowID
	}{
		// O caso que morde: o mesmo id em TODAS as fontes. Consumir só a
		// primeira devolveria a linha k vezes.
		{"mesmo id em todas", []types.RowIDIterator{lista(5), lista(5), lista(5)},
			[]types.RowID{5}},
		{"sobreposição parcial", []types.RowIDIterator{
			lista(1, 4, 7), lista(2, 4, 8), lista(4, 9)},
			[]types.RowID{1, 2, 4, 7, 8, 9}},
		{"disjuntos", []types.RowIDIterator{lista(1, 3), lista(2, 4)},
			[]types.RowID{1, 2, 3, 4}},
		{"uma fonte vazia", []types.RowIDIterator{lista(1, 2), lista()},
			[]types.RowID{1, 2}},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			it := &unionIter{its: tc.its, cur: make([]types.RowID, len(tc.its)),
				vivo: make([]bool, len(tc.its))}
			if got := drena(t, it); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("união = %v, want %v", got, tc.want)
			}
		})
	}
}

// Erro no meio de uma fonte não pode virar resultado silenciosamente
// incompleto: ele sobe pelo Err().
func TestErroDeFonteSobeNaUniaoEInterseccao(t *testing.T) {
	falha := func() *listaIter { return &listaIter{ids: []types.RowID{1, 2, 3}, erroEm: 2} }

	u := &unionIter{its: []types.RowIDIterator{falha(), lista(9)},
		cur: make([]types.RowID, 2), vivo: make([]bool, 2)}
	for u.Next() {
	}
	if u.Err() == nil {
		t.Error("união engoliu o erro da fonte")
	}

	i := &intersectIter{its: []types.RowIDIterator{falha(), lista(1, 2, 3)},
		cur: make([]types.RowID, 2)}
	for i.Next() {
	}
	if i.Err() == nil {
		t.Error("interseção engoliu o erro da fonte")
	}
}

func TestFechamentoFechaTodosOsIteradores(t *testing.T) {
	a, b := lista(1), lista(2)
	u := &unionIter{its: []types.RowIDIterator{a, b}, cur: make([]types.RowID, 2), vivo: make([]bool, 2)}
	if err := u.Close(); err != nil {
		t.Fatal(err)
	}
	if a.fechado != 1 || b.fechado != 1 {
		t.Errorf("fechados = %d/%d, want 1/1", a.fechado, b.fechado)
	}
}
