package executor

import (
	"errors"
	"reflect"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/planner"
	"github.com/nishisan-dev/nishidb/internal/schema"

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

// storeFalhaNoSegundo abre o primeiro índice e falha no segundo.
type storeFalhaNoSegundo struct {
	abertos []*listaIter
	n       int
}

func (s *storeFalhaNoSegundo) GetByRowID(types.Snapshot, types.RowID, []byte) (types.Record, []byte, bool, error) {
	return types.Record{}, nil, false, nil
}

func (s *storeFalhaNoSegundo) GetByKey(types.Snapshot, []byte, []byte) (types.Record, []byte, bool, error) {
	return types.Record{}, nil, false, nil
}

func (s *storeFalhaNoSegundo) ScanLive(types.Snapshot) types.RowIDIterator { return lista() }

func (s *storeFalhaNoSegundo) AbreSnapshot() types.Snapshot { return types.Snapshot{} }

func (s *storeFalhaNoSegundo) FechaSnapshot(types.Snapshot) {}

func (s *storeFalhaNoSegundo) Lookup(types.FieldID, types.Value) (types.RowIDIterator, error) {
	s.n++
	if s.n >= 2 {
		return nil, errors.New("índice indisponível")
	}
	it := lista(1, 2, 3)
	s.abertos = append(s.abertos, it)
	return it, nil
}

// Falha ao abrir o termo i FECHA os i-1 já abertos. Vazar iterador de índice
// num caminho de erro é o tipo de coisa que só aparece em produção, como
// descritor esgotado depois de horas.
func TestAbreTermosFechaOsJaAbertosNoErro(t *testing.T) {
	st := &storeFalhaNoSegundo{}
	campo := &schema.CompiledField{ID: 1, Name: "x", Indexado: true}
	termos := []planner.IndexTerm{{Field: campo}, {Field: campo}}

	if _, err := abreTermos(termos, st); err == nil {
		t.Fatal("abreTermos não propagou o erro")
	}
	if len(st.abertos) != 1 {
		t.Fatalf("%d iteradores abertos, want 1", len(st.abertos))
	}
	if st.abertos[0].fechado != 1 {
		t.Errorf("o iterador já aberto foi fechado %d vezes, want 1", st.abertos[0].fechado)
	}
}

// TestAbreTermosSemCapabilityDeHash saiu daqui: ele cobria o ramo
// `hash == nil` do abreTermos, e esse ramo era a capability OPCIONAL
// HashLookuper — detectada por type assertion e ausente do recorte Store. Com o
// Lookup dentro da interface, um store que não o implementa não compila, e o
// erro que o teste afirmava não tem mais como ser produzido em runtime.
