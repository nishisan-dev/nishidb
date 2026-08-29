package segindex

// Este arquivo existe só para o pacote de teste COMPILAR antes de a Tarefa 2
// implementar a leitura de verdade em selado.go.
//
// TestOEscritorAgrupaOrdenaEDeduplicaAsPostings (escritor_test.go) referencia
// AbreSelado e o tipo devolvido porque esse é o teste verbatim da Tarefa 1 —
// ele já nasce com o t.Skip("AbreSelado chega na Tarefa 2") na primeira linha
// do corpo, mas o Go resolve nomes na COMPILAÇÃO, não na execução: um
// `undefined: segindex.AbreSelado` derruba o build do pacote de teste inteiro
// mesmo com o corpo pulado, e os outros dois testes do escritor — que não
// dependem de leitura nenhuma — ficam vermelhos por tabela.
//
// Por isso cada símbolo aqui só existe para satisfazer o compilador: entra em
// pânico se for chamado, e nunca é chamado, porque o único teste que os usa
// está com skip. Não é o leitor selado — não sabe abrir arquivo, não sabe
// fazer busca binária, não sabe nada do layout de formato.go. Sufixo
// `_test.go` também importa: este código nunca entra em `make build` nem em
// nenhum binário de produção, só no binário de teste.
//
// A Tarefa 2 APAGA este arquivo inteiro ao criar selado.go com a
// implementação de verdade, e remove o t.Skip de escritor_test.go.
import "github.com/nishisan-dev/nishidb/internal/types"

// Selado é o placeholder do leitor da forma imutável.
type Selado struct{}

// AbreSelado é o placeholder de abertura do arquivo selado.
func AbreSelado(_ string) (*Selado, error) {
	panic("segindex: AbreSelado ainda não existe (chega na Tarefa 2)")
}

// Fecha é o placeholder de fechamento do arquivo selado. Sem valor de
// retorno de propósito: o teste verbatim da Tarefa 1 chama `defer
// s.Fecha()` sem checar erro, e um retorno `error` aqui só existiria para
// nunca ser lido — o próprio errcheck do lint aponta isso.
func (s *Selado) Fecha() {
	panic("segindex: Selado.Fecha ainda não existe (chega na Tarefa 2)")
}

// Campos é o placeholder da lista de campos indexados no arquivo.
func (s *Selado) Campos() []types.FieldID {
	panic("segindex: Selado.Campos ainda não existe (chega na Tarefa 2)")
}

// MinRowID é o placeholder do menor rowId do segmento.
func (s *Selado) MinRowID() types.RowID {
	panic("segindex: Selado.MinRowID ainda não existe (chega na Tarefa 2)")
}

// MaxRowID é o placeholder do maior rowId do segmento.
func (s *Selado) MaxRowID() types.RowID {
	panic("segindex: Selado.MaxRowID ainda não existe (chega na Tarefa 2)")
}

// Lookup é o placeholder da busca por valor dentro de um campo.
func (s *Selado) Lookup(_ types.FieldID, _ uint64) (*iteradorPlaceholder, bool) {
	panic("segindex: Selado.Lookup ainda não existe (chega na Tarefa 2)")
}

// iteradorPlaceholder é o placeholder do iterador de rowIds devolvido pelo
// Lookup.
type iteradorPlaceholder struct{}

func (i *iteradorPlaceholder) Next() bool {
	panic("segindex: iterador ainda não existe (chega na Tarefa 2)")
}

func (i *iteradorPlaceholder) RowID() types.RowID {
	panic("segindex: iterador ainda não existe (chega na Tarefa 2)")
}

func (i *iteradorPlaceholder) Err() error {
	panic("segindex: iterador ainda não existe (chega na Tarefa 2)")
}
