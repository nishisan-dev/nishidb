package schema

import (
	"errors"
	"sync"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// InMemoryDict é o FieldDictionary de uma tabela VIRTUAL: ids emitidos
// na ordem em que os nomes aparecem, sem disco e sem type hint.
//
// Existe aqui, e não no pacote que usa, porque toda tabela virtual precisa de
// um para compilar a sua spec sintética pelo compilador real — e reusar o
// compilador é o que garante que ela obedeça às mesmas regras de tipos, chave
// e ordem determinística de FieldID que um dataset de verdade. A primeira
// tabela virtual trouxe uma cópia deste objeto; a segunda o traria de novo, e
// a terceira teria três lugares para o mesmo comportamento.
//
// SetTypeHint RECUSA: o tipo de cada coluna sintética está escrito no YAML da
// tabela, e um hint que o sobrescrevesse seria a spec deixando de mandar.
type InMemoryDict struct {
	mu    sync.Mutex
	ids   map[string]types.FieldID
	names []string
}

// NewInMemoryDict devolve um dicionário vazio, pronto para o Compile.
func NewInMemoryDict() *InMemoryDict {
	return &InMemoryDict{ids: map[string]types.FieldID{}}
}

// LookupOrCreate devolve o id do nome, emitindo um novo na primeira vez que
// ele aparece. Os ids começam em 1: o zero é reservado.
func (d *InMemoryDict) LookupOrCreate(name []byte) (types.FieldID, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if id, ok := d.ids[string(name)]; ok {
		return id, nil
	}
	d.names = append(d.names, string(name))
	id := types.FieldID(len(d.names))
	d.ids[string(name)] = id
	return id, nil
}

// TypeHint responde SEMPRE "não sei": o tipo de cada coluna está no YAML da
// tabela virtual, e não num hint acumulado da ingestão que ela não tem.
func (d *InMemoryDict) TypeHint(types.FieldID) (types.FieldType, bool) {
	return types.TypeNull, false
}

// SetTypeHint recusa: ver o godoc do tipo.
func (d *InMemoryDict) SetTypeHint(types.FieldID, types.FieldType) error {
	return errors.New("schema: a tabela virtual não fixa tipo")
}

// Len é quantos nomes já foram emitidos.
func (d *InMemoryDict) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.names)
}

// Name resolve o id para o nome. ok=false para o zero e para id além do fim.
func (d *InMemoryDict) Name(id types.FieldID) (string, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if id < 1 || int(id) > len(d.names) {
		return "", false
	}
	return d.names[id-1], true
}
