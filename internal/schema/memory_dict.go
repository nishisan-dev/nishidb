package schema

import (
	"errors"
	"sync"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// DicionarioEmMemoria é o FieldDictionary de uma tabela VIRTUAL: ids emitidos
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
type DicionarioEmMemoria struct {
	mu    sync.Mutex
	ids   map[string]types.FieldID
	nomes []string
}

// NovoDicionarioEmMemoria devolve um dicionário vazio, pronto para o Compile.
func NovoDicionarioEmMemoria() *DicionarioEmMemoria {
	return &DicionarioEmMemoria{ids: map[string]types.FieldID{}}
}

// LookupOrCreate devolve o id do nome, emitindo um novo na primeira vez que
// ele aparece. Os ids começam em 1: o zero é reservado.
func (d *DicionarioEmMemoria) LookupOrCreate(name []byte) (types.FieldID, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if id, ok := d.ids[string(name)]; ok {
		return id, nil
	}
	d.nomes = append(d.nomes, string(name))
	id := types.FieldID(len(d.nomes))
	d.ids[string(name)] = id
	return id, nil
}

// TypeHint responde SEMPRE "não sei": o tipo de cada coluna está no YAML da
// tabela virtual, e não num hint acumulado da ingestão que ela não tem.
func (d *DicionarioEmMemoria) TypeHint(types.FieldID) (types.FieldType, bool) {
	return types.TypeNull, false
}

// SetTypeHint recusa: ver o godoc do tipo.
func (d *DicionarioEmMemoria) SetTypeHint(types.FieldID, types.FieldType) error {
	return errors.New("schema: a tabela virtual não fixa tipo")
}

// Len é quantos nomes já foram emitidos.
func (d *DicionarioEmMemoria) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.nomes)
}

// Name resolve o id para o nome. ok=false para o zero e para id além do fim.
func (d *DicionarioEmMemoria) Name(id types.FieldID) (string, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if id < 1 || int(id) > len(d.nomes) {
		return "", false
	}
	return d.nomes[id-1], true
}
