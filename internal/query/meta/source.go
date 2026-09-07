package meta

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func errorsAs(err error, target any) bool { return errors.As(err, target) }

// dicionario é o FieldDictionary da tabela virtual: em memória, sem disco,
// sem durabilidade. A tabela não persiste nada — ela é recalculada a cada
// consulta a partir do schema compilado do dataset alvo.
type dicionario struct {
	mu    sync.Mutex
	ids   map[string]types.FieldID
	nomes []string
}

func novoDicionario() *dicionario {
	return &dicionario{ids: map[string]types.FieldID{}}
}

func (d *dicionario) LookupOrCreate(name []byte) (types.FieldID, error) {
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

func (d *dicionario) TypeHint(types.FieldID) (types.FieldType, bool) { return types.TypeNull, false }
func (d *dicionario) SetTypeHint(types.FieldID, types.FieldType) error {
	return errors.New("meta: a tabela virtual não fixa tipo")
}

func (d *dicionario) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.nomes)
}

func (d *dicionario) Name(id types.FieldID) (string, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if id < 1 || int(id) > len(d.nomes) {
		return "", false
	}
	return d.nomes[id-1], true
}

// fonte é o Store virtual. Ele não lê disco: cada rowID endereça um campo do
// schema compilado do dataset alvo, e o record é codificado sob demanda.
//
// O snapshot dos campos é tirado UMA vez, no Abre. Sem ele, um campo
// autodescoberto registrado no meio da varredura mudaria o tamanho da fonte
// entre o ScanLive e o GetByRowID, e a query veria uma linha a mais ou a
// menos sem explicação.
type fonte struct {
	campos []*schema.CompiledField
	caps   Capacidades
	ids    map[string]types.FieldID
}

func novaFonte(alvo *schema.Compiled, caps Capacidades, _ *schema.Compiled) *fonte {
	return &fonte{campos: alvo.FieldsParaMetadata(), caps: caps, ids: colunasIDs}
}

func (f *fonte) GetByRowID(_ types.Snapshot, id types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	if int(id) >= len(f.campos) {
		return types.Record{}, buf, false, nil
	}
	return f.record(id, f.campos[id], buf)
}

func (f *fonte) GetByKey(_ types.Snapshot, key, buf []byte) (types.Record, []byte, bool, error) {
	for i, c := range f.campos {
		if c.Name == string(key) {
			return f.record(types.RowID(i), c, buf)
		}
	}
	return types.Record{}, buf, false, nil
}

func (f *fonte) ScanLive(types.Snapshot) types.RowIDIterator {
	return &iterador{n: len(f.campos), i: -1}
}

// OpenSnapshot devolve o snapshot ZERO, e o ignora na leitura: a fonte já é um
// retrato, tirado uma vez no Abre (ver o godoc do tipo). Não há relógio que
// ande nem rowId que nasça no meio da varredura — o que o snapshot congela no
// Store de verdade, aqui já está congelado por construção.
func (f *fonte) OpenSnapshot() types.Snapshot { return types.Snapshot{} }

// CloseSnapshot é no-op porque o Abre não retém nada: não há journal de
// versões sobre uma fonte que não é escrita.
func (f *fonte) CloseSnapshot(types.Snapshot) {}

// Indexed responde SEMPRE false: a tabela virtual não declara índice no seu
// YAML, então o planner degrada para varredura completa — que sobre sete
// colunas e algumas dezenas de linhas é o plano certo de qualquer forma.
// Confundir esta resposta com a do dataset ALVO seria o defeito: a coluna
// `indexed` fala do alvo, este método fala da tabela.
func (f *fonte) Indexed(types.FieldID) bool { return false }

// Lookup nunca é chamado: a tabela virtual não declara índice nenhum no
// YAML, então o planner jamais escolhe um plano indexado sobre ela. O erro
// aqui é rede de segurança contra um plano impossível, não um caminho vivo.
func (f *fonte) Lookup(field types.FieldID, _ types.Value) (types.RowIDIterator, error) {
	return nil, fmt.Errorf("meta: a tabela virtual não tem índice (campo %d)", field)
}

// record codifica UM campo do dataset alvo como uma linha da tabela.
//
// O payload exige FieldID crescente, sem duplicata e sem nulo — a ordenação
// abaixo é o que honra isso, e ela é sobre os IDs que o compilador atribuiu,
// não sobre a ordem em que as colunas foram escritas na spec.
func (f *fonte) record(row types.RowID, c *schema.CompiledField, buf []byte) (types.Record, []byte, bool, error) {
	tipo := "unknown"
	if t := c.EffectiveType(); t != types.TypeNull {
		tipo = t.String()
	}
	// `indexed` fala do MOTOR, e a coluna `index` ao lado fala do que o YAML
	// declara. As duas juntas é que respondem a pergunta do operador: "declarei
	// e está valendo?". Enquanto só o bitmap tinha motor, uma coluna com
	// `index: hash` saía com index=hash e indexed=false — correto na época,
	// mentira a partir do motor físico.
	indexado := false
	if f.caps != nil {
		indexado = f.caps.Indexed(c.ID)
	}

	vals := []struct {
		nome string
		v    types.Value
	}{
		{"name", texto(c.Name)},
		{"type", texto(tipo)},
		{"auto", booleano(c.IsAuto)},
		{"index", texto(c.NomeDoIndice())},
		{"indexed", booleano(indexado)},
		{"id", types.Value{Type: types.TypeInt64, I64: int64(c.ID)}},
	}
	// `parent` só existe para a coluna derivada. Campo de topo o deixa
	// AUSENTE, e não vazio: NULL é ausência, e string vazia diria "tem pai, e
	// o nome dele é nada".
	//
	// `occurrences`, `typeMismatches` e `coercions` seguem o MESMO precedente, e
	// pela mesma razão: são contadores de coluna DERIVADA. Num campo de topo
	// eles seriam sempre 0, e 0 é indistinguível de "a chave nunca apareceu" — a
	// coluna mentiria com um número em vez de calar. Como efeito colateral
	// bem-vindo, `WHERE typeMismatches > 0` já filtra o topo naturalmente.
	if c.Parent != nil {
		vals = append(vals, struct {
			nome string
			v    types.Value
		}{"parent", texto(c.Parent.Name)},
			struct {
				nome string
				v    types.Value
			}{"occurrences", inteiro(c.Ocorrencias())},
			struct {
				nome string
				v    types.Value
			}{"typeMismatches", inteiro(c.Divergencias())},
			struct {
				nome string
				v    types.Value
			}{"coercions", inteiro(c.Coercoes())})
	}

	type par struct {
		id types.FieldID
		v  types.Value
	}
	pares := make([]par, 0, len(vals))
	for _, kv := range vals {
		id, ok := f.ids[kv.nome]
		if !ok {
			return types.Record{}, buf, false, fmt.Errorf("meta: coluna %q sem FieldID", kv.nome)
		}
		pares = append(pares, par{id, kv.v})
	}
	sort.Slice(pares, func(i, j int) bool { return pares[i].id < pares[j].id })

	buf = buf[:0]
	var err error
	for _, p := range pares {
		if buf, err = encoding.AppendField(buf, p.id, p.v); err != nil {
			return types.Record{}, buf, false, fmt.Errorf("meta: codificando %d: %w", p.id, err)
		}
	}
	return types.Record{
		RowID:    row,
		Sequence: int64(row),
		Key:      []byte(c.Name),
		Payload:  buf,
	}, buf, true, nil
}

func texto(s string) types.Value { return types.Value{Type: types.TypeString, Bytes: []byte(s)} }

// inteiro converte um contador para o int64 da coluna. Os contadores são
// uint64 e a coluna é int64 porque o SQL do produto não tem inteiro sem sinal:
// estourar o int64 exigiria 9,2 quintilhões de ocorrências numa única coluna, o
// que a 500 ev/s levaria mais de meio bilhão de anos.
func inteiro(n uint64) types.Value {
	return types.Value{Type: types.TypeInt64, I64: int64(n)}
}

func booleano(b bool) types.Value {
	v := types.Value{Type: types.TypeBool}
	if b {
		v.I64 = 1
	}
	return v
}

// iterador percorre os rowIDs da tabela virtual, que são densos e começam em
// zero — como os do storage real.
type iterador struct {
	n int
	i int
}

func (it *iterador) Next() bool {
	if it.i+1 >= it.n {
		it.i = it.n
		return false
	}
	it.i++
	return true
}

func (it *iterador) RowID() types.RowID { return types.RowID(it.i) }
func (it *iterador) Err() error         { return nil }
func (it *iterador) Close() error       { return nil }
