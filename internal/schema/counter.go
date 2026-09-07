package schema

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/nishisan-dev/nishidb/internal/units"
)

// Os contadores mantidos na escrita (planning/contadores-mantidos-na-escrita.md)
// são declarados no bloco `counters:` do YAML do dataset:
//
//	counters:
//	  - name: autenticados_por_olt
//	    groupBy: [olt]
//	    where: authenticated = true      # opcional; ausente conta toda linha
//	    maxGroups: 65536                 # opcional
//	    retainEmptyGroups: 7d            # opcional; `never` mantém para sempre
//
// A leitura sai por SQL em `_counters.<name>`, uma tabela por contador com a
// coluna `groupKey`, as colunas do groupBy abertas e tipadas, e `total`.
const (
	// DefaultMaxGroups é o teto de grupos vivos quando o YAML não declara. É a
	// proteção contra um groupBy declarado, por engano, sobre uma coluna de
	// alta cardinalidade: o nível de OLT cabe folgado; PON e splitter exigem
	// declarar.
	DefaultMaxGroups = 65_536
	// MaxGroupsPhysicalCeiling é o maior teto declarável: o vetor de grupos guarda
	// o id em 31 bits, e o bit restante é a condição.
	MaxGroupsPhysicalCeiling = 1<<31 - 1
	// DefaultRetainEmptyGroups é quanto tempo um grupo sem linhas continua
	// visível com total zero. O critério é operacional (planning §6.5): maior
	// que o tempo entre um equipamento apagar por inteiro e alguém agir.
	DefaultRetainEmptyGroups = 7 * 24 * time.Hour
	// CountersNamespace é o espaço de nome virtual das tabelas dos contadores.
	CountersNamespace = "_counters"
	// CounterKeyColumn é a key de toda tabela `_counters.<name>`: o texto
	// canônico do grupo. Existe porque a key da v1 é uma coluna string única, e
	// um groupBy composto não tem coluna que identifique a linha.
	CounterKeyColumn = "groupKey"
	// CounterTotalColumn é a coluna com a contagem.
	CounterTotalColumn = "total"
)

// CounterSpec é um contador validado, com defaults aplicados.
type CounterSpec struct {
	Name string
	// GroupBy são as colunas de agrupamento, na ordem declarada — a ordem dos
	// componentes da chave e das colunas da tabela.
	GroupBy []string
	// Where é a condição normalizada (espaços colapsados); "" é sem condição.
	// NÃO é validada aqui: precisa do schema compilado, como o when do TTL.
	Where string
	// MaxGroups é o teto de grupos vivos.
	MaxGroups uint32
	// RetainEmptyGroups é o prazo de visibilidade do grupo vazio; zero é never.
	RetainEmptyGroups time.Duration
}

// groupingTypes são os tipos que o groupBy aceita. Três, e não a lista de
// indexáveis: são os únicos com renderização textual declarada para o groupKey,
// e um tipo novo aqui exige a renderização antes.
var groupingTypes = map[string]bool{"string": true, "int64": true, "boolean": true}

// rawCounter é um item do bloco como lido do YAML, antes da validação.
type rawCounter struct {
	Name              string
	GroupBy           []string
	Where             *string
	MaxGroups         *int64
	RetainEmptyGroups string
	hasGroupBy        bool
}

// countersFromNode decodifica o bloco `counters`. Ausente devolve nil; lista
// vazia devolve slice vazio (e a validação recusa). Cada item passa por bloco()
// para que uma chave desconhecida (`wher: …`) seja recusada em vez de
// silenciosamente ignorada.
func countersFromNode(n *yaml.Node) ([]rawCounter, error) {
	for n != nil && n.Kind == yaml.AliasNode {
		n = n.Alias
	}
	if absent(n) {
		return nil, nil
	}
	if n.Kind != yaml.SequenceNode {
		return nil, specErrf("counters deve ser uma lista de contadores")
	}
	out := make([]rawCounter, 0, len(n.Content))
	for i, item := range n.Content {
		keyPath := fmt.Sprintf("counters[%d]", i)
		keys, err := block(item, keyPath, "name", "groupBy", "where", "maxGroups", "retainEmptyGroups")
		if err != nil {
			return nil, err
		}
		var r rawCounter
		for key, value := range keys {
			var err error
			switch key {
			case "name":
				err = decodeInto(value, keyPath+".name", &r.Name)
			case "groupBy":
				r.hasGroupBy = true
				err = decodeInto(value, keyPath+".groupBy", &r.GroupBy)
			case "where":
				var w string
				err = decodeInto(value, keyPath+".where", &w)
				r.Where = &w
			case "maxGroups":
				var m int64
				err = decodeInto(value, keyPath+".maxGroups", &m)
				r.MaxGroups = &m
			case "retainEmptyGroups":
				err = decodeInto(value, keyPath+".retainEmptyGroups", &r.RetainEmptyGroups)
			}
			if err != nil {
				return nil, err
			}
		}
		out = append(out, r)
	}
	return out, nil
}

// validateCounters confere a lista inteira e devolve a forma validada.
//
// fields são os campos declarados; as colunas extraídas entram pelo nome que
// derivedColumnName lhes dá. key e seq são os papéis que o groupBy não pode
// usar: agrupar pela key é um grupo por entidade, e pela sequence é agrupar
// pela ordem do evento — os dois são cardinalidade sem teto por engano.
func validateCounters(raws []rawCounter, fields map[string]FieldSpec, key, seq string) ([]CounterSpec, error) {
	if len(raws) == 0 {
		return nil, specErrf("counters: lista vazia — remova a chave ou declare um contador")
	}
	columns, err := declaredColumns(fields)
	if err != nil {
		return nil, err
	}
	seenNames := make(map[string]int, len(raws))
	out := make([]CounterSpec, 0, len(raws))
	for i, r := range raws {
		keyPath := fmt.Sprintf("counters[%d]", i)
		if r.Name == "" {
			return nil, specErrf("%s.name obrigatório", keyPath)
		}
		if !isIdentName(r.Name) {
			return nil, specErrf("%s.name %q inválido (esperado [a-z0-9_]+)", keyPath, r.Name)
		}
		if j, dup := seenNames[r.Name]; dup {
			return nil, specErrf("%s.name %q repetido (já declarado em counters[%d])", keyPath, r.Name, j)
		}
		seenNames[r.Name] = i
		if !r.hasGroupBy || len(r.GroupBy) == 0 {
			return nil, specErrf("%s.groupBy obrigatório (lista de colunas de agrupamento)", keyPath)
		}
		seenColumns := make(map[string]bool, len(r.GroupBy))
		for _, col := range r.GroupBy {
			if seenColumns[col] {
				return nil, specErrf("%s.groupBy: coluna %q repetida", keyPath, col)
			}
			seenColumns[col] = true
			if err := validateGroupingColumn(keyPath, col, columns, key, seq); err != nil {
				return nil, err
			}
		}
		c := CounterSpec{Name: r.Name, GroupBy: r.GroupBy, MaxGroups: DefaultMaxGroups, RetainEmptyGroups: DefaultRetainEmptyGroups}
		if r.Where != nil {
			w := strings.Join(strings.Fields(*r.Where), " ")
			if w == "" {
				return nil, specErrf("%s.where vazio — omita a chave para contar toda linha com grupo", keyPath)
			}
			c.Where = w
		}
		if r.MaxGroups != nil {
			if *r.MaxGroups < 1 || *r.MaxGroups > MaxGroupsPhysicalCeiling {
				return nil, specErrf("%s.maxGroups %d fora de [1, %d]", keyPath, *r.MaxGroups, MaxGroupsPhysicalCeiling)
			}
			c.MaxGroups = uint32(*r.MaxGroups)
		}
		if r.RetainEmptyGroups != "" {
			d, err := visibilityDuration(keyPath, r.RetainEmptyGroups)
			if err != nil {
				return nil, err
			}
			c.RetainEmptyGroups = d
		}
		out = append(out, c)
	}
	return out, nil
}

// declaredColumns devolve nome → tipo declarado de toda coluna que o groupBy
// pode referenciar: os campos de topo e as colunas extraídas. A congelada entra
// sem tipo (ela o herda na compilação) e por isso é recusada como "sem tipo
// declarado" — agrupar por uma coluna cujo tipo só se conhece depois seria
// decidir a chave do grupo tarde demais.
func declaredColumns(fields map[string]FieldSpec) (map[string]string, error) {
	out := make(map[string]string, len(fields))
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		f := fields[name]
		out[name] = f.Type
		for i, e := range f.Extract {
			column, err := derivedColumnName(name, i, e)
			if err != nil {
				return nil, err
			}
			out[column] = e.Type
		}
	}
	return out, nil
}

func validateGroupingColumn(keyPath, col string, columns map[string]string, key, seq string) error {
	switch col {
	case CounterKeyColumn, CounterTotalColumn:
		return specErrf("%s.groupBy: coluna %q colide com a coluna %s da tabela %s.<name>; renomeie a coluna no schema",
			keyPath, col, col, CountersNamespace)
	case key:
		return specErrf("%s.groupBy: coluna %q é a key do dataset — agrupar pela key é um grupo por entidade", keyPath, col)
	case seq:
		if seq != "" {
			return specErrf("%s.groupBy: coluna %q é a sequence do dataset — a ordem do evento não é um grupo", keyPath, col)
		}
	}
	typ, ok := columns[col]
	if !ok {
		return specErrf("%s.groupBy: coluna %q não declarada em schema.fields (nem como extração)", keyPath, col)
	}
	if typ == "" || typ == "auto" {
		return specErrf("%s.groupBy: coluna %q precisa de tipo declarado (é auto ou herdado) — a chave do grupo é decidida pelo tipo",
			keyPath, col)
	}
	if !groupingTypes[typ] {
		return specErrf("%s.groupBy: coluna %q tem tipo %q; groupBy aceita string, int64 e boolean", keyPath, col, typ)
	}
	return nil
}

// visibilityDuration lê retainEmptyGroups com as mesmas regras do
// expireAfterWrite: `never` é zero, zero explícito é recusado apontando para
// `never`, e abaixo da resolução do coletor é recusado.
func visibilityDuration(keyPath, raw string) (time.Duration, error) {
	if raw == DurationNever {
		return 0, nil
	}
	if isZeroDuration(raw) {
		return 0, specErrf("%s.retainEmptyGroups %q não declara prazo — use %q para manter o grupo vazio para sempre",
			keyPath, raw, DurationNever)
	}
	d, err := units.DurationWithDays(raw, 0)
	if err != nil {
		return 0, specErrf("%s.retainEmptyGroups: %s", keyPath, withoutSpecPrefix(err))
	}
	if d < ExpiryResolution {
		return 0, specErrf("%s.retainEmptyGroups %s é menor que a resolução do coletor (%s) — use %q para não aposentar",
			keyPath, raw, ExpiryResolution, DurationNever)
	}
	return d, nil
}
