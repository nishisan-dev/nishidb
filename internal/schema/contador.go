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
	// MaxGroupsPadrao é o teto de grupos vivos quando o YAML não declara. É a
	// proteção contra um groupBy declarado, por engano, sobre uma coluna de
	// alta cardinalidade: o nível de OLT cabe folgado; PON e splitter exigem
	// declarar.
	MaxGroupsPadrao = 65_536
	// MaxGroupsTetoFisico é o maior teto declarável: o vetor de grupos guarda
	// o id em 31 bits, e o bit restante é a condição.
	MaxGroupsTetoFisico = 1<<31 - 1
	// RetainEmptyGroupsPadrao é quanto tempo um grupo sem linhas continua
	// visível com total zero. O critério é operacional (planning §6.5): maior
	// que o tempo entre um equipamento apagar por inteiro e alguém agir.
	RetainEmptyGroupsPadrao = 7 * 24 * time.Hour
	// NamespaceContadores é o espaço de nome virtual das tabelas dos contadores.
	NamespaceContadores = "_counters"
	// ColunaChaveDoContador é a key de toda tabela `_counters.<name>`: o texto
	// canônico do grupo. Existe porque a key da v1 é uma coluna string única, e
	// um groupBy composto não tem coluna que identifique a linha.
	ColunaChaveDoContador = "groupKey"
	// ColunaTotalDoContador é a coluna com a contagem.
	ColunaTotalDoContador = "total"
)

// ContadorSpec é um contador validado, com defaults aplicados.
type ContadorSpec struct {
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

// tiposDeAgrupamento são os tipos que o groupBy aceita. Três, e não a lista de
// indexáveis: são os únicos com renderização textual declarada para o groupKey,
// e um tipo novo aqui exige a renderização antes.
var tiposDeAgrupamento = map[string]bool{"string": true, "int64": true, "boolean": true}

// rawContador é um item do bloco como lido do YAML, antes da validação.
type rawContador struct {
	Name              string
	GroupBy           []string
	Where             *string
	MaxGroups         *int64
	RetainEmptyGroups string
	temGroupBy        bool
}

// contadoresDoNo decodifica o bloco `counters`. Ausente devolve nil; lista
// vazia devolve slice vazio (e a validação recusa). Cada item passa por bloco()
// para que uma chave desconhecida (`wher: …`) seja recusada em vez de
// silenciosamente ignorada.
func contadoresDoNo(n *yaml.Node) ([]rawContador, error) {
	for n != nil && n.Kind == yaml.AliasNode {
		n = n.Alias
	}
	if ausente(n) {
		return nil, nil
	}
	if n.Kind != yaml.SequenceNode {
		return nil, specErrf("counters deve ser uma lista de contadores")
	}
	out := make([]rawContador, 0, len(n.Content))
	for i, item := range n.Content {
		caminho := fmt.Sprintf("counters[%d]", i)
		chaves, err := bloco(item, caminho, "name", "groupBy", "where", "maxGroups", "retainEmptyGroups")
		if err != nil {
			return nil, err
		}
		var r rawContador
		for chave, valor := range chaves {
			var err error
			switch chave {
			case "name":
				err = decodeEm(valor, caminho+".name", &r.Name)
			case "groupBy":
				r.temGroupBy = true
				err = decodeEm(valor, caminho+".groupBy", &r.GroupBy)
			case "where":
				var w string
				err = decodeEm(valor, caminho+".where", &w)
				r.Where = &w
			case "maxGroups":
				var m int64
				err = decodeEm(valor, caminho+".maxGroups", &m)
				r.MaxGroups = &m
			case "retainEmptyGroups":
				err = decodeEm(valor, caminho+".retainEmptyGroups", &r.RetainEmptyGroups)
			}
			if err != nil {
				return nil, err
			}
		}
		out = append(out, r)
	}
	return out, nil
}

// validaContadores confere a lista inteira e devolve a forma validada.
//
// fields são os campos declarados; as colunas extraídas entram pelo nome que
// nomeDaExtracao lhes dá. key e seq são os papéis que o groupBy não pode
// usar: agrupar pela key é um grupo por entidade, e pela sequence é agrupar
// pela ordem do evento — os dois são cardinalidade sem teto por engano.
func validaContadores(raws []rawContador, fields map[string]FieldSpec, key, seq string) ([]ContadorSpec, error) {
	if len(raws) == 0 {
		return nil, specErrf("counters: lista vazia — remova a chave ou declare um contador")
	}
	colunas, err := colunasDeclaradas(fields)
	if err != nil {
		return nil, err
	}
	vistos := make(map[string]int, len(raws))
	out := make([]ContadorSpec, 0, len(raws))
	for i, r := range raws {
		caminho := fmt.Sprintf("counters[%d]", i)
		if r.Name == "" {
			return nil, specErrf("%s.name obrigatório", caminho)
		}
		if !isIdentName(r.Name) {
			return nil, specErrf("%s.name %q inválido (esperado [a-z0-9_]+)", caminho, r.Name)
		}
		if j, dup := vistos[r.Name]; dup {
			return nil, specErrf("%s.name %q repetido (já declarado em counters[%d])", caminho, r.Name, j)
		}
		vistos[r.Name] = i
		if !r.temGroupBy || len(r.GroupBy) == 0 {
			return nil, specErrf("%s.groupBy obrigatório (lista de colunas de agrupamento)", caminho)
		}
		vistas := make(map[string]bool, len(r.GroupBy))
		for _, col := range r.GroupBy {
			if vistas[col] {
				return nil, specErrf("%s.groupBy: coluna %q repetida", caminho, col)
			}
			vistas[col] = true
			if err := validaColunaDeAgrupamento(caminho, col, colunas, key, seq); err != nil {
				return nil, err
			}
		}
		c := ContadorSpec{Name: r.Name, GroupBy: r.GroupBy, MaxGroups: MaxGroupsPadrao, RetainEmptyGroups: RetainEmptyGroupsPadrao}
		if r.Where != nil {
			w := strings.Join(strings.Fields(*r.Where), " ")
			if w == "" {
				return nil, specErrf("%s.where vazio — omita a chave para contar toda linha com grupo", caminho)
			}
			c.Where = w
		}
		if r.MaxGroups != nil {
			if *r.MaxGroups < 1 || *r.MaxGroups > MaxGroupsTetoFisico {
				return nil, specErrf("%s.maxGroups %d fora de [1, %d]", caminho, *r.MaxGroups, MaxGroupsTetoFisico)
			}
			c.MaxGroups = uint32(*r.MaxGroups)
		}
		if r.RetainEmptyGroups != "" {
			d, err := duracaoDeVisibilidade(caminho, r.RetainEmptyGroups)
			if err != nil {
				return nil, err
			}
			c.RetainEmptyGroups = d
		}
		out = append(out, c)
	}
	return out, nil
}

// colunasDeclaradas devolve nome → tipo declarado de toda coluna que o groupBy
// pode referenciar: os campos de topo e as colunas extraídas. A congelada entra
// sem tipo (ela o herda na compilação) e por isso é recusada como "sem tipo
// declarado" — agrupar por uma coluna cujo tipo só se conhece depois seria
// decidir a chave do grupo tarde demais.
func colunasDeclaradas(fields map[string]FieldSpec) (map[string]string, error) {
	out := make(map[string]string, len(fields))
	nomes := make([]string, 0, len(fields))
	for nome := range fields {
		nomes = append(nomes, nome)
	}
	sort.Strings(nomes)
	for _, nome := range nomes {
		f := fields[nome]
		out[nome] = f.Type
		for i, e := range f.Extract {
			coluna, err := nomeDaExtracao(nome, i, e)
			if err != nil {
				return nil, err
			}
			out[coluna] = e.Type
		}
	}
	return out, nil
}

func validaColunaDeAgrupamento(caminho, col string, colunas map[string]string, key, seq string) error {
	switch col {
	case ColunaChaveDoContador, ColunaTotalDoContador:
		return specErrf("%s.groupBy: coluna %q colide com a coluna %s da tabela %s.<name>; renomeie a coluna no schema",
			caminho, col, col, NamespaceContadores)
	case key:
		return specErrf("%s.groupBy: coluna %q é a key do dataset — agrupar pela key é um grupo por entidade", caminho, col)
	case seq:
		if seq != "" {
			return specErrf("%s.groupBy: coluna %q é a sequence do dataset — a ordem do evento não é um grupo", caminho, col)
		}
	}
	tipo, ok := colunas[col]
	if !ok {
		return specErrf("%s.groupBy: coluna %q não declarada em schema.fields (nem como extração)", caminho, col)
	}
	if tipo == "" || tipo == "auto" {
		return specErrf("%s.groupBy: coluna %q precisa de tipo declarado (é auto ou herdado) — a chave do grupo é decidida pelo tipo",
			caminho, col)
	}
	if !tiposDeAgrupamento[tipo] {
		return specErrf("%s.groupBy: coluna %q tem tipo %q; groupBy aceita string, int64 e boolean", caminho, col, tipo)
	}
	return nil
}

// duracaoDeVisibilidade lê retainEmptyGroups com as mesmas regras do
// expireAfterWrite: `never` é zero, zero explícito é recusado apontando para
// `never`, e abaixo da resolução do coletor é recusado.
func duracaoDeVisibilidade(caminho, bruto string) (time.Duration, error) {
	if bruto == DuracaoNunca {
		return 0, nil
	}
	if ehDuracaoZero(bruto) {
		return 0, specErrf("%s.retainEmptyGroups %q não declara prazo — use %q para manter o grupo vazio para sempre",
			caminho, bruto, DuracaoNunca)
	}
	d, err := units.DuracaoComDias(bruto, 0)
	if err != nil {
		return 0, specErrf("%s.retainEmptyGroups: %s", caminho, semPrefixoDeSpec(err))
	}
	if d < ResolucaoDaExpiracao {
		return 0, specErrf("%s.retainEmptyGroups %s é menor que a resolução do coletor (%s) — use %q para não aposentar",
			caminho, bruto, ResolucaoDaExpiracao, DuracaoNunca)
	}
	return d, nil
}
