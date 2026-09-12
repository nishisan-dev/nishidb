package schema

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/nishisan-dev/nishidb/internal/units"
)

// Os alarmes de um contador são declarados no bloco `alarms:` do próprio
// contador:
//
//	counters:
//	  - name: autenticados_por_olt
//	    groupBy: [olt]
//	    where: authenticated = true
//	    alarms:
//	      - name: olt_down
//	        when: "total < 100"        # condição sobre as colunas da tabela
//	        sustainedFor: 2m           # por quanto tempo ela tem de valer
//	        enrich:                    # opcional
//	          - name: servicos_da_olt   # identifica o bloco no evento
//	            sql: "SELECT customerId FROM servicos WHERE olt = ? LIMIT 5"
//	            bind: [olt]
//
// O `when` fala a MESMA linguagem do WHERE de uma consulta e do `when:` de uma
// regra de TTL, e roda pelo mesmo avaliador (internal/predicate) sobre as
// colunas da tabela `_counters.<name>`: `groupKey`, as colunas do groupBy e
// `total`. Como o `where` do contador, ele NÃO é validado aqui — precisa do
// schema compilado, e quem o compila é internal/alarm.
//
// `sustainedFor` é o que separa alarme de rajada: a condição tem de valer por
// esse prazo antes de o evento sair.
//
// O `enrich` acompanha o evento com o resultado de consultas parametrizadas
// pelas colunas do grupo. Ele NUNCA decide se o alarme sai — uma consulta que
// falha produz evento sem aquele enriquecimento, nunca alarme engolido.

// MinSustainedFor é o menor prazo de sustentação declarável.
//
// O piso existe pelo mesmo motivo do MinWatchdogTimeout (internal/schema/
// source.go): a cadência da varredura é derivada do prazo, e abaixo de dez
// segundos ela viraria varredura contínua sobre milhares de grupos para medir
// uma oscilação que nenhum operador trataria como queda. Não há grafia para
// "dispare imediatamente": um alarme sem sustentação é a consulta que a
// aplicação já sabe fazer.
const MinSustainedFor = 10 * time.Second

// AlarmSpec é um alarme validado de um contador.
type AlarmSpec struct {
	Name string
	// When é a condição normalizada (espaços colapsados) sobre as colunas da
	// tabela do contador. Nunca vazia.
	When string
	// SustainedFor é quanto tempo o When precisa valer antes de o alarme
	// disparar. Sempre >= MinSustainedFor.
	SustainedFor time.Duration
	// Enrich são as consultas que acompanham o evento, na ordem declarada; nil
	// quando o bloco não veio.
	Enrich []EnrichSpec
}

// EnrichSpec é uma consulta de enriquecimento do evento de alarme.
type EnrichSpec struct {
	// Name identifica o bloco de resultado dentro do evento.
	//
	// É obrigatório porque a alternativa é o consumidor casar a consulta pela
	// POSIÇÃO na lista — e a posição muda toda vez que alguém insere uma
	// consulta no meio, sem que nada no evento avise. Único dentro do alarme.
	Name string
	// SQL é a consulta como declarada, apenas com as pontas aparadas. NÃO é
	// normalizada: colapsar espaços mudaria o valor de uma string literal
	// dentro da própria consulta.
	SQL string
	// Bind são as colunas do contador ligadas aos `?` do SQL, na ordem; nil
	// quando a consulta não tem placeholder. A contagem contra os placeholders
	// de verdade é conferida pelo parser do SQL na compilação — contá-los aqui
	// com busca textual acertaria o `?` dentro de uma string literal.
	Bind []string
}

// rawAlarm é um item do bloco como lido do YAML, antes da validação.
type rawAlarm struct {
	Name         string
	When         *string
	SustainedFor string
	Enrich       []rawEnrich
}

type rawEnrich struct {
	Name string
	SQL  *string
	Bind []string
}

// alarmsFromNode decodifica o bloco `alarms` de um contador. Cada item passa
// por block() para que uma chave desconhecida (`sustainedForr:`) seja recusada
// em vez de silenciosamente ignorada.
func alarmsFromNode(n *yaml.Node, keyPath string) ([]rawAlarm, error) {
	for n != nil && n.Kind == yaml.AliasNode {
		n = n.Alias
	}
	if n == nil || n.Kind != yaml.SequenceNode {
		return nil, specErrf("%s deve ser uma lista de alarmes", keyPath)
	}
	out := make([]rawAlarm, 0, len(n.Content))
	for i, item := range n.Content {
		path := fmt.Sprintf("%s[%d]", keyPath, i)
		keys, err := block(item, path, "name", "when", "sustainedFor", "enrich")
		if err != nil {
			return nil, err
		}
		var r rawAlarm
		for key, value := range keys {
			var err error
			switch key {
			case "name":
				err = decodeInto(value, path+".name", &r.Name)
			case "when":
				var w string
				err = decodeInto(value, path+".when", &w)
				r.When = &w
			case "sustainedFor":
				err = decodeInto(value, path+".sustainedFor", &r.SustainedFor)
			case "enrich":
				r.Enrich, err = enrichFromNode(value, path+".enrich")
			}
			if err != nil {
				return nil, err
			}
		}
		out = append(out, r)
	}
	return out, nil
}

// enrichFromNode decodifica o bloco `enrich` de um alarme.
func enrichFromNode(n *yaml.Node, keyPath string) ([]rawEnrich, error) {
	for n != nil && n.Kind == yaml.AliasNode {
		n = n.Alias
	}
	if n == nil || n.Kind != yaml.SequenceNode {
		return nil, specErrf("%s deve ser uma lista de consultas", keyPath)
	}
	out := make([]rawEnrich, 0, len(n.Content))
	for i, item := range n.Content {
		path := fmt.Sprintf("%s[%d]", keyPath, i)
		keys, err := block(item, path, "name", "sql", "bind")
		if err != nil {
			return nil, err
		}
		var r rawEnrich
		for key, value := range keys {
			var err error
			switch key {
			case "name":
				err = decodeInto(value, path+".name", &r.Name)
			case "sql":
				var s string
				err = decodeInto(value, path+".sql", &s)
				r.SQL = &s
			case "bind":
				err = decodeInto(value, path+".bind", &r.Bind)
			}
			if err != nil {
				return nil, err
			}
		}
		out = append(out, r)
	}
	return out, nil
}

// validateAlarms confere a lista de alarmes de UM contador. O nome só precisa
// ser único dentro do contador: o que identifica um alarme é o par
// (contador, alarme), e é ele que vai para o evento e para o rótulo da métrica.
func validateAlarms(raws []rawAlarm, keyPath string) ([]AlarmSpec, error) {
	if len(raws) == 0 {
		return nil, specErrf("%s: lista vazia — remova a chave ou declare um alarme", keyPath)
	}
	seenNames := make(map[string]int, len(raws))
	out := make([]AlarmSpec, 0, len(raws))
	for i, r := range raws {
		path := fmt.Sprintf("%s[%d]", keyPath, i)
		if r.Name == "" {
			return nil, specErrf("%s.name obrigatório", path)
		}
		if !isIdentName(r.Name) {
			return nil, specErrf("%s.name %q inválido (esperado [a-z0-9_]+)", path, r.Name)
		}
		if j, dup := seenNames[r.Name]; dup {
			return nil, specErrf("%s.name %q repetido (já declarado em %s[%d])", path, r.Name, keyPath, j)
		}
		seenNames[r.Name] = i
		a := AlarmSpec{Name: r.Name}
		if r.When == nil {
			return nil, specErrf("%s.when obrigatório (a condição que dispara o alarme)", path)
		}
		w := strings.Join(strings.Fields(*r.When), " ")
		if w == "" {
			return nil, specErrf("%s.when vazio — um alarme sem condição não tem o que observar", path)
		}
		a.When = w
		if r.SustainedFor == "" {
			return nil, specErrf(
				"%s.sustainedFor obrigatório — é o prazo que separa alarme de rajada", path)
		}
		d, err := sustainedDuration(path, r.SustainedFor)
		if err != nil {
			return nil, err
		}
		a.SustainedFor = d
		if r.Enrich != nil {
			if a.Enrich, err = validateEnrich(r.Enrich, path+".enrich"); err != nil {
				return nil, err
			}
		}
		out = append(out, a)
	}
	return out, nil
}

// sustainedDuration lê sustainedFor. `never` é recusado em vez de virar zero:
// ao contrário de retainEmptyGroups, onde "para sempre" é uma política válida,
// um alarme que nunca dispara não é um alarme.
func sustainedDuration(keyPath, raw string) (time.Duration, error) {
	if raw == DurationNever {
		return 0, specErrf("%s.sustainedFor %q não existe no alarme: um alarme que nunca dispara não alarma — "+
			"remova o alarme para não observar a condição", keyPath, DurationNever)
	}
	if isZeroDuration(raw) {
		return 0, specErrf("%s.sustainedFor %q não declara prazo — o mínimo é %s", keyPath, raw, MinSustainedFor)
	}
	d, err := units.DurationWithDays(raw, 0)
	if err != nil {
		return 0, specErrf("%s.sustainedFor: %s", keyPath, withoutSpecPrefix(err))
	}
	if d < MinSustainedFor {
		return 0, specErrf("%s.sustainedFor %s é menor que o mínimo de %s — "+
			"abaixo disso a varredura vira contínua para medir uma oscilação que não é queda",
			keyPath, raw, MinSustainedFor)
	}
	return d, nil
}

// validateEnrich confere as consultas de enriquecimento de UM alarme.
func validateEnrich(raws []rawEnrich, keyPath string) ([]EnrichSpec, error) {
	if len(raws) == 0 {
		return nil, specErrf("%s: lista vazia — remova a chave ou declare uma consulta", keyPath)
	}
	seenNames := make(map[string]int, len(raws))
	out := make([]EnrichSpec, 0, len(raws))
	for i, r := range raws {
		path := fmt.Sprintf("%s[%d]", keyPath, i)
		if r.Name == "" {
			return nil, specErrf("%s.name obrigatório — é ele que identifica o bloco dentro do evento", path)
		}
		if !isIdentName(r.Name) {
			return nil, specErrf("%s.name %q inválido (esperado [a-z0-9_]+)", path, r.Name)
		}
		if j, dup := seenNames[r.Name]; dup {
			return nil, specErrf("%s.name %q repetido (já declarado em %s[%d])", path, r.Name, keyPath, j)
		}
		seenNames[r.Name] = i
		if r.SQL == nil {
			return nil, specErrf("%s.sql obrigatório", path)
		}
		sql := strings.TrimSpace(*r.SQL)
		if sql == "" {
			return nil, specErrf("%s.sql vazio", path)
		}
		e := EnrichSpec{Name: r.Name, SQL: sql}
		if r.Bind != nil {
			seen := make(map[string]bool, len(r.Bind))
			for _, col := range r.Bind {
				if col == "" {
					return nil, specErrf("%s.bind: coluna vazia", path)
				}
				if seen[col] {
					return nil, specErrf("%s.bind: coluna %q repetida — cada `?` liga uma posição, "+
						"e a mesma coluna duas vezes é quase sempre um `?` a mais do que o pretendido", path, col)
				}
				seen[col] = true
			}
			e.Bind = r.Bind
		}
		out = append(out, e)
	}
	return out, nil
}
