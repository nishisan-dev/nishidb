package schema

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/nishisan-dev/nishidb/internal/types"
	"github.com/nishisan-dev/nishidb/internal/units"
)

// DurationNever é o literal que declara imortalidade. `0` numérico é recusado:
// `never` diz a intenção, `0` parece um esquecimento.
const DurationNever = "never"

// TTLFieldName carrega o PRAZO da entidade contado a partir de
// WriteInstantFieldName, em millis: a entidade vence em
// `_writtenAt + _expiresIn`.
//
// Numa regra sem `anchor`, o prazo é a própria duração declarada — o idle
// timeout, renovado a cada escrita. Numa regra com `anchor`, o materializer o
// resolve sobre o record como `min(âncora, _writtenAt) + expireAfter −
// _writtenAt`, e por isso uma escrita que não muda a âncora não o renova.
//
// Prazo relativo à escrita, e não instante absoluto, para que se componha com
// o WriteInstantFieldName sem o storage conhecer campo de negócio: ele soma os
// dois e nunca lê a âncora. Os dois vêm do PRÓPRIO record, então o replay
// reconstrói o mesmo vencimento.
const TTLFieldName = "_expiresIn"

// TTLRuleSpec é uma regra de expiração como escrita no YAML.
//
// A lista substituiu o objeto único, e a forma `{field, type}` — deadline
// absoluto vindo do payload — foi REMOVIDA: ela era parseada e validada sem
// nenhum consumidor, e manter configuração que não faz nada ao lado de um
// mecanismo que funciona é o defeito que este projeto já trata como tal.
type TTLRuleSpec struct {
	// When é a condição, com a mesma gramática do WHERE.
	When string `yaml:"when"`
	// Anchor é o campo temporal do record a partir do qual o prazo é contado.
	// Exige ExpireAfter e exclui ExpireAfterWrite.
	Anchor string `yaml:"anchor"`
	// ExpireAfter é o prazo contado a partir da âncora.
	ExpireAfter string `yaml:"expireAfter"`
	// ExpireAfterWrite é o idle timeout desta regra, ou o literal "never".
	ExpireAfterWrite string `yaml:"expireAfterWrite"`
}

// TTLRule é uma regra validada.
//
// Sem Anchor, Duration é o idle timeout, e zero significa IMORTAL — a regra
// casou e decidiu que a entidade não expira —, distinto de "nenhuma regra
// casou", que o chamador representa não achando regra nenhuma. Com Anchor,
// Duration é o `expireAfter`, sempre positivo: âncora com `never` é recusada.
type TTLRule struct {
	When     string
	Anchor   string
	Duration time.Duration
}

// IsFallback diz se a regra é a de FUNDO: sem `when` E sem `anchor`, ela casa
// com toda escrita. É o único predicado de fundo do pacote — a validação da
// posição, a duração de fundo, a compilação e a descrição do log usam este, e
// não "sem when": uma regra só com `anchor` não casa quando a âncora está
// ausente, e tratá-la como fundo daria o prazo dela às linhas que não têm o
// campo.
func (r TTLRule) IsFallback() bool { return r.When == "" && r.Anchor == "" }

// HasFallback diz se a lista termina numa regra de fundo — inclusive uma de
// fundo `never`, cuja duração é zero e que por isso não pode ser inferida da
// FallbackDuration.
func HasFallback(rules []TTLRule) bool {
	n := len(rules)
	return n > 0 && rules[n-1].IsFallback()
}

// validateTTLRules confere a lista inteira e devolve a forma validada. A
// validação do `when` e do campo da `anchor` NÃO cabe aqui: as duas precisam
// do schema compilado, que só existe depois do dicionário.
func validateTTLRules(rules []TTLRuleSpec) ([]TTLRule, error) {
	if rules == nil {
		return nil, nil
	}
	if len(rules) == 0 {
		return nil, specErrf("materialization.ttl: empty list — remove the key or declare a rule")
	}
	out := make([]TTLRule, 0, len(rules))
	for i, r := range rules {
		key := fmt.Sprintf("materialization.ttl[%d]", i)
		anchored := r.Anchor != ""
		switch {
		case anchored && r.ExpireAfterWrite != "":
			return nil, specErrf("%s: anchor takes expireAfter, counted from the anchor field; "+
				"expireAfterWrite counts from the write and cannot be combined with anchor", key)
		case !anchored && r.ExpireAfter != "":
			return nil, specErrf("%s: expireAfter is counted from an anchor field and requires anchor; "+
				"without one, use expireAfterWrite", key)
		case anchored && r.ExpireAfter == "":
			return nil, specErrf("%s: anchor requires expireAfter", key)
		case !anchored && r.ExpireAfterWrite == "":
			return nil, specErrf("%s: expireAfterWrite (or anchor with expireAfter) is required", key)
		}
		rule := TTLRule{When: r.When, Anchor: r.Anchor}
		if rule.IsFallback() && i != len(rules)-1 {
			// A regra de fundo casa com tudo. Colocá-la antes de outra
			// tornaria a seguinte inalcançável, e a ordem é semântica aqui:
			// primeira que casa vence.
			return nil, specErrf("%s: a fallback rule (no when and no anchor) must be the last one", key)
		}
		var err error
		if anchored {
			if r.ExpireAfter == DurationNever {
				// Imortal não tem prazo a contar de lugar nenhum: a âncora
				// seria lida, validada e ignorada. O operador que quer
				// imortalidade condicionada à presença do campo escreve
				// `when: "<campo> IS NOT NULL"` com expireAfterWrite: never.
				return nil, specErrf("%s: anchor with expireAfter %q is refused — an immortal rule makes the "+
					"anchor irrelevant; use when: \"%s IS NOT NULL\" with expireAfterWrite: %s",
					key, DurationNever, r.Anchor, DurationNever)
			}
			rule.Duration, err = ruleDuration(key, "expireAfter", r.ExpireAfter)
		} else {
			rule.Duration, err = ruleDuration(key, "expireAfterWrite", r.ExpireAfterWrite)
		}
		if err != nil {
			return nil, err
		}
		out = append(out, rule)
	}
	return out, nil
}

// ruleDuration lê a duração de uma regra, com `never` como zero. O piso é o
// mesmo nas duas chaves: o coletor tem um passo, e um prazo abaixo dele
// mentiria.
func ruleDuration(key, name, raw string) (time.Duration, error) {
	if raw == DurationNever {
		return 0, nil
	}
	// O zero explícito tem mensagem PRÓPRIA: ele é o esquecimento típico, e
	// um "duração deve ser positiva" seco deixaria o operador sem saber que
	// existe forma de declarar imortalidade.
	if isZeroDuration(raw) {
		return 0, specErrf("%s: %s %q declares no deadline — use %q to declare that the entity does not expire",
			key, name, raw, DurationNever)
	}
	d, err := units.DurationWithDays(raw, 0)
	if err != nil {
		return 0, specErrf("%s: %s: %s", key, name, withoutSpecPrefix(err))
	}
	if d < ExpiryResolution {
		return 0, specErrf("%s: %s %s is below the collector resolution (%s) — use %q to declare that the entity does not expire",
			key, name, raw, ExpiryResolution, DurationNever)
	}
	return d, nil
}

// isZeroDuration reconhece o zero em qualquer das duas grafias que o YAML
// aceita — o inteiro cru e a duração com unidade.
func isZeroDuration(s string) bool {
	s = strings.TrimSpace(s)
	if n, err := strconv.Atoi(s); err == nil {
		return n == 0
	}
	d, err := time.ParseDuration(s)
	return err == nil && d == 0
}

// fallbackDuration devolve a duração da regra de FUNDO, ou zero quando não há.
// É a duração que vale para a linha materializada ANTES de as regras
// existirem, e por isso viaja separada até as Options do storage.
//
// Só a regra de fundo conta: uma regra com `anchor` na última posição NÃO é
// fundo, e dar o prazo dela à linha legada — que nem tem o campo âncora
// avaliado — seria inventar um vencimento que regra nenhuma decidiu.
func fallbackDuration(rules []TTLRule) time.Duration {
	if HasFallback(rules) {
		return rules[len(rules)-1].Duration
	}
	return 0
}

// validateTTLAnchors confere, contra o schema compilado, o campo de cada
// regra com `anchor`: ele precisa existir no dataset, ser declarado (não
// `auto`), não ser de sistema e ter tipo temporal — timestamp ou datetime,
// que o storage guarda em epoch millis UTC, a mesma escala do `_writtenAt`.
//
// Roda no Compile, e não no parse do YAML, porque o tipo de uma congelada é o
// da origem e o de `frozenFrom: "@writeTime"` é timestamp: os dois só existem
// depois da religação das congeladas. Coluna derivada (`extract:`) é aceita
// com tipo declarado temporal — ela é coluna do record como as outras, e o
// prazo é decidido sobre o record.
//
// `auto` é recusado pela DECLARAÇÃO, nunca pelo tipo já observado: o tipo de
// um auto é fixado pelo primeiro evento, e uma regra que dependesse dele
// mudaria de validade conforme o dado que chegou primeiro.
func validateTTLAnchors(c *Compiled) error {
	for i, r := range c.TTLRules {
		if r.Anchor == "" {
			continue
		}
		key := fmt.Sprintf("materialization.ttl[%d].anchor", i)
		// Os campos de sistema estão em byName sempre que há regra (o Compile
		// os registra junto com elas), então o IsSystem abaixo os pega.
		cf, ok := c.byName[r.Anchor]
		if !ok {
			if _, excluded := c.exclude[r.Anchor]; excluded {
				return specErrf("%s %q is excluded from the dataset", key, r.Anchor)
			}
			return specErrf("%s %q is not a field of the dataset", key, r.Anchor)
		}
		switch {
		case cf.IsSystem:
			return specErrf("%s %q is a system field; to count from the write, use expireAfterWrite", key, r.Anchor)
		case cf.IsAuto:
			return specErrf("%s %q is auto-typed; an anchor must be declared with type timestamp or datetime",
				key, r.Anchor)
		case cf.Type != types.TypeTimestamp && cf.Type != types.TypeDateTime:
			return specErrf("%s %q has type %s; an anchor must be timestamp or datetime", key, r.Anchor, cf.Type)
		}
	}
	return nil
}

// rulesFromNode decodifica o bloco `ttl` do YAML.
//
// A leitura é pelo nó, e não por decodificação direta em []TTLRuleSpec, para
// que a forma ANTIGA — o mapa único, nas duas variantes que existiram — seja
// recusada com uma mensagem que nomeia a lista. Sem isto o operador receberia
// a mensagem do yaml.v3 sobre tipos que ele nunca escreveu, num arquivo que
// funcionava na versão anterior.
func rulesFromNode(n *yaml.Node) ([]TTLRuleSpec, error) {
	for n != nil && n.Kind == yaml.AliasNode {
		n = n.Alias
	}
	if absent(n) {
		return nil, nil
	}
	if n.Kind == yaml.MappingNode {
		return nil, specErrf("materialization.ttl is now a list of rules, not a single block:\n" +
			"  ttl:\n" +
			"    - when: \"severity = 2\"   # condition with the WHERE grammar\n" +
			"      expireAfterWrite: 3m\n" +
			"    - anchor: lastSeenAt      # deadline counted from a timestamp field\n" +
			"      expireAfter: 48h\n" +
			"    - expireAfterWrite: 7d     # fallback rule, always the last one")
	}
	if n.Kind != yaml.SequenceNode {
		return nil, specErrf("materialization.ttl must be a list of rules")
	}
	rules := make([]TTLRuleSpec, 0, len(n.Content))
	for i, item := range n.Content {
		keys, err := block(item, fmt.Sprintf("materialization.ttl[%d]", i),
			"when", "anchor", "expireAfter", "expireAfterWrite")
		if err != nil {
			return nil, err
		}
		var r TTLRuleSpec
		for key, value := range keys {
			var target *string
			switch key {
			case "when":
				target = &r.When
			case "anchor":
				target = &r.Anchor
			case "expireAfter":
				target = &r.ExpireAfter
			default:
				target = &r.ExpireAfterWrite
			}
			if err := decodeInto(value, fmt.Sprintf("materialization.ttl[%d].%s", i, key), target); err != nil {
				return nil, err
			}
		}
		rules = append(rules, r)
	}
	return rules, nil
}

// DescribeRules rende a lista num texto de UMA linha, para o log de subida.
//
// A ordem é preservada porque ela é semântica: a primeira regra que casa
// vence, e é justamente isso que o operador precisa conferir contra o YAML que
// escreveu. `never` aparece como texto, e não como "0s", pela mesma razão que
// ele existe: zero e imortal não são a mesma coisa. A regra ancorada nomeia a
// âncora e a chave da duração, porque a mesma duração significa outra coisa
// contada da âncora.
func DescribeRules(rules []TTLRule) string {
	var sb strings.Builder
	for i, r := range rules {
		if i > 0 {
			sb.WriteString("; ")
		}
		fmt.Fprintf(&sb, "[%d] ", i)
		if r.IsFallback() {
			sb.WriteString("fallback ")
		}
		if r.When != "" {
			fmt.Fprintf(&sb, "when=%q ", r.When)
		}
		if r.Anchor != "" {
			fmt.Fprintf(&sb, "anchor=%s expireAfter=%s", r.Anchor, r.Duration)
			continue
		}
		if r.Duration == 0 {
			sb.WriteString(DurationNever)
			continue
		}
		sb.WriteString(r.Duration.String())
	}
	return sb.String()
}
