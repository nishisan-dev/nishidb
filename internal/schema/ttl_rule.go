package schema

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/nishisan-dev/nishidb/internal/units"
)

// DurationNever é o literal que declara imortalidade. `0` numérico é recusado:
// `never` diz a intenção, `0` parece um esquecimento.
const DurationNever = "never"

// TTLFieldName carrega a DURAÇÃO de ociosidade que vale para a entidade, em
// millis. Duração, e não prazo absoluto, para que se componha com o
// WriteInstantFieldName e para que reordenar as regras no YAML não mude o
// significado do que já está gravado.
const TTLFieldName = "_expiresIn"

// TTLRuleSpec é uma regra de expiração como escrita no YAML.
//
// A lista substituiu o objeto único, e a forma `{field, type}` — deadline
// absoluto vindo do payload — foi REMOVIDA: ela era parseada e validada sem
// nenhum consumidor, e manter configuração que não faz nada ao lado de um
// mecanismo que funciona é o defeito que este projeto já trata como tal.
type TTLRuleSpec struct {
	// When é a condição, com a mesma gramática do WHERE. Vazio marca a regra
	// de FUNDO: no máximo uma, obrigatoriamente a última.
	When string `yaml:"when"`
	// ExpireAfterWrite é o idle timeout desta regra, ou o literal "never".
	ExpireAfterWrite string `yaml:"expireAfterWrite"`
}

// TTLRule é uma regra validada. Duration zero significa IMORTAL — a regra
// casou e decidiu que a entidade não expira —, e é distinto de "nenhuma regra
// casou", que o chamador representa não achando regra nenhuma.
type TTLRule struct {
	When     string
	Duration time.Duration
}

// validateTTLRules confere a lista inteira e devolve a forma validada. A
// validação do `when` NÃO cabe aqui: ela precisa do schema compilado, que só
// existe depois do dicionário.
func validateTTLRules(rules []TTLRuleSpec) ([]TTLRule, error) {
	if rules == nil {
		return nil, nil
	}
	if len(rules) == 0 {
		return nil, specErrf("materialization.ttl: lista vazia — remova a chave ou declare uma regra")
	}
	out := make([]TTLRule, 0, len(rules))
	for i, r := range rules {
		if r.When == "" && i != len(rules)-1 {
			// A regra de fundo casa com tudo. Colocá-la antes de uma
			// condicional tornaria a condicional inalcançável, e a ordem é
			// semântica aqui: primeira que casa vence.
			return nil, specErrf("materialization.ttl[%d]: regra sem `when` só pode ser a última", i)
		}
		if r.ExpireAfterWrite == "" {
			return nil, specErrf("materialization.ttl[%d]: expireAfterWrite é obrigatório", i)
		}
		var d time.Duration
		if r.ExpireAfterWrite != DurationNever {
			// O zero explícito tem mensagem PRÓPRIA: ele é o esquecimento
			// típico, e um "duração deve ser positiva" seco deixaria o
			// operador sem saber que existe forma de declarar imortalidade.
			if isZeroDuration(r.ExpireAfterWrite) {
				return nil, specErrf(
					"materialization.ttl[%d]: expireAfterWrite %q não declara prazo — "+
						"use %q para dizer que a entidade não expira",
					i, r.ExpireAfterWrite, DurationNever)
			}
			var err error
			if d, err = units.DurationWithDays(r.ExpireAfterWrite, 0); err != nil {
				return nil, specErrf("materialization.ttl[%d]: expireAfterWrite: %s", i, withoutSpecPrefix(err))
			}
			if d < ExpiryResolution {
				return nil, specErrf(
					"materialization.ttl[%d]: expireAfterWrite %s é menor que a resolução do coletor (%s) — "+
						"use %q para declarar que a entidade não expira",
					i, r.ExpireAfterWrite, ExpiryResolution, DurationNever)
			}
		}
		out = append(out, TTLRule{When: r.When, Duration: d})
	}
	return out, nil
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

// fallbackDuration devolve a duração da regra SEM `when`, ou zero quando não há
// regra de fundo. É a duração que vale para a linha materializada ANTES de as
// regras existirem, e por isso viaja separada até as Options do storage.
func fallbackDuration(rules []TTLRule) time.Duration {
	if n := len(rules); n > 0 && rules[n-1].When == "" {
		return rules[n-1].Duration
	}
	return 0
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
		return nil, specErrf("materialization.ttl agora é uma lista de regras, não um bloco único:\n" +
			"  ttl:\n" +
			"    - when: \"severity = 2\"   # condição com a gramática do WHERE\n" +
			"      expireAfterWrite: 3m\n" +
			"    - expireAfterWrite: 7d     # regra de fundo, sempre a última")
	}
	if n.Kind != yaml.SequenceNode {
		return nil, specErrf("materialization.ttl deve ser uma lista de regras")
	}
	rules := make([]TTLRuleSpec, 0, len(n.Content))
	for i, item := range n.Content {
		keys, err := block(item, fmt.Sprintf("materialization.ttl[%d]", i), "when", "expireAfterWrite")
		if err != nil {
			return nil, err
		}
		var r TTLRuleSpec
		for key, value := range keys {
			target := &r.When
			if key == "expireAfterWrite" {
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
// ele existe: zero e imortal não são a mesma coisa.
func DescribeRules(rules []TTLRule) string {
	var sb strings.Builder
	for i, r := range rules {
		if i > 0 {
			sb.WriteString("; ")
		}
		fmt.Fprintf(&sb, "[%d] ", i)
		if r.When == "" {
			sb.WriteString("fundo ")
		} else {
			fmt.Fprintf(&sb, "when=%q ", r.When)
		}
		if r.Duration == 0 {
			sb.WriteString(DurationNever)
			continue
		}
		sb.WriteString(r.Duration.String())
	}
	return sb.String()
}
