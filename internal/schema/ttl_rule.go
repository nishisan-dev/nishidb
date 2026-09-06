package schema

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/nishisan-dev/nishidb/internal/units"
)

// DuracaoNunca é o literal que declara imortalidade. `0` numérico é recusado:
// `never` diz a intenção, `0` parece um esquecimento.
const DuracaoNunca = "never"

// CampoDePrazo carrega a DURAÇÃO de ociosidade que vale para a entidade, em
// millis. Duração, e não prazo absoluto, para que se componha com o
// CampoInstanteDeEscrita e para que reordenar as regras no YAML não mude o
// significado do que já está gravado.
const CampoDePrazo = "_expiresIn"

// RegraTTLSpec é uma regra de expiração como escrita no YAML.
//
// A lista substituiu o objeto único, e a forma `{field, type}` — deadline
// absoluto vindo do payload — foi REMOVIDA: ela era parseada e validada sem
// nenhum consumidor, e manter configuração que não faz nada ao lado de um
// mecanismo que funciona é o defeito que este projeto já trata como tal.
type RegraTTLSpec struct {
	// When é a condição, com a mesma gramática do WHERE. Vazio marca a regra
	// de FUNDO: no máximo uma, obrigatoriamente a última.
	When string `yaml:"when"`
	// ExpireAfterWrite é o idle timeout desta regra, ou o literal "never".
	ExpireAfterWrite string `yaml:"expireAfterWrite"`
}

// RegraTTL é uma regra validada. Duracao zero significa IMORTAL — a regra
// casou e decidiu que a entidade não expira —, e é distinto de "nenhuma regra
// casou", que o chamador representa não achando regra nenhuma.
type RegraTTL struct {
	When    string
	Duracao time.Duration
}

// validaRegrasTTL confere a lista inteira e devolve a forma validada. A
// validação do `when` NÃO cabe aqui: ela precisa do schema compilado, que só
// existe depois do dicionário.
func validaRegrasTTL(regras []RegraTTLSpec) ([]RegraTTL, error) {
	if regras == nil {
		return nil, nil
	}
	if len(regras) == 0 {
		return nil, specErrf("materialization.ttl: lista vazia — remova a chave ou declare uma regra")
	}
	out := make([]RegraTTL, 0, len(regras))
	for i, r := range regras {
		if r.When == "" && i != len(regras)-1 {
			// A regra de fundo casa com tudo. Colocá-la antes de uma
			// condicional tornaria a condicional inalcançável, e a ordem é
			// semântica aqui: primeira que casa vence.
			return nil, specErrf("materialization.ttl[%d]: regra sem `when` só pode ser a última", i)
		}
		if r.ExpireAfterWrite == "" {
			return nil, specErrf("materialization.ttl[%d]: expireAfterWrite é obrigatório", i)
		}
		var d time.Duration
		if r.ExpireAfterWrite != DuracaoNunca {
			// O zero explícito tem mensagem PRÓPRIA: ele é o esquecimento
			// típico, e um "duração deve ser positiva" seco deixaria o
			// operador sem saber que existe forma de declarar imortalidade.
			if ehDuracaoZero(r.ExpireAfterWrite) {
				return nil, specErrf(
					"materialization.ttl[%d]: expireAfterWrite %q não declara prazo — "+
						"use %q para dizer que a entidade não expira",
					i, r.ExpireAfterWrite, DuracaoNunca)
			}
			var err error
			if d, err = units.DurationWithDays(r.ExpireAfterWrite, 0); err != nil {
				return nil, specErrf("materialization.ttl[%d]: expireAfterWrite: %s", i, semPrefixoDeSpec(err))
			}
			if d < ResolucaoDaExpiracao {
				return nil, specErrf(
					"materialization.ttl[%d]: expireAfterWrite %s é menor que a resolução do coletor (%s) — "+
						"use %q para declarar que a entidade não expira",
					i, r.ExpireAfterWrite, ResolucaoDaExpiracao, DuracaoNunca)
			}
		}
		out = append(out, RegraTTL{When: r.When, Duracao: d})
	}
	return out, nil
}

// ehDuracaoZero reconhece o zero em qualquer das duas grafias que o YAML
// aceita — o inteiro cru e a duração com unidade.
func ehDuracaoZero(s string) bool {
	s = strings.TrimSpace(s)
	if n, err := strconv.Atoi(s); err == nil {
		return n == 0
	}
	d, err := time.ParseDuration(s)
	return err == nil && d == 0
}

// duracaoDeFundo devolve a duração da regra SEM `when`, ou zero quando não há
// regra de fundo. É a duração que vale para a linha materializada ANTES de as
// regras existirem, e por isso viaja separada até as Options do storage.
func duracaoDeFundo(regras []RegraTTL) time.Duration {
	if n := len(regras); n > 0 && regras[n-1].When == "" {
		return regras[n-1].Duracao
	}
	return 0
}

// regrasDoNo decodifica o bloco `ttl` do YAML.
//
// A leitura é pelo nó, e não por decodificação direta em []RegraTTLSpec, para
// que a forma ANTIGA — o mapa único, nas duas variantes que existiram — seja
// recusada com uma mensagem que nomeia a lista. Sem isto o operador receberia
// a mensagem do yaml.v3 sobre tipos que ele nunca escreveu, num arquivo que
// funcionava na versão anterior.
func regrasDoNo(n *yaml.Node) ([]RegraTTLSpec, error) {
	for n != nil && n.Kind == yaml.AliasNode {
		n = n.Alias
	}
	if ausente(n) {
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
	regras := make([]RegraTTLSpec, 0, len(n.Content))
	for i, item := range n.Content {
		chaves, err := bloco(item, fmt.Sprintf("materialization.ttl[%d]", i), "when", "expireAfterWrite")
		if err != nil {
			return nil, err
		}
		var r RegraTTLSpec
		for chave, valor := range chaves {
			alvo := &r.When
			if chave == "expireAfterWrite" {
				alvo = &r.ExpireAfterWrite
			}
			if err := decodeEm(valor, fmt.Sprintf("materialization.ttl[%d].%s", i, chave), alvo); err != nil {
				return nil, err
			}
		}
		regras = append(regras, r)
	}
	return regras, nil
}

// DescreveRegras rende a lista num texto de UMA linha, para o log de subida.
//
// A ordem é preservada porque ela é semântica: a primeira regra que casa
// vence, e é justamente isso que o operador precisa conferir contra o YAML que
// escreveu. `never` aparece como texto, e não como "0s", pela mesma razão que
// ele existe: zero e imortal não são a mesma coisa.
func DescreveRegras(regras []RegraTTL) string {
	var sb strings.Builder
	for i, r := range regras {
		if i > 0 {
			sb.WriteString("; ")
		}
		fmt.Fprintf(&sb, "[%d] ", i)
		if r.When == "" {
			sb.WriteString("fundo ")
		} else {
			fmt.Fprintf(&sb, "when=%q ", r.When)
		}
		if r.Duracao == 0 {
			sb.WriteString(DuracaoNunca)
			continue
		}
		sb.WriteString(r.Duracao.String())
	}
	return sb.String()
}
