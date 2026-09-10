package schema

import (
	"errors"
	"fmt"
)

// FrozenFromClock é a origem especial que entrega o instante da escrita em vez
// do valor de outra coluna. É o que faz um `createdAt` sair da mesma máquina
// que o `originalSeverity`, sem caso especial em lugar nenhum.
const FrozenFromClock = "@writeTime"

// ErrObsoleteIndexSpelling é devolvido quando o YAML declara `index: hash` ou
// `index: bitmap` — as duas grafias dos motores globais que a D5
// (planning/spec-subsistema-1-indice-por-segmento.md) aposentou.
//
// Sentinela PRÓPRIA, e não um specErrf genérico: o CLI precisa distinguir esta
// config recusada de qualquer outro erro de spec para sair com ExitDataset
// (3), e não com o exit 1 genérico de erro de uso — quem lê a mensagem está
// com o servidor fora do ar, e o código de saída é o primeiro sinal de que o
// problema está no dataset, não na linha de comando.
var ErrObsoleteIndexSpelling = errors.New("index: hash e index: bitmap foram substituídos por um índice único")

// validateFrozen confere as regras de frozenFrom que dependem de OUTROS
// campos, e por isso não cabem no laço por campo: cadeia, autorreferência e
// origem inexistente só são decidíveis com o mapa inteiro na mão.
func validateFrozen(fields map[string]FieldSpec, keyField, mode string) error {
	for name, f := range fields {
		if f.FrozenFrom == "" {
			continue
		}
		if mode == "replace" {
			// No replace o estado anterior é descartado inteiro e encodePatch
			// nem o lê. Uma coluna que sobrevivesse ao replace contradiria o
			// que replace significa, e sustentá-la custaria uma leitura de
			// record por evento no modo cuja razão de existir é não pagá-la.
			return specErrf("campo %q: frozenFrom não se aplica a mode: replace — "+
				"no replace o estado anterior é descartado inteiro", name)
		}
		if f.Type != "" {
			return specErrf("campo %q: frozenFrom e type juntos — o tipo é herdado da origem %q",
				name, f.FrozenFrom)
		}
		if f.Format != "" {
			return specErrf("campo %q: format não se aplica a frozenFrom — "+
				"o formato é do parsing da origem", name)
		}
		if f.FrozenFrom == FrozenFromClock {
			// O relógio entrega timestamp, e nenhum dos dois motores de índice
			// aceita esse tipo — a checagem sai aqui em vez de deixar o
			// `index:` decodificar em silêncio e não fazer nada.
			if err := validateIndex(name, f.Index, "timestamp"); err != nil {
				return err
			}
			continue
		}
		if f.FrozenFrom == name {
			return specErrf("campo %q: frozenFrom aponta para si mesmo", name)
		}
		if f.FrozenFrom == keyField {
			return specErrf("campo %q: frozenFrom não aceita a key %q — "+
				"a key identifica a entidade e já é constante", name, keyField)
		}
		source, exists := fields[f.FrozenFrom]
		if !exists {
			return specErrf("campo %q: frozenFrom nomeia %q, que não está declarado em schema.fields",
				name, f.FrozenFrom)
		}
		// A indexabilidade se decide pelo tipo da ORIGEM: é o valor dela que
		// vai para a posting list.
		if err := validateIndex(name, f.Index, source.Type); err != nil {
			return err
		}
		if source.FrozenFrom != "" {
			// A cadeia faria a ordem de avaliação virar contrato implícito, e
			// o valor dependeria de qual das duas foi capturada primeiro.
			return specErrf("campo %q: frozenFrom aponta para %q, que também é congelada — "+
				"cadeia de congelamento não é suportada", name, f.FrozenFrom)
		}
	}
	return nil
}

// validateIndex confere a grafia de `index:` e, quando ela liga a indexação, a
// compatibilidade do tipo da coluna com o índice único. Vive numa função
// porque a congelada faz a MESMA pergunta sobre o tipo de outro campo, e duas
// cópias divergiriam no dia em que o índice passasse a aceitar mais um tipo.
//
// `true` é a ÚNICA grafia aceita (D5): com um motor só, `hash` e `bitmap`
// construiriam exatamente a mesma coisa, e aceitá-las como sinônimo deixaria
// o operador escrevendo uma distinção que não existe mais — o mesmo
// `expireAfterWrite: 30d` decorativo que o projeto já carrega como débito,
// agora na porta de entrada da configuração. `hash` e `bitmap` têm sentinela
// PRÓPRIA (ErrObsoleteIndexSpelling) porque a subida precisa sair com
// ExitDataset, e não com o exit de uso genérico.
func validateIndex(name, index, typ string) error {
	switch index {
	case "":
		return nil
	case "true":
		if !isIndexableType(typ) {
			return specErrf("campo %q: tipo %q não suporta índice", name, typ)
		}
		return nil
	case "hash", "bitmap":
		return fmt.Errorf("%w: campo %q declara index: %s — escreva index: true",
			ErrObsoleteIndexSpelling, name, index)
	default:
		return specErrf("campo %q: index %q inválido; a única grafia aceita é index: true", name, index)
	}
}
