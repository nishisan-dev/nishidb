package executor

import "github.com/nishisan-dev/nishidb/internal/predicate"

// O avaliador mora em internal/predicate desde a extração: `type = 2` numa
// regra de TTL e `type = 2` num WHERE têm de ser o MESMO código, senão as
// duas divergem em silêncio nos casos de coerção.
//
// Os apelidos existem para que os laços de sort.go, topn.go e row.go
// continuem escrevendo `slot` e `coleta` — o nome curto é do domínio do
// executor, e trocá-lo em toda linha seria ruído de renomeação numa
// refatoração que é para ser invisível.
type slot = predicate.Slot

var (
	novosSlots = predicate.NovosSlots
	coleta     = predicate.Coleta
)
