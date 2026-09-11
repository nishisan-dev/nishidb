package executor

import "github.com/nishisan-dev/nishidb/internal/predicate"

// programa e compila são o predicado compilado, agora em internal/predicate.
// Ver o godoc de collect.go para o motivo da extração.
type programa = predicate.Programa

var compile = predicate.Compile
