package executor

import "github.com/nishisan-dev/nishidb/internal/predicado"

// programa e compila são o predicado compilado, agora em internal/predicado.
// Ver o godoc de collect.go para o motivo da extração.
type programa = predicado.Programa

var compila = predicado.Compila
