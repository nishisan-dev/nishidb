package executor

import "errors"

var (
	// ErrInvalidPlan indica que o plano ou a query ligada violam uma
	// pré-condição do executor. É bug do binder/planner, NÃO erro do usuário —
	// por isso sai em Open, antes de qualquer leitura, e não no meio do
	// streaming.
	ErrInvalidPlan = errors.New("executor: plano inválido")

	// ErrMaxRowsExceeded indica resultado truncado no teto implícito
	// query.maxRows. Sai por Err() DEPOIS das rows já emitidas: um resultado
	// truncado apresentado como completo é uma resposta errada, e o cliente
	// precisa poder distinguir "acabou" de "parou no teto".
	ErrMaxRowsExceeded = errors.New("executor: resultado truncado no teto de query.maxRows")
)
