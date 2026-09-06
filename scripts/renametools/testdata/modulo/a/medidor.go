package a

// Medidor não tem relação de implementação com nada em c: compartilha só a
// FORMA de Mede com c.Regua, ligando-se a ela pelo nome do método — o mesmo
// mecanismo do query.Store/executor.Store/planner.Store do NishiDB real.
type Medidor interface {
	Mede(n int) bool
}
