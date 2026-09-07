package c

// Regua tem a MESMA forma de a.Gauge (mesmo nome e assinatura de método),
// sem uma implementar a outra — ligam-se pelo nome do método.
//
// Anel só embute Regua, sem método explícito próprio: é o tipo-ponte que a
// varredura do repositório real achou roubando a atribuição de quem declara
// de verdade (internal/query.Store, que só embute executor.Store e
// planner.Store, roubava Indexado de planner.Store por satisfazer de raspão
// internal/query/meta.Capacidades). Anel entra ANTES de Regua na varredura de
// `ifaces` do pacote c (ordem alfabética), que é a ordem que reproduz o bug.
type Regua interface {
	Mede(n int) bool
}

// Anel só embute Regua — não declara Mede por si.
type Anel interface {
	Regua
}
