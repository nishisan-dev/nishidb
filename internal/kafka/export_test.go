package kafka

import "time"

// PublicaMarcaParaTeste injeta uma marca d'água com um instante ARBITRÁRIO.
//
// É o único jeito de exercitar a fronteira da validade sem esperar um minuto
// de relógio, e o único jeito de provar que o Instantaneo NÃO foi ao broker:
// uma marca fabricada que SOBREVIVE à chamada é a prova de que ele a reusou.
func (c *Consumidor) PublicaMarcaParaTeste(fim map[int32]int64, em time.Time) {
	c.met.publicaMarca(fim, em)
}
