// Package a declara a interface e o consumidor.
package a

// Escritor recebe registros. Fecha libera o que Apenda reteve.
type Escritor interface {
	// Apenda apenda um registro.
	Apenda(chave []byte) error
	Fecha() error
}

// Contagem é o que Roda devolve.
type Contagem struct {
	// Total é o total apendado.
	Total int
	erros int
}

// Roda apenda em e e conta.
func Roda(e Escritor, chaves [][]byte) Contagem {
	var c Contagem
	for _, chave := range chaves {
		if err := e.Apenda(chave); err != nil {
			c.erros++
			continue
		}
		c.Total++
	}
	return c
}
