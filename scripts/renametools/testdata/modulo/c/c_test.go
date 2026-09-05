package c_test

import (
	"testing"

	"exemplo.local/modulo/a"
	"exemplo.local/modulo/c"
)

type dublê struct{}

func (dublê) Apenda(chave []byte) error { return nil }
func (dublê) Fecha() error              { return nil }

func TestUsaAceitaOsDois(t *testing.T) {
	var e a.Escritor = dublê{}
	c.Usa(dublê{}, e)
}
