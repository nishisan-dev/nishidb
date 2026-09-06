// Package c tem uma interface que se SOBREPÕE a a.Escritor: só Apenda.
package c

import (
	"exemplo.local/modulo/a"
	"exemplo.local/modulo/b"
)

// SoApenda é satisfeita por tudo que satisfaz a.Escritor.
type SoApenda interface {
	Apenda(chave []byte) error
}

// Usa recebe as duas formas.
func Usa(s SoApenda, e a.Escritor) {
	_ = s.Apenda(nil)
	_ = e.Apenda(nil)
	arq := b.Abre()
	_ = arq.Apenda([]byte("z"))
}
