// Package b implementa a.Escritor em outro pacote.
package b

// Arquivo é um Escritor de mentira.
type Arquivo struct {
	// Linhas guarda o que Apenda recebeu.
	Linhas [][]byte
}

// Apenda guarda a chave. Ver a.Escritor.
func (arq *Arquivo) Apenda(chave []byte) error {
	arq.Linhas = append(arq.Linhas, chave)
	return nil
}

// Fecha não faz nada.
func (arq *Arquivo) Fecha() error { return nil }

// Abre devolve um Arquivo vazio.
func Abre() *Arquivo {
	var tamanho int
	tamanho = 0
	_ = tamanho
	return &Arquivo{}
}
