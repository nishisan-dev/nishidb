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

// Rotula declara um tipo struct DENTRO da função: rotulo não é endereço de
// pacote, então o campo nome não tem sintaxe `T.campo` que o rf resolva — o
// inventário marca esse campo para o aplicador renomear por gopls (Ruling
// R6, achado na varredura real em internal/storage/indices_test.go:36).
func Rotula() string {
	type rotulo struct {
		nome string
	}
	r := rotulo{nome: "x"}
	return r.nome
}
