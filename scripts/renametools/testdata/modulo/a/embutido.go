package a

import "exemplo.local/modulo/b"

// EnvoltorioDeArquivo embrulha um b.Arquivo só para o teste de grupos: Apenda
// e Fecha aqui são promovidos pelo campo embutido, não declarados por
// EnvoltorioDeArquivo — não podem aparecer como implementador em nome dele. A
// varredura do repositório real achou justamente isso: um tipo cujo pacote é
// varrido antes do dono verdadeiro do método promovido roubava a linha do
// grupo (ex.: internal/storage.ParComHistorico embrulhando *Store.Put).
type EnvoltorioDeArquivo struct {
	*b.Arquivo
}
