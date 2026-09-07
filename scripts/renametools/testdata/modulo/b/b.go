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

// Conta tem um parâmetro com o MESMO nome da função: Conta.Conta não é um
// endereço declarado duas vezes (Conta.Conta refere-se sempre ao parâmetro;
// o nome da própria função é endereçado por `Conta`, sem sufixo), e por isso
// não pode sair marcado #ambiguo (achado na varredura real ao contar Defs
// sobre a FuncDecl inteira, que passa pelo Ident do próprio nome).
func Conta(Conta int) int {
	return Conta
}

// Tabela declara a struct ANÔNIMA da tabela de casos dentro da função — a
// forma dominante das tabelas de teste do repositório
// (`casos := []struct{...}{...}`). Não há TypeSpec nem `var` de pacote para o
// rf endereçar, e quem batiza o campo é a variável do `:=` (Ruling R29): o
// inventário emite Tabela.casos.nome e Tabela.casos.conta com espécie
// field#gopls. Sem isso esses campos não chegam ao tradutor — foi assim que
// 17 campos de tabela de caso ficaram em PT-BR depois do G2.
func Tabela() int {
	casos := []struct {
		nome  string
		conta int
	}{{"um", 1}, {"dois", 2}}
	total := 0
	for _, c := range casos {
		if c.nome != "" {
			total += c.conta
		}
	}
	return total
}

// Mapa declara a struct anônima como VALOR de um map, sem nenhum tipo
// nomeado no caminho: o dono continua sendo a variável do `:=`.
func Mapa() int {
	pesos := map[string]struct{ peso int }{"a": {1}}
	return pesos["a"].peso
}

// Pesa recebe a struct anônima por PARÂMETRO: quem batiza o campo é o
// parâmetro (Pesa.p.peso).
func Pesa(p struct{ peso int }) int {
	return p.peso
}

// Marca passa a struct anônima direto na chamada: nenhum nome a batiza, e o
// endereço fica sem componente de dono no meio (Marca.peso).
func Marca() int {
	return Pesa(struct{ peso int }{7})
}
