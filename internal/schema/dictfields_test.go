package schema

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// preenche registra um campo no dicionário ANTES do Compile, com ou sem tipo
// fixado — é o estado que uma execução anterior deixou no fields.dict.
func preenche(t *testing.T, d *fakeDict, nome string, tipo types.FieldType) types.FieldID {
	t.Helper()
	id, err := d.LookupOrCreate([]byte(nome))
	if err != nil {
		t.Fatal(err)
	}
	if tipo != types.TypeNull {
		if err := d.SetTypeHint(id, tipo); err != nil {
			t.Fatal(err)
		}
	}
	return id
}

// O requisito: depois de um restart, um campo auto materializado numa execução
// anterior existe no DICIONÁRIO mas não no YAML. Sem carregá-lo aqui,
// FieldByName devolveria false e a query sobre ele erraria "coluna não
// existe" — mentira, o campo tem dados — e o SELECT * o omitiria.
//
// A rota é read-only por construção: o Compile só LÊ o dicionário. A única
// rota que cria campo (ResolveUnknown) é do writer, e o binder não pode criar
// campo por causa de um erro de digitação numa query.
func TestCompileCarregaCamposDoDicionario(t *testing.T) {
	dict := newFakeDict()
	id := preenche(t, dict, "X-Vendor-Attr", types.TypeString)

	s := parseOK(t, baseYAML)
	c, err := s.Compile(dict)
	if err != nil {
		t.Fatal(err)
	}

	f, ok := c.FieldByName([]byte("X-Vendor-Attr"))
	if !ok {
		t.Fatal("campo do dicionário não foi carregado no Compiled")
	}
	if f.ID != id {
		t.Errorf("ID = %d, want %d (o do dicionário, que já está em records duráveis)", f.ID, id)
	}
	if !f.IsAuto {
		t.Error("campo vindo do dicionário é auto: não há declaração no YAML")
	}
	if got := f.EffectiveType(); got != types.TypeString {
		t.Errorf("EffectiveType = %v, want string (o tipo fixado sobrevive ao restart)", got)
	}
	if g, ok := c.FieldByID(id); !ok || g != f {
		t.Error("FieldByID deveria resolver o mesmo ponteiro")
	}
	// SELECT * enxerga a coluna.
	var achou bool
	for _, cf := range c.FieldsByID() {
		if cf.ID == id {
			achou = true
		}
	}
	if !achou {
		t.Error("FieldsByID omitiu o campo do dicionário: SELECT * perderia a coluna")
	}
}

// Campo criado no dicionário mas nunca observado com valor (o evento trouxe
// null) entra SEM tipo. É a diferença entre "não sei o tipo desta coluna" e
// "esta coluna não existe" — a segunda é mentira e fecharia a investigação de
// quem escreveu a query.
func TestCompileCarregaCampoDoDicionarioSemTipo(t *testing.T) {
	dict := newFakeDict()
	id := preenche(t, dict, "X-Sem-Tipo", types.TypeNull)

	c, err := parseOK(t, baseYAML).Compile(dict)
	if err != nil {
		t.Fatal(err)
	}

	f, ok := c.FieldByName([]byte("X-Sem-Tipo"))
	if !ok {
		t.Fatal("campo sem tipo do dicionário deveria existir no Compiled")
	}
	if f.ID != id || !f.IsAuto {
		t.Errorf("campo = %+v, want id=%d auto", f, id)
	}
	if got := f.EffectiveType(); got != types.TypeNull {
		t.Errorf("EffectiveType = %v, want null (nunca observado)", got)
	}
}

// O Compile não pode CRIAR campo: a rota é de leitura. Um campo que só existe
// no dicionário é carregado com o ID que já tem, sem nova entry.
func TestCompileNaoCriaCampoDoDicionario(t *testing.T) {
	dict := newFakeDict()
	preenche(t, dict, "X-Vendor-Attr", types.TypeString)
	antes := append([]string(nil), dict.order...)

	if _, err := parseOK(t, baseYAML).Compile(dict); err != nil {
		t.Fatal(err)
	}

	// O YAML registra os seus; o campo do dicionário não pode aparecer duas
	// vezes na ordem de criação.
	var vezes int
	for _, n := range dict.order {
		if n == "X-Vendor-Attr" {
			vezes++
		}
	}
	if vezes != 1 {
		t.Errorf("X-Vendor-Attr criado %d vezes no dicionário, want 1", vezes)
	}
	if dict.order[0] != antes[0] {
		t.Errorf("ordem do dicionário mudou: %v", dict.order)
	}
}

// Campo excluído é INEXISTENTE para o runtime — inclusive quando o dicionário
// o conhece de uma configuração anterior. Ressuscitá-lo pela porta dos fundos
// desfaria a decisão do operador.
func TestCompileNaoRessuscitaExcluidoDoDicionario(t *testing.T) {
	dict := newFakeDict()
	preenche(t, dict, "debug", types.TypeString) // baseYAML tem exclude: [debug]

	c, err := parseOK(t, baseYAML).Compile(dict)
	if err != nil {
		t.Fatal(err)
	}
	if f, ok := c.FieldByName([]byte("debug")); ok {
		t.Errorf("campo excluído voltou pelo dicionário: %+v", f)
	}
}

// Universo FECHADO (include explícito, ou wildcard sem unknownFields:
// include) não ganha colunas do dicionário: nesses datasets ResolveUnknown
// nunca cria campo, então o que sobra no dicionário é resíduo de uma
// configuração anterior — e o operador que tirou o campo da lista quer ele
// fora. O Compile carrega exatamente o que o writer poderia ter criado.
func TestCompileNaoCarregaDicionarioEmUniversoFechado(t *testing.T) {
	casos := []struct {
		nome string
		yaml string
	}{
		{"include explícito", `
version: 1
name: fechado
materialization:
  key:
    field: id
schema:
  include:
    - id
  fields:
    id:
      type: string
`},
		{"wildcard com unknownFields ignore",
			strings.Replace(baseYAML, "unknownFields: include", "unknownFields: ignore", 1)},
		{"wildcard com unknownFields reject",
			strings.Replace(baseYAML, "unknownFields: include", "unknownFields: reject", 1)},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			dict := newFakeDict()
			preenche(t, dict, "X-Residuo", types.TypeString)

			c, err := parseOK(t, tc.yaml).Compile(dict)
			if err != nil {
				t.Fatal(err)
			}
			if f, ok := c.FieldByName([]byte("X-Residuo")); ok {
				t.Errorf("universo fechado ganhou coluna do dicionário: %+v", f)
			}
		})
	}
}

// Nome do dicionário que colide com o ALIAS de um campo declarado não pode
// sombrear o campo declarado: o alias já resolve para outro FieldID, e uma
// segunda entrada com o mesmo nome tornaria o resultado da query dependente
// da ordem de inserção no mapa.
func TestCompileDicionarioNaoSombreiaAlias(t *testing.T) {
	dict := newFakeDict()
	colisao := preenche(t, dict, "status", types.TypeString) // alias de Acct-Status-Type

	c, err := parseOK(t, baseYAML).Compile(dict)
	if err != nil {
		t.Fatal(err)
	}
	f, ok := c.FieldByName([]byte("status"))
	if !ok {
		t.Fatal("alias deveria continuar resolvendo")
	}
	if f.Name != "Acct-Status-Type" || f.ID == colisao {
		t.Errorf("alias resolveu para %+v, want o campo declarado", f)
	}
}
