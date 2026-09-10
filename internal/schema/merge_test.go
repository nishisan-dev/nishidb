package schema

import (
	"strings"
	"testing"
)

// yamlWithMerge monta uma spec mínima com o bloco de campos dado.
func yamlWithMerge(mode, fields string) string {
	m := ""
	if mode != "" {
		m = "  mode: " + mode + "\n"
	}
	return "version: 1\nname: t\nmaterialization:\n  key:\n    field: id\n" + m +
		"schema:\n  fields:\n" + fields
}

func refusesMerge(t *testing.T, yaml string, snippets ...string) {
	t.Helper()
	_, err := Parse([]byte(yaml), BuiltinDefaults)
	if err == nil {
		t.Fatal("a spec foi aceita")
	}
	for _, snippet := range snippets {
		if !strings.Contains(err.Error(), snippet) {
			t.Errorf("a mensagem precisa nomear %q: %v", snippet, err)
		}
	}
}

func TestMergeSomaEhAceitoEmInt64(t *testing.T) {
	s, err := Parse([]byte(yamlWithMerge("", "    id:\n      type: string\n"+
		"    contador:\n      type: int64\n      merge: sum\n")), BuiltinDefaults)
	if err != nil {
		t.Fatalf("merge: sum sobre int64 foi recusado: %v", err)
	}
	if got := s.Schema.Fields["contador"].Merge; got != MergeSum {
		t.Errorf("Merge = %q, quero %q", got, MergeSum)
	}
}

// A soma é de INTEIRO. Ponto flutuante acumula erro a cada parcela, e um
// contador que deriva do próprio histórico não pode depender da ordem em que
// as parcelas chegaram.
func TestMergeSomaEmTipoNaoInteiroEhRecusado(t *testing.T) {
	for _, typ := range []string{"double", "string", "boolean", "timestamp"} {
		refusesMerge(t, yamlWithMerge("", "    id:\n      type: string\n"+
			"    campo:\n      type: "+typ+"\n      merge: sum\n"), "merge", typ)
	}
}

// Campo auto não tem tipo em plan-time: aceitar a política aqui deixaria a
// soma valer ou não conforme o primeiro evento que chegasse.
func TestMergeSomaEmCampoAutoEhRecusado(t *testing.T) {
	refusesMerge(t, yamlWithMerge("", "    id:\n      type: string\n"+
		"    campo:\n      type: auto\n      merge: sum\n"), "auto")
}

func TestMergeDesconhecidoEhRecusado(t *testing.T) {
	refusesMerge(t, yamlWithMerge("", "    id:\n      type: string\n"+
		"    campo:\n      type: int64\n      merge: media\n"), "media", "sum")
}

// No replace o estado anterior é descartado inteiro: não há a que somar.
func TestMergeSomaEmModoReplaceEhRecusado(t *testing.T) {
	refusesMerge(t, yamlWithMerge("replace", "    id:\n      type: string\n"+
		"    contador:\n      type: int64\n      merge: sum\n"), "replace")
}

func TestMergeSomaNaKeyEhRecusado(t *testing.T) {
	refusesMerge(t, yamlWithMerge("", "    id:\n      type: string\n      merge: sum\n"), "key")
}

func TestMergeSomaNaSequenceEhRecusado(t *testing.T) {
	yaml := "version: 1\nname: t\nmaterialization:\n  key:\n    field: id\n" +
		"  sequence:\n    field: seq\n" +
		"schema:\n  fields:\n    id:\n      type: string\n" +
		"    seq:\n      type: int64\n      merge: sum\n"
	refusesMerge(t, yaml, "sequence")
}

// As duas são políticas de escrita do MESMO campo, e opostas: uma preserva o
// anterior, a outra o acumula.
func TestMergeSomaJuntoDeFrozenFromEhRecusado(t *testing.T) {
	refusesMerge(t, yamlWithMerge("", "    id:\n      type: string\n"+
		"    origem:\n      type: int64\n"+
		"    congelada:\n      frozenFrom: origem\n      merge: sum\n"), "frozenFrom", "merge")
}

func TestCompilaResolveAPoliticaDeSoma(t *testing.T) {
	s, err := Parse([]byte(yamlWithMerge("", "    id:\n      type: string\n"+
		"    contador:\n      type: int64\n      merge: sum\n"+
		"    normal:\n      type: int64\n")), BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.Compile(newFakeDict())
	if err != nil {
		t.Fatal(err)
	}
	counter, ok := c.FieldByName([]byte("contador"))
	if !ok {
		t.Fatal("contador não compilou")
	}
	if !counter.Accumulates {
		t.Error("a política de soma não chegou ao campo compilado")
	}
	normal, _ := c.FieldByName([]byte("normal"))
	if normal.Accumulates {
		t.Error("campo sem a chave virou acumulador")
	}
}
