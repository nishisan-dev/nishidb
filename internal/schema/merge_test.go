package schema

import (
	"strings"
	"testing"
)

// yamlComMerge monta uma spec mínima com o bloco de campos dado.
func yamlComMerge(mode, campos string) string {
	m := ""
	if mode != "" {
		m = "  mode: " + mode + "\n"
	}
	return "version: 1\nname: t\nmaterialization:\n  key:\n    field: id\n" + m +
		"schema:\n  fields:\n" + campos
}

func recusaMerge(t *testing.T, yaml string, trechos ...string) {
	t.Helper()
	_, err := Parse([]byte(yaml), DefaultsPadrao)
	if err == nil {
		t.Fatal("a spec foi aceita")
	}
	for _, trecho := range trechos {
		if !strings.Contains(err.Error(), trecho) {
			t.Errorf("a mensagem precisa nomear %q: %v", trecho, err)
		}
	}
}

func TestMergeSomaEhAceitoEmInt64(t *testing.T) {
	s, err := Parse([]byte(yamlComMerge("", "    id:\n      type: string\n"+
		"    contador:\n      type: int64\n      merge: sum\n")), DefaultsPadrao)
	if err != nil {
		t.Fatalf("merge: sum sobre int64 foi recusado: %v", err)
	}
	if got := s.Schema.Fields["contador"].Merge; got != MergeSoma {
		t.Errorf("Merge = %q, quero %q", got, MergeSoma)
	}
}

// A soma é de INTEIRO. Ponto flutuante acumula erro a cada parcela, e um
// contador que deriva do próprio histórico não pode depender da ordem em que
// as parcelas chegaram.
func TestMergeSomaEmTipoNaoInteiroEhRecusado(t *testing.T) {
	for _, tipo := range []string{"double", "string", "boolean", "timestamp"} {
		recusaMerge(t, yamlComMerge("", "    id:\n      type: string\n"+
			"    campo:\n      type: "+tipo+"\n      merge: sum\n"), "merge", tipo)
	}
}

// Campo auto não tem tipo em plan-time: aceitar a política aqui deixaria a
// soma valer ou não conforme o primeiro evento que chegasse.
func TestMergeSomaEmCampoAutoEhRecusado(t *testing.T) {
	recusaMerge(t, yamlComMerge("", "    id:\n      type: string\n"+
		"    campo:\n      type: auto\n      merge: sum\n"), "auto")
}

func TestMergeDesconhecidoEhRecusado(t *testing.T) {
	recusaMerge(t, yamlComMerge("", "    id:\n      type: string\n"+
		"    campo:\n      type: int64\n      merge: media\n"), "media", "sum")
}

// No replace o estado anterior é descartado inteiro: não há a que somar.
func TestMergeSomaEmModoReplaceEhRecusado(t *testing.T) {
	recusaMerge(t, yamlComMerge("replace", "    id:\n      type: string\n"+
		"    contador:\n      type: int64\n      merge: sum\n"), "replace")
}

func TestMergeSomaNaKeyEhRecusado(t *testing.T) {
	recusaMerge(t, yamlComMerge("", "    id:\n      type: string\n      merge: sum\n"), "key")
}

func TestMergeSomaNaSequenceEhRecusado(t *testing.T) {
	yaml := "version: 1\nname: t\nmaterialization:\n  key:\n    field: id\n" +
		"  sequence:\n    field: seq\n" +
		"schema:\n  fields:\n    id:\n      type: string\n" +
		"    seq:\n      type: int64\n      merge: sum\n"
	recusaMerge(t, yaml, "sequence")
}

// As duas são políticas de escrita do MESMO campo, e opostas: uma preserva o
// anterior, a outra o acumula.
func TestMergeSomaJuntoDeFrozenFromEhRecusado(t *testing.T) {
	recusaMerge(t, yamlComMerge("", "    id:\n      type: string\n"+
		"    origem:\n      type: int64\n"+
		"    congelada:\n      frozenFrom: origem\n      merge: sum\n"), "frozenFrom", "merge")
}

func TestCompilaResolveAPoliticaDeSoma(t *testing.T) {
	s, err := Parse([]byte(yamlComMerge("", "    id:\n      type: string\n"+
		"    contador:\n      type: int64\n      merge: sum\n"+
		"    normal:\n      type: int64\n")), DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.Compile(newFakeDict())
	if err != nil {
		t.Fatal(err)
	}
	contador, ok := c.FieldByName([]byte("contador"))
	if !ok {
		t.Fatal("contador não compilou")
	}
	if !contador.Acumula {
		t.Error("a política de soma não chegou ao campo compilado")
	}
	normal, _ := c.FieldByName([]byte("normal"))
	if normal.Acumula {
		t.Error("campo sem a chave virou acumulador")
	}
}
