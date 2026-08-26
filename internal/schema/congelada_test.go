package schema

import (
	"strings"
	"testing"
)

// yamlCongelada monta uma spec mínima com o bloco de campos dado. O `mode` é
// parâmetro porque metade das regras de frozenFrom só existe no merge.
func yamlCongelada(mode, campos string) string {
	m := ""
	if mode != "" {
		m = "  mode: " + mode + "\n"
	}
	return "version: 1\nname: t\nmaterialization:\n  key:\n    field: id\n" + m +
		"schema:\n  fields:\n" + campos
}

// recusaCongelada confere que a spec foi recusada e que a mensagem nomeia os
// trechos que o operador precisa ler para corrigir o YAML sozinho.
func recusaCongelada(t *testing.T, yaml string, trechos ...string) {
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

func aceitaCongelada(t *testing.T, yaml string) *DatasetSpec {
	t.Helper()
	s, err := Parse([]byte(yaml), DefaultsPadrao)
	if err != nil {
		t.Fatalf("a spec foi recusada: %v", err)
	}
	return s
}

// O tipo da congelada é HERDADO da origem. Declará-lo abriria a porta para os
// dois divergirem, e a divergência só apareceria na primeira captura.
func TestFrozenFromComTypeEhRecusado(t *testing.T) {
	recusaCongelada(t, yamlCongelada("", "    id:\n      type: string\n"+
		"    sev:\n      type: int64\n"+
		"    origSev:\n      type: int64\n      frozenFrom: sev\n"),
		"frozenFrom", "type")
}

func TestFrozenFromParaCampoInexistenteEhRecusado(t *testing.T) {
	recusaCongelada(t, yamlCongelada("", "    id:\n      type: string\n"+
		"    origSev:\n      frozenFrom: naoExiste\n"),
		"naoExiste", "schema.fields")
}

func TestFrozenFromParaSiMesmaEhRecusado(t *testing.T) {
	recusaCongelada(t, yamlCongelada("", "    id:\n      type: string\n"+
		"    origSev:\n      frozenFrom: origSev\n"),
		"si mesmo")
}

// A cadeia faria a ordem de avaliação virar contrato implícito: o valor de `c`
// dependeria de `b` já ter sido capturada, e nada no YAML diz isso.
func TestFrozenFromEmCadeiaEhRecusado(t *testing.T) {
	recusaCongelada(t, yamlCongelada("", "    id:\n      type: string\n"+
		"    a:\n      type: int64\n"+
		"    b:\n      frozenFrom: a\n"+
		"    c:\n      frozenFrom: b\n"),
		"cadeia")
}

func TestFrozenFromParaAKeyEhRecusado(t *testing.T) {
	recusaCongelada(t, yamlCongelada("", "    id:\n      type: string\n"+
		"    origID:\n      frozenFrom: id\n"),
		"key")
}

// No replace o estado anterior é descartado inteiro, então uma coluna que
// sobrevivesse contradiria o que replace significa.
func TestFrozenFromEmModoReplaceEhRecusado(t *testing.T) {
	recusaCongelada(t, yamlCongelada("replace", "    id:\n      type: string\n"+
		"    sev:\n      type: int64\n"+
		"    origSev:\n      frozenFrom: sev\n"),
		"replace")
}

func TestFormatJuntoDeFrozenFromEhRecusado(t *testing.T) {
	recusaCongelada(t, yamlCongelada("", "    id:\n      type: string\n"+
		"    quando:\n      type: datetime\n      format: \"yyyy-MM-dd HH:mm:ss\"\n"+
		"    origQuando:\n      frozenFrom: quando\n      format: \"yyyy-MM-dd HH:mm:ss\"\n"),
		"format", "frozenFrom")
}

// A sequence é uma coluna comum do ponto de vista do congelamento: congelar a
// PRIMEIRA sequence vista é um pedido legítimo, e nada nela impede a captura.
func TestFrozenFromParaASequenceEhAceito(t *testing.T) {
	yaml := "version: 1\nname: t\nmaterialization:\n  key:\n    field: id\n" +
		"  sequence:\n    field: seq\n" +
		"schema:\n  fields:\n    id:\n      type: string\n" +
		"    seq:\n      type: int64\n" +
		"    origSeq:\n      frozenFrom: seq\n"
	aceitaCongelada(t, yaml)
}

func TestFrozenFromRelogioEhAceito(t *testing.T) {
	s := aceitaCongelada(t, yamlCongelada("", "    id:\n      type: string\n"+
		"    criadoEm:\n      frozenFrom: \"@writeTime\"\n"))
	if got := s.Schema.Fields["criadoEm"].FrozenFrom; got != OrigemRelogio {
		t.Errorf("FrozenFrom = %q, quero %q", got, OrigemRelogio)
	}
}
