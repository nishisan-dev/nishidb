package schema

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
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

func compilaCongelada(t *testing.T, yaml string) *Compiled {
	t.Helper()
	s := aceitaCongelada(t, yaml)
	c, err := s.Compile(newFakeDict())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return c
}

func TestCompilaCongeladaHerdaOTipoDaOrigem(t *testing.T) {
	c := compilaCongelada(t, yamlCongelada("", "    id:\n      type: string\n"+
		"    sev:\n      type: int64\n"+
		"    origSev:\n      frozenFrom: sev\n"))
	f, ok := c.FieldByName([]byte("origSev"))
	if !ok {
		t.Fatal("origSev não compilou")
	}
	if f.Type != types.TypeInt64 {
		t.Errorf("tipo herdado = %v, quero int64", f.Type)
	}
	if f.FrozenFrom == nil || f.FrozenFrom.Name != "sev" {
		t.Error("a origem não foi religada")
	}
	if f.IsSystem {
		t.Error("congelada virou campo de sistema; ela é dado do usuário e tem de sair no SELECT *")
	}
	if len(c.Congeladas) != 1 || c.Congeladas[0] != f {
		t.Error("a lista pré-computada não tem a congelada")
	}
}

func TestCompilaCongeladaDeOrigemAutoTambemEhAuto(t *testing.T) {
	c := compilaCongelada(t, yamlCongelada("", "    id:\n      type: string\n"+
		"    sev:\n      type: auto\n"+
		"    origSev:\n      frozenFrom: sev\n"))
	f, _ := c.FieldByName([]byte("origSev"))
	if !f.IsAuto {
		t.Error("a congelada de origem auto precisa ser auto: o tipo dela só é conhecido na primeira observação")
	}
}

func TestCompilaCongeladaDoRelogioEhTimestamp(t *testing.T) {
	c := compilaCongelada(t, yamlCongelada("", "    id:\n      type: string\n"+
		"    criadoEm:\n      frozenFrom: \"@writeTime\"\n"))
	f, ok := c.FieldByName([]byte("criadoEm"))
	if !ok {
		t.Fatal("criadoEm não compilou")
	}
	if f.Type != types.TypeTimestamp || !f.DoRelogio {
		t.Errorf("congelada do relógio = (tipo %v, doRelogio %v), quero (timestamp, true)", f.Type, f.DoRelogio)
	}
	if f.FrozenFrom != nil {
		t.Error("a congelada do relógio não tem coluna de origem")
	}
	if len(c.Congeladas) != 1 || c.Congeladas[0] != f {
		t.Error("a congelada do relógio precisa entrar na lista pré-computada")
	}
}

// A ordem é a de FieldID pela mesma razão das Derivadas: o materializer
// consulta a lista por evento e não pode varrer o mapa no caminho quente.
func TestCongeladasSaemOrdenadasPorFieldID(t *testing.T) {
	c := compilaCongelada(t, yamlCongelada("", "    id:\n      type: string\n"+
		"    a:\n      type: int64\n"+
		"    b:\n      type: int64\n"+
		"    zOrigA:\n      frozenFrom: a\n"+
		"    mOrigB:\n      frozenFrom: b\n"))
	if len(c.Congeladas) != 2 {
		t.Fatalf("%d congeladas, quero 2", len(c.Congeladas))
	}
	if c.Congeladas[0].ID >= c.Congeladas[1].ID {
		t.Errorf("fora de ordem: %d depois de %d", c.Congeladas[1].ID, c.Congeladas[0].ID)
	}
}

// O índice da congelada se decide pelo tipo da ORIGEM: é o valor dela que vai
// para a posting list.
func TestIndiceDaCongeladaSegueOTipoDaOrigem(t *testing.T) {
	aceitaCongelada(t, yamlCongelada("", "    id:\n      type: string\n"+
		"    sev:\n      type: int64\n"+
		"    origSev:\n      frozenFrom: sev\n      index: true\n"))
	recusaCongelada(t, yamlCongelada("", "    id:\n      type: string\n"+
		"    quando:\n      type: timestamp\n"+
		"    origQuando:\n      frozenFrom: quando\n      index: true\n"),
		"timestamp")
}

func TestIndiceSobreACongeladaDoRelogioEhRecusado(t *testing.T) {
	recusaCongelada(t, yamlCongelada("", "    id:\n      type: string\n"+
		"    criadoEm:\n      frozenFrom: \"@writeTime\"\n      index: true\n"),
		"timestamp")
}

// O format é do parsing da ORIGEM, e a congelada não o declara — mas precisa
// dele: em datetime é o layout que converte o literal de texto do WHERE. Sem a
// herança, a coluna guardaria o valor e recusaria a grafia que a origem aceita.
func TestCongeladaDeDatetimeHerdaOLayoutDaOrigem(t *testing.T) {
	c := compilaCongelada(t, yamlCongelada("", "    id:\n      type: string\n"+
		"    visto:\n      type: datetime\n      format: \"MMM d, yyyy h:mm:ss a\"\n"+
		"    primeiroVisto:\n      frozenFrom: visto\n"))
	f, ok := c.FieldByName([]byte("primeiroVisto"))
	if !ok {
		t.Fatal("primeiroVisto não compilou")
	}
	if f.Type != types.TypeDateTime {
		t.Fatalf("tipo = %v, quero datetime", f.Type)
	}
	if f.Layout == nil {
		t.Fatal("o layout não foi herdado: o WHERE sobre a coluna recusaria todo literal de texto")
	}
	if origem, _ := c.FieldByName([]byte("visto")); f.Format != origem.Format {
		t.Errorf("format = %q, a origem tem %q", f.Format, origem.Format)
	}
}
