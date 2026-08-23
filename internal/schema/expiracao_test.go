package schema

import (
	"strings"
	"testing"
	"time"
)

// yamlComTTL monta uma spec mínima com o bloco ttl dado.
func yamlComTTL(bloco string) string {
	return "version: 1\nname: t\nmaterialization:\n  key:\n    field: id\n" + bloco +
		"schema:\n  include: [id, exp]\n  fields:\n    id:\n      type: string\n" +
		"    exp:\n      type: timestamp\n"
}

// As duas formas precisam de agendadores diferentes: com duração uniforme o
// prazo é monótono na ordem de escrita e a fila é uma deque; com o prazo vindo
// do payload ele pode chegar no passado. Aceitar as duas juntas seria escrever
// dois agendadores e deixar a interação invisível no dado.
func TestExpireAfterWriteEFieldNoMesmoBlocoSaoRecusados(t *testing.T) {
	yaml := yamlComTTL("  ttl:\n    field: exp\n    expireAfterWrite: 2h\n")
	_, err := Parse([]byte(yaml), DefaultsPadrao)
	if err == nil {
		t.Fatal("as duas formas juntas foram aceitas")
	}
	for _, trecho := range []string{"field", "expireAfterWrite"} {
		if !strings.Contains(err.Error(), trecho) {
			t.Errorf("a mensagem precisa nomear %q para o operador saber o que escolher: %v", trecho, err)
		}
	}
}

func TestBlocoTTLVazioEhRecusado(t *testing.T) {
	yaml := yamlComTTL("  ttl:\n    type: timestamp\n")
	if _, err := Parse([]byte(yaml), DefaultsPadrao); err == nil {
		t.Fatal("bloco ttl sem field e sem expireAfterWrite foi aceito")
	}
}

// Abaixo da resolução do agendador a chave mentiria: o operador leria 30s e
// teria até 90s. Recusa, não arredondamento — mesma doutrina da razão de
// liveness da compaction.
func TestExpireAfterWriteAbaixoDaResolucaoEhRecusadoEmVezDeArredondado(t *testing.T) {
	for _, d := range []string{"30s", "1s", "59s"} {
		yaml := yamlComTTL("  ttl:\n    expireAfterWrite: " + d + "\n")
		_, err := Parse([]byte(yaml), DefaultsPadrao)
		if err == nil {
			t.Errorf("%s: aceito", d)
			continue
		}
		if !strings.Contains(err.Error(), "resolução") {
			t.Errorf("%s: a mensagem não explica o motivo: %v", d, err)
		}
	}
	// O piso exato é aceito.
	yaml := yamlComTTL("  ttl:\n    expireAfterWrite: 1m\n")
	if _, err := Parse([]byte(yaml), DefaultsPadrao); err != nil {
		t.Errorf("1m (a própria resolução) foi recusado: %v", err)
	}
}

// A forma idle não referencia campo nenhum do payload, então não deve exigir
// declaração em schema.fields — ao contrário da forma ttl.field.
func TestExpireAfterWriteNaoExigeCampoNoSchema(t *testing.T) {
	yaml := "version: 1\nname: t\nmaterialization:\n  key:\n    field: id\n" +
		"  ttl:\n    expireAfterWrite: 7d\n" +
		"schema:\n  include: [id]\n  fields:\n    id:\n      type: string\n"
	spec, err := Parse([]byte(yaml), DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	if got := spec.Materialization.TTL.ExpireAfterWrite; got != 7*24*time.Hour {
		t.Fatalf("ExpireAfterWrite = %v, quer 168h", got)
	}
	c, err := spec.Compile(newFakeDict())
	if err != nil {
		t.Fatal(err)
	}
	if c.ExpireAfterWrite != 7*24*time.Hour {
		t.Fatalf("Compiled.ExpireAfterWrite = %v", c.ExpireAfterWrite)
	}
	if c.TTL != nil {
		t.Error("a forma idle não tem campo de TTL")
	}
}

func TestTypeNaoSeAplicaAExpireAfterWrite(t *testing.T) {
	yaml := yamlComTTL("  ttl:\n    expireAfterWrite: 2h\n    type: timestamp\n")
	if _, err := Parse([]byte(yaml), DefaultsPadrao); err == nil {
		t.Fatal("ttl.type junto de expireAfterWrite foi aceito")
	}
}

// Era string crua sem parse e sem leitor: configuração que não faz nada e não
// avisa. Agora a gramática é conferida no load.
func TestTombstoneRetentionEhValidadoNoLoad(t *testing.T) {
	bom := "version: 1\nname: t\nmaterialization:\n  key:\n    field: id\n  tombstoneRetention: 7d\n" +
		"schema:\n  include: [id]\n  fields:\n    id:\n      type: string\n"
	spec, err := Parse([]byte(bom), DefaultsPadrao)
	if err != nil {
		t.Fatalf("7d (a forma do §12) foi recusado: %v", err)
	}
	if got := spec.Materialization.TombstoneRetention; got != 7*24*time.Hour {
		t.Fatalf("TombstoneRetention = %v, quer 168h", got)
	}
	ruim := strings.Replace(bom, "7d", "sempre", 1)
	if _, err := Parse([]byte(ruim), DefaultsPadrao); err == nil {
		t.Fatal("duração inválida foi aceita")
	}
}

// O writer injeta este campo a cada escrita; declará-lo deixaria o evento
// sobrescrever o relógio do writer.
func TestCampoDeSistemaNaoPodeSerDeclarado(t *testing.T) {
	yaml := "version: 1\nname: t\nmaterialization:\n  key:\n    field: id\n" +
		"schema:\n  include: [id]\n  fields:\n    id:\n      type: string\n" +
		"    " + CampoInstanteDeEscrita + ":\n      type: timestamp\n"
	_, err := Parse([]byte(yaml), DefaultsPadrao)
	if err == nil {
		t.Fatal("campo de sistema foi aceito em schema.fields")
	}
	if !strings.Contains(err.Error(), "reservado") {
		t.Errorf("a mensagem não diz que é reservado: %v", err)
	}
}

// A reserva é uma lista FECHADA, não o prefixo "_": campos com underscore são
// legítimos e já estão em uso — _SEQ é o nome de sequence do §12. Reservar o
// prefixo recusaria datasets que hoje sobem.
func TestCampoComUnderscoreQueNaoEhDeSistemaContinuaValido(t *testing.T) {
	if _, err := Parse([]byte(baseYAML), DefaultsPadrao); err != nil {
		t.Fatalf("a spec base, que declara _SEQ, foi recusada: %v", err)
	}
}
