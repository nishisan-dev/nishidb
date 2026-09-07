package schema

import (
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// yamlWithTTL monta uma spec mínima com o bloco ttl dado.
func yamlWithTTL(ttlBlock string) string {
	return "version: 1\nname: t\nmaterialization:\n  key:\n    field: id\n" + ttlBlock +
		"schema:\n  include: [id, exp]\n  fields:\n    id:\n      type: string\n" +
		"    exp:\n      type: timestamp\n"
}

// O deadline absoluto vindo do payload — `ttl: {field, type}` — foi REMOVIDO:
// era parseado e validado sem nenhum consumidor, e configuração que não faz
// nada ao lado de um mecanismo que funciona é defeito. A recusa nomeia a
// lista, que é para onde o operador tem de migrar.
func TestBlocoTTLAntigoEhRecusadoNomeandoALista(t *testing.T) {
	for _, ttlBlock := range []string{
		"  ttl:\n    field: exp\n    expireAfterWrite: 2h\n",
		"  ttl:\n    type: timestamp\n",
		"  ttl:\n    expireAfterWrite: 2h\n",
	} {
		_, err := Parse([]byte(yamlWithTTL(ttlBlock)), BuiltinDefaults)
		if err == nil {
			t.Errorf("a forma antiga foi aceita: %s", ttlBlock)
			continue
		}
		if !strings.Contains(err.Error(), "lista") {
			t.Errorf("a mensagem precisa apontar a forma nova: %v", err)
		}
	}
}

// Abaixo da resolução do agendador a chave mentiria: o operador leria 30s e
// teria até 90s. Recusa, não arredondamento — mesma doutrina da razão de
// liveness da compaction.
func TestExpireAfterWriteAbaixoDaResolucaoEhRecusadoEmVezDeArredondado(t *testing.T) {
	for _, d := range []string{"30s", "1s", "59s"} {
		yaml := yamlWithTTL("  ttl:\n    - expireAfterWrite: " + d + "\n")
		_, err := Parse([]byte(yaml), BuiltinDefaults)
		if err == nil {
			t.Errorf("%s: aceito", d)
			continue
		}
		if !strings.Contains(err.Error(), "resolução") {
			t.Errorf("%s: a mensagem não explica o motivo: %v", d, err)
		}
	}
	// O piso exato é aceito.
	yaml := yamlWithTTL("  ttl:\n    - expireAfterWrite: 1m\n")
	if _, err := Parse([]byte(yaml), BuiltinDefaults); err != nil {
		t.Errorf("1m (a própria resolução) foi recusado: %v", err)
	}
}

// A regra não referencia campo nenhum do payload, então não exige declaração
// em schema.fields.
func TestExpireAfterWriteNaoExigeCampoNoSchema(t *testing.T) {
	yaml := "version: 1\nname: t\nmaterialization:\n  key:\n    field: id\n" +
		"  ttl:\n    - expireAfterWrite: 7d\n" +
		"schema:\n  include: [id]\n  fields:\n    id:\n      type: string\n"
	spec, err := Parse([]byte(yaml), BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	if got := spec.Materialization.FallbackDuration; got != 7*24*time.Hour {
		t.Fatalf("DuracaoDeFundo = %v, quer 168h", got)
	}
	c, err := spec.Compile(newFakeDict())
	if err != nil {
		t.Fatal(err)
	}
	if c.FallbackDuration != 7*24*time.Hour {
		t.Fatalf("Compiled.FallbackDuration = %v", c.FallbackDuration)
	}
	if len(c.TTLRules) != 1 {
		t.Fatalf("%d regras compiladas, quero 1", len(c.TTLRules))
	}
}

func TestChaveTypeNaRegraEhRecusada(t *testing.T) {
	yaml := yamlWithTTL("  ttl:\n    - expireAfterWrite: 2h\n      type: timestamp\n")
	if _, err := Parse([]byte(yaml), BuiltinDefaults); err == nil {
		t.Fatal("ttl[].type foi aceito")
	}
}

// Era string crua sem parse e sem leitor: configuração que não faz nada e não
// avisa. Agora a gramática é conferida no load.
func TestTombstoneRetentionEhValidadoNoLoad(t *testing.T) {
	good := "version: 1\nname: t\nmaterialization:\n  key:\n    field: id\n  tombstoneRetention: 7d\n" +
		"schema:\n  include: [id]\n  fields:\n    id:\n      type: string\n"
	spec, err := Parse([]byte(good), BuiltinDefaults)
	if err != nil {
		t.Fatalf("7d (a forma do §12) foi recusado: %v", err)
	}
	if got := spec.Materialization.TombstoneRetention; got != 7*24*time.Hour {
		t.Fatalf("TombstoneRetention = %v, quer 168h", got)
	}
	bad := strings.Replace(good, "7d", "sempre", 1)
	if _, err := Parse([]byte(bad), BuiltinDefaults); err == nil {
		t.Fatal("duração inválida foi aceita")
	}
}

// O writer injeta este campo a cada escrita; declará-lo deixaria o evento
// sobrescrever o relógio do writer.
func TestCampoDeSistemaNaoPodeSerDeclarado(t *testing.T) {
	yaml := "version: 1\nname: t\nmaterialization:\n  key:\n    field: id\n" +
		"schema:\n  include: [id]\n  fields:\n    id:\n      type: string\n" +
		"    " + WriteInstantFieldName + ":\n      type: timestamp\n"
	_, err := Parse([]byte(yaml), BuiltinDefaults)
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
	if _, err := Parse([]byte(baseYAML), BuiltinDefaults); err != nil {
		t.Fatalf("a spec base, que declara _SEQ, foi recusada: %v", err)
	}
}

// Quem pede "tudo" quer os campos do próprio dado, não a contabilidade
// interna do banco. Mas quem precisa do conjunto FÍSICO do payload — o
// storage, a compaction — precisa vê-lo.
func TestFieldsByIDOcultaOCampoDeSistemaMasAVersaoCompletaNao(t *testing.T) {
	yaml := "version: 1\nname: t\nmaterialization:\n  key:\n    field: id\n" +
		"  ttl:\n    - expireAfterWrite: 2h\n" +
		"schema:\n  include: [id]\n  fields:\n    id:\n      type: string\n"
	spec, err := Parse([]byte(yaml), BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	c, err := spec.Compile(newFakeDict())
	if err != nil {
		t.Fatal(err)
	}
	if c.WriteInstantField == nil {
		t.Fatal("campo de sistema não compilado")
	}

	hasSystem := func(fields []*CompiledField) bool {
		for _, f := range fields {
			if f.Name == WriteInstantFieldName {
				return true
			}
		}
		return false
	}
	if hasSystem(c.FieldsByID()) {
		t.Error("FieldsByID expôs o campo de sistema: ele apareceria no SELECT * e no /v1/metadata")
	}
	if !hasSystem(c.FieldsByIDIncludingSystem()) {
		t.Error("FieldsByIDIncludingSystem escondeu o campo de sistema")
	}

	// Endereçável por nome, como ROWID/xmin: oculto no *, não inacessível.
	f, ok := c.FieldByName([]byte(WriteInstantFieldName))
	if !ok {
		t.Fatal("FieldByName não resolve o campo de sistema")
	}
	if !f.IsSystem || f.Type != types.TypeTimestamp {
		t.Fatalf("IsSystem=%v Type=%v", f.IsSystem, f.Type)
	}
}
