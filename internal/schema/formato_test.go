package schema

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// yamlComCampo monta uma spec mínima com um campo extra declarado.
func yamlComCampo(decl string) string {
	return "version: 1\nname: t\nmaterialization:\n  key:\n    field: id\n" +
		"schema:\n  include: [id, alvo]\n  fields:\n    id:\n      type: string\n" +
		"    alvo:\n" + decl
}

func parseFalha(t *testing.T, yaml, trecho string) error {
	t.Helper()
	_, err := Parse([]byte(yaml), DefaultsPadrao)
	if err == nil {
		t.Fatalf("esperava recusa contendo %q, a spec foi aceita", trecho)
	}
	if !strings.Contains(err.Error(), trecho) {
		t.Fatalf("mensagem %q não contém %q", err, trecho)
	}
	return err
}

// Sem default de format: um format esquecido escolheria um padrão que recusa
// todo evento, e sob log-and-skip isso é um dataset que não ingere nada.
func TestDatetimeETimeSemFormatSaoRecusados(t *testing.T) {
	for _, tipo := range []string{"datetime", "time"} {
		err := parseFalha(t, yamlComCampo("      type: "+tipo+"\n"), "exige format")
		if !strings.Contains(err.Error(), "yyyy") {
			t.Errorf("%s: a mensagem precisa listar os tokens suportados: %v", tipo, err)
		}
	}
}

func TestFormatInvalidoEhRecusadoNoLoad(t *testing.T) {
	casos := map[string]string{
		"token desconhecido":  "      type: datetime\n      format: \"yyyy-MM-dd EEE HH:mm\"\n",
		"letra nua":           "      type: datetime\n      format: \"yyyy-MM-ddTHH:mm:ss\"\n",
		"ambiguidade":         "      type: datetime\n      format: \"yyyyMd HH:mm\"\n",
		"12h sem meridiano":   "      type: datetime\n      format: \"yyyy-MM-dd hh:mm:ss\"\n",
		"24h com meridiano":   "      type: datetime\n      format: \"yyyy-MM-dd HH:mm:ss a\"\n",
		"time com data":       "      type: time\n      format: \"yyyy-MM-dd HH:mm\"\n",
		"datetime incompleto": "      type: datetime\n      format: \"yyyy-MM-dd\"\n",
	}
	for nome, decl := range casos {
		t.Run(nome, func(t *testing.T) { parseFalha(t, yamlComCampo(decl), "alvo") })
	}
}

// FieldSpec é decodificado sem KnownFields: um format sob um tipo que não o
// usa decodifica com sucesso e não faz nada — configuração sem leitor, que é
// o defeito que o débito 37 registra. Tem de recusar.
func TestFormatEmTipoQueNaoOUsaEhRecusado(t *testing.T) {
	for _, tipo := range []string{"string", "timestamp", "int64", "auto"} {
		decl := "      type: " + tipo + "\n      format: \"yyyy-MM-dd HH:mm\"\n"
		parseFalha(t, yamlComCampo(decl), "format só se aplica")
	}
}

func TestDatetimeETimeNaoSaoIndexaveis(t *testing.T) {
	for _, tipo := range []string{"datetime", "time"} {
		for _, idx := range []string{"bitmap", "hash"} {
			decl := "      type: " + tipo + "\n      format: \"" + formatDe(tipo) + "\"\n      index: " + idx + "\n"
			parseFalha(t, yamlComCampo(decl), "não suporta índice "+idx)
		}
	}
}

func formatDe(tipo string) string {
	if tipo == "time" {
		return "HH:mm:ss"
	}
	return "yyyy-MM-dd HH:mm:ss"
}

func TestSpecValidaCompilaOLayoutDeCadaCampoTemporal(t *testing.T) {
	yaml := yamlComCampo("      type: datetime\n      format: \"MMM d, yyyy hh:mm:ss a\"\n")
	spec, err := Parse([]byte(yaml), DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	c, err := spec.Compile(newFakeDict())
	if err != nil {
		t.Fatal(err)
	}
	f, ok := c.FieldByName([]byte("alvo"))
	if !ok {
		t.Fatal("campo alvo não compilou")
	}
	if f.EffectiveType() != types.TypeDateTime {
		t.Fatalf("tipo efetivo = %v", f.EffectiveType())
	}
	if f.Layout == nil {
		t.Fatal("Layout nil num campo datetime: a ingestão e o binder derefereciariam nil")
	}
	if f.Format != "MMM d, yyyy hh:mm:ss a" {
		t.Fatalf("Format = %q; a mensagem de literal inválido precisa dele", f.Format)
	}
	if got := f.Layout.Padrao(); got != f.Format {
		t.Fatalf("Layout.Padrao() = %q, Format = %q", got, f.Format)
	}
}

func TestCampoNaoTemporalNaoGanhaLayout(t *testing.T) {
	spec, err := Parse([]byte(baseYAML), DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	c, err := spec.Compile(newFakeDict())
	if err != nil {
		t.Fatal(err)
	}
	for _, nome := range []string{"id", "Event-Timestamp", "ratio", "extra"} {
		f, ok := c.FieldByName([]byte(nome))
		if !ok {
			t.Fatalf("campo %q não compilou", nome)
		}
		if f.Layout != nil {
			t.Errorf("campo %q (%v) ganhou Layout", nome, f.Type)
		}
	}
}

// datetime e time só existem DECLARADOS, porque só a declaração traz o
// format. Um hint temporal num campo auto produziria CompiledField com
// EffectiveType temporal e Layout nil — nil deref no materializer e no
// binder, no primeiro evento ou na primeira query. InferType nunca os produz,
// então o hint só chega de dicionário escrito por outra configuração: recusa
// alta na subida, com a causa nomeada.
func TestCampoAutoComHintTemporalEhRecusadoNoCompile(t *testing.T) {
	for _, tag := range []types.FieldType{types.TypeDateTime, types.TypeTime} {
		yaml := yamlComCampo("      type: auto\n")
		spec, err := Parse([]byte(yaml), DefaultsPadrao)
		if err != nil {
			t.Fatal(err)
		}
		dict := newFakeDict()
		id, err := dict.LookupOrCreate([]byte("alvo"))
		if err != nil {
			t.Fatal(err)
		}
		dict.hints[id] = tag

		if _, err := spec.Compile(dict); err == nil {
			t.Errorf("%v: Compile aceitou campo auto com hint temporal", tag)
		} else if !strings.Contains(err.Error(), "format") {
			t.Errorf("%v: a mensagem precisa dizer o que fazer: %v", tag, err)
		}
	}
}

// Contraprova: hint NÃO temporal continua carregando normalmente, senão o
// guard acima teria quebrado a fixação de tipo que sobrevive a restart.
func TestCampoAutoComHintComumContinuaCarregando(t *testing.T) {
	yaml := yamlComCampo("      type: auto\n")
	spec, err := Parse([]byte(yaml), DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	dict := newFakeDict()
	id, err := dict.LookupOrCreate([]byte("alvo"))
	if err != nil {
		t.Fatal(err)
	}
	dict.hints[id] = types.TypeTimestamp

	c, err := spec.Compile(dict)
	if err != nil {
		t.Fatal(err)
	}
	f, ok := c.FieldByName([]byte("alvo"))
	if !ok {
		t.Fatal("campo alvo não compilou")
	}
	if f.EffectiveType() != types.TypeTimestamp {
		t.Fatalf("tipo fixado não sobreviveu: %v", f.EffectiveType())
	}
}
