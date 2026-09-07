package schema

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// yamlWithField monta uma spec mínima com um campo extra declarado.
func yamlWithField(decl string) string {
	return "version: 1\nname: t\nmaterialization:\n  key:\n    field: id\n" +
		"schema:\n  include: [id, alvo]\n  fields:\n    id:\n      type: string\n" +
		"    alvo:\n" + decl
}

// parseFails exige que a spec seja recusada com uma mensagem que contenha o
// trecho. Devolve o erro para quem quiser conferir mais da mensagem.
func parseFails(t *testing.T, yaml, snippet string) error {
	t.Helper()
	_, err := Parse([]byte(yaml), BuiltinDefaults)
	if err == nil {
		t.Fatalf("esperava recusa contendo %q, a spec foi aceita", snippet)
	}
	if !strings.Contains(err.Error(), snippet) {
		t.Fatalf("mensagem %q não contém %q", err, snippet)
	}
	return err
}

// requireRefusal é a forma sem retorno, para os casos que só precisam do fato.
func requireRefusal(t *testing.T, yaml, snippet string) {
	t.Helper()
	_ = parseFails(t, yaml, snippet)
}

// Sem default de format: um format esquecido escolheria um padrão que recusa
// todo evento, e sob log-and-skip isso é um dataset que não ingere nada.
func TestDatetimeETimeSemFormatSaoRecusados(t *testing.T) {
	for _, typ := range []string{"datetime", "time"} {
		err := parseFails(t, yamlWithField("      type: "+typ+"\n"), "exige format")
		if !strings.Contains(err.Error(), "yyyy") {
			t.Errorf("%s: a mensagem precisa listar os tokens suportados: %v", typ, err)
		}
	}
}

func TestFormatInvalidoEhRecusadoNoLoad(t *testing.T) {
	cases := map[string]string{
		"token desconhecido":  "      type: datetime\n      format: \"yyyy-MM-dd EEE HH:mm\"\n",
		"letra nua":           "      type: datetime\n      format: \"yyyy-MM-ddTHH:mm:ss\"\n",
		"ambiguidade":         "      type: datetime\n      format: \"yyyyMd HH:mm\"\n",
		"12h sem meridiano":   "      type: datetime\n      format: \"yyyy-MM-dd hh:mm:ss\"\n",
		"24h com meridiano":   "      type: datetime\n      format: \"yyyy-MM-dd HH:mm:ss a\"\n",
		"time com data":       "      type: time\n      format: \"yyyy-MM-dd HH:mm\"\n",
		"datetime incompleto": "      type: datetime\n      format: \"yyyy-MM-dd\"\n",
	}
	for name, decl := range cases {
		t.Run(name, func(t *testing.T) { requireRefusal(t, yamlWithField(decl), "alvo") })
	}
}

// FieldSpec é decodificado sem KnownFields: um format sob um tipo que não o
// usa decodifica com sucesso e não faz nada — configuração sem leitor, que é
// o defeito que o débito 37 registra. Tem de recusar.
func TestFormatEmTipoQueNaoOUsaEhRecusado(t *testing.T) {
	for _, typ := range []string{"string", "timestamp", "int64", "auto"} {
		decl := "      type: " + typ + "\n      format: \"yyyy-MM-dd HH:mm\"\n"
		requireRefusal(t, yamlWithField(decl), "format só se aplica")
	}
}

// A D5 tirou a segunda dimensão deste teste (bitmap × hash): com um índice só,
// `index: true` é a única grafia que declara indexação, e datetime/time
// continuam fora do conjunto indexável (indexavel, em spec.go) que qualquer
// um dos dois motores antigos já recusava.
func TestDatetimeETimeNaoSaoIndexaveis(t *testing.T) {
	for _, typ := range []string{"datetime", "time"} {
		decl := "      type: " + typ + "\n      format: \"" + formatOf(typ) + "\"\n      index: true\n"
		requireRefusal(t, yamlWithField(decl), "não suporta índice")
	}
}

func formatOf(typ string) string {
	if typ == "time" {
		return "HH:mm:ss"
	}
	return "yyyy-MM-dd HH:mm:ss"
}

func TestSpecValidaCompilaOLayoutDeCadaCampoTemporal(t *testing.T) {
	yaml := yamlWithField("      type: datetime\n      format: \"MMM d, yyyy hh:mm:ss a\"\n")
	spec, err := Parse([]byte(yaml), BuiltinDefaults)
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
	if got := f.Layout.Pattern(); got != f.Format {
		t.Fatalf("Layout.Pattern() = %q, Format = %q", got, f.Format)
	}
}

func TestCampoNaoTemporalNaoGanhaLayout(t *testing.T) {
	spec, err := Parse([]byte(baseYAML), BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	c, err := spec.Compile(newFakeDict())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"id", "Event-Timestamp", "ratio", "extra"} {
		f, ok := c.FieldByName([]byte(name))
		if !ok {
			t.Fatalf("campo %q não compilou", name)
		}
		if f.Layout != nil {
			t.Errorf("campo %q (%v) ganhou Layout", name, f.Type)
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
		yaml := yamlWithField("      type: auto\n")
		spec, err := Parse([]byte(yaml), BuiltinDefaults)
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
	yaml := yamlWithField("      type: auto\n")
	spec, err := Parse([]byte(yaml), BuiltinDefaults)
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
