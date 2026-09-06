package query_test

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// O cenário que motivou a fixação de tipo (R11) e que nunca tinha rodado de
// ponta a ponta com o Store real: um campo que NÃO está no YAML é
// materializado pela ingestão, o processo REINICIA, e a query filtra por ele.
//
// Antes da QP isto era impossível de duas maneiras: o tipo do campo não
// sobrevivia ao restart, e o campo nem aparecia no Compiled. Contra o dublê
// nenhuma das duas falhas apareceria com esta clareza, porque o dicionário do
// dublê vive no teste e não no dataset.
func TestCampoAutoSobreviveAoRestartEFiltra(t *testing.T) {
	ig := &integracao{t: t, dir: t.TempDir() + "/auto", oracle: oraculo{}}
	ig.abre()

	mat, err := materializer.New(ig.sch, ig.store)
	if err != nil {
		t.Fatal(err)
	}
	eventos := []string{
		`{"id":"A","_SEQ":1,"Acct-Status-Type":"Stop","X-Vendor-Attr":"alpha"}`,
		`{"id":"B","_SEQ":1,"Acct-Status-Type":"Stop","X-Vendor-Attr":"beta"}`,
		`{"id":"C","_SEQ":1,"Acct-Status-Type":"Start"}`,
	}
	for _, ev := range eventos {
		if r := mat.Apply([]byte(ev)); r.Err != nil {
			t.Fatalf("ingestão de %s: %v", ev, r.Err)
		}
	}
	if err := ig.store.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// O campo não está no YAML: ele nasceu da ingestão.
	if _, declarado := ig.sch.FieldByName([]byte("X-Vendor-Attr")); !declarado {
		t.Fatal("o campo auto não foi materializado na primeira execução")
	}

	// REINICIA.
	ig.fecha()
	ig.abre()

	campo, ok := ig.sch.FieldByName([]byte("X-Vendor-Attr"))
	if !ok {
		t.Fatal("após o restart, o campo auto sumiu do Compiled: " +
			"a query responderia \"coluna não existe\" para uma coluna com dados")
	}
	if !campo.IsAuto {
		t.Error("o campo deveria ser auto")
	}
	if got := campo.EffectiveType(); got != types.TypeString {
		t.Fatalf("EffectiveType após restart = %v, want string (o tipo fixado é durável)", got)
	}

	// E a query FILTRA por ele.
	got := ig.ids(`SELECT id FROM radius_sessions WHERE "X-Vendor-Attr" = 'alpha'`)
	if len(got) != 1 || got[0] != "A" {
		t.Errorf("filtro por campo auto = %v, want [A]", got)
	}

	// IS NULL sobre ele encontra quem nunca o teve.
	semCampo := ig.ids(`SELECT id FROM radius_sessions WHERE "X-Vendor-Attr" IS NULL`)
	if len(semCampo) != 1 || semCampo[0] != "C" {
		t.Errorf("IS NULL sobre campo auto = %v, want [C]", semCampo)
	}

	// E o SELECT * o inclui como coluna.
	linhas := ig.consulta(`SELECT * FROM radius_sessions WHERE id = 'A'`)
	if len(linhas) != 1 {
		t.Fatalf("SELECT * = %v", linhas)
	}
	if got := linhas[0]["X-Vendor-Attr"]; got != "alpha" {
		t.Errorf("SELECT * não trouxe o campo auto: %v", linhas[0]["X-Vendor-Attr"])
	}
}

// Campo criado no dicionário mas NUNCA observado com valor (o evento trouxe
// null) fica sem tipo. A comparação sobre ele é recusada — com a mensagem que
// diz a verdade, e não com "coluna não existe", que mandaria o operador
// procurar erro de digitação numa coluna que existe.
func TestCampoAutoSemTipoRecusaComMensagemCerta(t *testing.T) {
	ig := &integracao{t: t, dir: t.TempDir() + "/auto2", oracle: oraculo{}}
	ig.abre()

	mat, err := materializer.New(ig.sch, ig.store)
	if err != nil {
		t.Fatal(err)
	}
	// O null cria a entry no dicionário e NÃO fixa tipo.
	if r := mat.Apply([]byte(`{"id":"A","_SEQ":1,"X-Nunca-Visto":null}`)); r.Err != nil {
		t.Fatalf("ingestão: %v", r.Err)
	}
	if err := ig.store.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	ig.fecha()
	ig.abre()

	campo, ok := ig.sch.FieldByName([]byte("X-Nunca-Visto"))
	if !ok {
		t.Fatal("o campo criado no dicionário deveria existir no Compiled após o restart")
	}
	if got := campo.EffectiveType(); got != types.TypeNull {
		t.Errorf("EffectiveType = %v, want null (nunca observado com valor)", got)
	}

	_, err = ig.engine.Execute(t.Context(), dataset,
		`SELECT id FROM radius_sessions WHERE "X-Nunca-Visto" = 'x'`, nil)
	if err == nil {
		t.Fatal("comparação sobre campo sem tipo deveria falhar")
	}
	if strings.Contains(err.Error(), "não existe") {
		t.Errorf("erro = %q: a coluna EXISTE, o que falta é tipo", err)
	}
	if !strings.Contains(err.Error(), "nunca foi observada") {
		t.Errorf("erro = %q, want explicar que o campo ainda não tem tipo", err)
	}

	// IS NULL sobre ele continua válido: presença não depende de tipo.
	if got := ig.ids(`SELECT id FROM radius_sessions WHERE "X-Nunca-Visto" IS NULL`); len(got) != 1 {
		t.Errorf("IS NULL sobre campo sem tipo = %v, want [A]", got)
	}
}
