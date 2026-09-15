package alarm

import (
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/keyformat"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/types"
)

const (
	fOlt  types.FieldID = 1
	fSlot types.FieldID = 2
)

// defByPON é a definição de um contador agrupado por olt e slot — dois tipos
// diferentes, para que o bind de cada um exercite a conversão do seu tipo.
func defByPON(name string) counter.Definition {
	d := counter.Definition{
		Name:      name,
		Columns:   []types.FieldID{fOlt, fSlot},
		Names:     []string{"olt", "slot"},
		Types:     []types.FieldType{types.TypeString, types.TypeInt64},
		MaxGroups: 1000,
	}
	d.Format = keyformat.Default(keyformat.Columns(d.Names, d.Types))
	return d
}

// rowOf devolve o payload da linha do grupo (olt, slot) com o total dado,
// montado pela MESMA tabela virtual que o SQL consulta.
func rowOf(t *testing.T, def counter.Definition, olt string, slot, total int64) []byte {
	t.Helper()
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: olt, slot: slot, total: total, live: true})
	_, store, err := counters.Table(def, f, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	snap := store.OpenSnapshot()
	rec, _, ok, err := store.GetByRowID(snap, 1, nil)
	if err != nil || !ok {
		t.Fatalf("linha do grupo indisponível: ok=%v err=%v", ok, err)
	}
	return rec.Payload
}

func specWith(alarms ...schema.AlarmSpec) []schema.CounterSpec {
	return []schema.CounterSpec{{
		Name:    "autenticados_por_pon",
		GroupBy: []string{"olt", "slot"},
		Alarms:  alarms,
	}}
}

func alarmeDe(when string, enrich ...schema.EnrichSpec) schema.AlarmSpec {
	return schema.AlarmSpec{Name: "olt_down", When: when, SustainedFor: 2 * time.Minute, Enrich: enrich}
}

// TestCondicaoCompiladaAvaliaALinhaDoContador é o que fecha o desenho: a
// condição roda pelo avaliador compartilhado, sobre o payload que a tabela
// virtual produz. É por isso que não há gramática nova — `total < 100` no
// alarme é o mesmo código de `total < 100` num WHERE.
func TestCondicaoCompiladaAvaliaALinhaDoContador(t *testing.T) {
	def := defByPON("autenticados_por_pon")
	defs, err := Compile("telecom", specWith(alarmeDe("total < 100")), []counter.Definition{def})
	if err != nil {
		t.Fatal(err)
	}
	if len(defs) != 1 || len(defs[0].alarms) != 1 {
		t.Fatalf("compilado = %+v", defs)
	}
	av := defs[0].alarms[0].newEvaluator()
	casos := map[int64]bool{50: true, 99: true, 100: false, 150: false}
	for total, quero := range casos {
		got, err := av.Evaluate(rowOf(t, def, "OLT_A", 3, total))
		if err != nil {
			t.Fatalf("total %d: %v", total, err)
		}
		if got != quero {
			t.Errorf("total %d: condição = %v, quero %v", total, got, quero)
		}
	}
}

// TestCadaAvaliadorTemScratchProprio: o avaliador do predicado tem buffer
// interno e é de UM dono por vez. A fábrica existe para que o laço de alarme e
// qualquer outro leitor não disputem o mesmo scratch.
func TestCadaAvaliadorTemScratchProprio(t *testing.T) {
	def := defByPON("autenticados_por_pon")
	defs, err := Compile("telecom", specWith(alarmeDe("total < 100")), []counter.Definition{def})
	if err != nil {
		t.Fatal(err)
	}
	a, b := defs[0].alarms[0].newEvaluator(), defs[0].alarms[0].newEvaluator()
	if a == b {
		t.Fatal("a fábrica devolveu o MESMO avaliador duas vezes: dois donos no mesmo scratch é data race")
	}
}

// TestBindsSaemLigadosAsColunasDoGrupo: cada `?` é resolvido para a coluna que
// vai alimentá-lo no disparo — posição no groupBy, total, ou o texto do grupo.
func TestBindsSaemLigadosAsColunasDoGrupo(t *testing.T) {
	def := defByPON("autenticados_por_pon")
	defs, err := Compile("telecom", specWith(alarmeDe("total < 100",
		schema.EnrichSpec{
			SQL:  "SELECT customerId FROM servicos WHERE olt = ? AND slot = ? AND seq = ? LIMIT 5",
			Bind: []string{"slot", "groupKey", "total"},
		})), []counter.Definition{def})
	if err != nil {
		t.Fatal(err)
	}
	binds := defs[0].alarms[0].enrich[0].binds
	if len(binds) != 3 {
		t.Fatalf("%d binds, quero 3", len(binds))
	}
	if binds[0].kind != bindGroupColumn || binds[0].groupIndex != 1 || binds[0].fieldType != types.TypeInt64 {
		t.Errorf("bind de slot = %+v; quero coluna 1 do groupBy, int64", binds[0])
	}
	if binds[1].kind != bindGroupKey || binds[1].fieldType != types.TypeString {
		t.Errorf("bind de groupKey = %+v; quero o texto do grupo, string", binds[1])
	}
	if binds[2].kind != bindTotal || binds[2].fieldType != types.TypeInt64 {
		t.Errorf("bind de total = %+v; quero o total, int64", binds[2])
	}
}

// TestPlaceholderDentroDeLiteralNaoEhContado é a razão de a contagem passar
// pelo parser e não por busca textual: `'?'` é o caractere de interrogação
// dentro de uma string, não um parâmetro. Contando por texto, esta consulta
// exigiria um bind que ela não tem, e o alarme seria recusado na subida por um
// defeito que não existe.
func TestPlaceholderDentroDeLiteralNaoEhContado(t *testing.T) {
	def := defByPON("autenticados_por_pon")
	_, err := Compile("telecom", specWith(alarmeDe("total < 100",
		schema.EnrichSpec{SQL: "SELECT customerId FROM servicos WHERE olt LIKE '?%' LIMIT 5"})),
		[]counter.Definition{def})
	if err != nil {
		t.Fatalf("a consulta não tem placeholder nenhum e foi recusada: %v", err)
	}
}

// TestCompilaRecusas cobre o que tem de ser recusado na subida, para que nada
// disso seja descoberto de madrugada quando o alarme dispara.
func TestCompilaRecusas(t *testing.T) {
	def := defByPON("autenticados_por_pon")
	cases := []struct {
		name  string
		specs []schema.CounterSpec
		want  string
	}{
		{
			"condição sobre coluna que a tabela não tem",
			specWith(alarmeDe("inexistente < 100")),
			"inexistente",
		},
		{
			"condição com sintaxe inválida",
			specWith(alarmeDe("total <")),
			"when",
		},
		{
			"condição sobre coluna do dataset que não é do groupBy",
			specWith(alarmeDe("authenticated = true")),
			"authenticated",
		},
		{
			"sql com sintaxe inválida",
			specWith(alarmeDe("total < 100", schema.EnrichSpec{SQL: "SELEC x FROM y LIMIT 1"})),
			"sql",
		},
		{
			"sql sem LIMIT",
			specWith(alarmeDe("total < 100", schema.EnrichSpec{SQL: "SELECT customerId FROM servicos WHERE olt = ?", Bind: []string{"olt"}})),
			"LIMIT",
		},
		{
			"LIMIT acima do teto",
			specWith(alarmeDe("total < 100", schema.EnrichSpec{SQL: "SELECT customerId FROM servicos LIMIT 5000"})),
			"acima do teto",
		},
		{
			"bind de coluna que não é do contador",
			specWith(alarmeDe("total < 100", schema.EnrichSpec{SQL: "SELECT customerId FROM servicos WHERE x = ? LIMIT 1", Bind: []string{"authenticated"}})),
			"authenticated",
		},
		{
			"bind a menos",
			specWith(alarmeDe("total < 100", schema.EnrichSpec{SQL: "SELECT customerId FROM servicos WHERE a = ? AND b = ? LIMIT 1", Bind: []string{"olt"}})),
			"2 parâmetro",
		},
		{
			"bind a mais",
			specWith(alarmeDe("total < 100", schema.EnrichSpec{SQL: "SELECT customerId FROM servicos WHERE a = ? LIMIT 1", Bind: []string{"olt", "slot"}})),
			"1 parâmetro",
		},
		{
			"alarme num contador que não existe",
			[]schema.CounterSpec{{Name: "outro_contador", GroupBy: []string{"olt"}, Alarms: []schema.AlarmSpec{alarmeDe("total < 100")}}},
			"outro_contador",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Compile("telecom", c.specs, []counter.Definition{def})
			if err == nil {
				t.Fatal("aceitou o que tem de ser recusado na subida")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("erro %q não menciona %q", err, c.want)
			}
		})
	}
}

// TestContadorSemAlarmeNaoEntraNoCompilado: sem isto o laço subiria para
// datasets que não declararam alarme nenhum.
func TestContadorSemAlarmeNaoEntraNoCompilado(t *testing.T) {
	def := defByPON("autenticados_por_pon")
	defs, err := Compile("telecom", []schema.CounterSpec{{Name: "autenticados_por_pon", GroupBy: []string{"olt", "slot"}}}, []counter.Definition{def})
	if err != nil {
		t.Fatal(err)
	}
	if len(defs) != 0 {
		t.Fatalf("%d contadores compilados, quero nenhum", len(defs))
	}
}
