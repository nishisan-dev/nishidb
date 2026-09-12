package alarm

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// specInventory é o dataset que o enriquecimento consulta: um serviço por
// linha, com a OLT e um campo de nome sensível para exercitar a redação.
const specInventory = `version: 1
name: servicos
schemaName: telecom
materialization:
  key:
    field: customerId
schema:
  fields:
    customerId:
      type: string
    olt:
      type: string
    token:
      type: string
`

// engineWithInventory registra o dataset consultável e o povoa.
func engineWithInventory(t *testing.T, linhas ...[3]string) *query.Engine {
	t.Helper()
	spec, err := schema.Parse([]byte(specInventory), schema.BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	sc, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	st := fakestorage.New(fakestorage.Indexes{})
	for i, l := range linhas {
		var buf []byte
		for _, par := range [][2]string{{"customerId", l[0]}, {"olt", l[1]}, {"token", l[2]}} {
			f, ok := sc.FieldByName([]byte(par[0]))
			if !ok {
				t.Fatalf("campo %q ausente do schema", par[0])
			}
			if buf, err = encoding.AppendField(buf, f.ID, types.Value{Type: types.TypeString, Bytes: []byte(par[1])}); err != nil {
				t.Fatal(err)
			}
		}
		if _, _, err := st.Put([]byte(l[0]), int64(i+1), 0, buf, nil); err != nil {
			t.Fatal(err)
		}
	}
	eng := query.NewEngine()
	if err := eng.Register("servicos", sc, st); err != nil {
		t.Fatal(err)
	}
	return eng
}

// runnerComEnriquecimento monta o runner com um alarme que enriquece pela OLT.
func runnerComEnriquecimento(t *testing.T, f *fakeCounter, eng *query.Engine, es ...schema.EnrichSpec) (*Runner, *sinkOfTest, *clockOfTest) {
	t.Helper()
	def := defByPON("autenticados_por_pon")
	defs, err := Compile("servicos", specWith(alarmeDe("total < 100", es...)), []counter.Definition{def})
	if err != nil {
		t.Fatal(err)
	}
	sink := &sinkOfTest{}
	clock := &clockOfTest{now: t0}
	r, err := New(defs, map[string]counters.Reader{"autenticados_por_pon": f},
		Options{Sink: sink, Engine: eng, Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	return r, sink, clock
}

// dispara faz as duas passadas que completam o prazo.
func dispara(t *testing.T, r *Runner, clock *clockOfTest) {
	t.Helper()
	passada(t, r)
	clock.advance(2 * time.Minute)
	passada(t, r)
}

// TestEnriquecimentoAcompanhaODisparo: é o pedido original — o alarme sai com o
// resultado de uma consulta feita no instante em que ele dispara.
func TestEnriquecimentoAcompanhaODisparo(t *testing.T) {
	eng := engineWithInventory(t,
		[3]string{"c1", "OLT_A", "t1"},
		[3]string{"c2", "OLT_A", "t2"},
		[3]string{"c3", "OLT_B", "t3"},
	)
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 3, total: 50, live: true})
	r, sink, clock := runnerComEnriquecimento(t, f, eng, schema.EnrichSpec{
		Name: "servicos_da_olt",
		SQL:  "SELECT customerId FROM servicos WHERE olt = ? LIMIT 5",
		Bind: []string{"olt"},
	})
	dispara(t, r, clock)

	e := sink.last()
	if e.State != StateFiring {
		t.Fatalf("estado = %q", e.State)
	}
	if len(e.Enrichment) != 1 {
		t.Fatalf("%d blocos de enriquecimento, quero 1: %+v", len(e.Enrichment), e.Enrichment)
	}
	en := e.Enrichment[0]
	if en.Name != "servicos_da_olt" {
		t.Errorf("nome do bloco = %q", en.Name)
	}
	if en.Note != "" {
		t.Fatalf("o enriquecimento não trouxe linhas: %q", en.Note)
	}
	// Só as duas linhas da OLT_A: o parâmetro filtrou de verdade.
	rows := string(en.Rows)
	if !strings.Contains(rows, `"c1"`) || !strings.Contains(rows, `"c2"`) {
		t.Errorf("faltam as linhas da OLT_A: %s", rows)
	}
	if strings.Contains(rows, `"c3"`) {
		t.Errorf("veio a linha de outra OLT: o parâmetro não filtrou: %s", rows)
	}
}

// TestValorDoGrupoVaiLigadoComoParametro é o teste de injeção.
//
// O nome de um equipamento é dado que veio do cliente. Interpolado no texto da
// consulta, um apóstrofo dentro dele reescreveria o WHERE — e o alarme passaria
// a enriquecer com o resultado de outra pergunta, ou a falhar com erro de
// sintaxe. Ligado como parâmetro, é só um valor.
func TestValorDoGrupoVaiLigadoComoParametro(t *testing.T) {
	const nomeHostil = `OLT' OR '1'='1`
	eng := engineWithInventory(t,
		[3]string{"c1", nomeHostil, "t1"},
		[3]string{"c2", "OLT_B", "t2"},
	)
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: nomeHostil, slot: 3, total: 50, live: true})
	r, sink, clock := runnerComEnriquecimento(t, f, eng, schema.EnrichSpec{
		Name: "servicos_da_olt",
		SQL:  "SELECT customerId FROM servicos WHERE olt = ? LIMIT 5",
		Bind: []string{"olt"},
	})
	dispara(t, r, clock)

	en := sink.last().Enrichment[0]
	if en.Note != "" {
		t.Fatalf("a consulta falhou com o nome hostil: %q", en.Note)
	}
	rows := string(en.Rows)
	if !strings.Contains(rows, `"c1"`) {
		t.Errorf("a linha do grupo alarmado não veio: %s", rows)
	}
	if strings.Contains(rows, `"c2"`) {
		t.Errorf("o apóstrofo reescreveu o WHERE e a consulta trouxe tudo: %s", rows)
	}
}

// TestEnriquecimentoQueFalhaNaoBarraOAlarme é a decisão fechada no desenho: a
// consulta NUNCA decide se o alarme sai. Um alarme engolido porque uma consulta
// auxiliar falhou é o pior defeito possível neste subsistema.
func TestEnriquecimentoQueFalhaNaoBarraOAlarme(t *testing.T) {
	// Motor sem o dataset registrado: a consulta falha na resolução.
	eng := query.NewEngine()
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 3, total: 50, live: true})
	r, sink, clock := runnerComEnriquecimento(t, f, eng, schema.EnrichSpec{
		Name: "servicos_da_olt",
		SQL:  "SELECT customerId FROM servicos WHERE olt = ? LIMIT 5",
		Bind: []string{"olt"},
	})
	dispara(t, r, clock)

	if len(sink.events) != 1 {
		t.Fatalf("%d eventos, quero 1: o alarme tem de sair mesmo sem enriquecimento", len(sink.events))
	}
	e := sink.last()
	if e.State != StateFiring {
		t.Fatalf("estado = %q", e.State)
	}
	if len(e.Enrichment) != 1 || e.Enrichment[0].Note == "" {
		t.Fatalf("o evento tem de dizer por que o bloco veio vazio: %+v", e.Enrichment)
	}
}

// TestColunaSensivelDoEnriquecimentoEhRedigida: o nome da coluna vem do SQL
// declarado no YAML, e a redação do handler de log NÃO alcança o interior de um
// valor estruturado. Se a substituição não acontecer na montagem, um campo
// chamado `token` sai em claro no log.
func TestColunaSensivelDoEnriquecimentoEhRedigida(t *testing.T) {
	eng := engineWithInventory(t, [3]string{"c1", "OLT_A", "segredo-do-cliente"})
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 3, total: 50, live: true})
	r, sink, clock := runnerComEnriquecimento(t, f, eng, schema.EnrichSpec{
		Name: "servicos_da_olt",
		SQL:  "SELECT customerId, token FROM servicos WHERE olt = ? LIMIT 5",
		Bind: []string{"olt"},
	})
	dispara(t, r, clock)

	rows := string(sink.last().Enrichment[0].Rows)
	if strings.Contains(rows, "segredo-do-cliente") {
		t.Fatalf("o valor de uma coluna sensível saiu em claro: %s", rows)
	}
	if !strings.Contains(rows, "[REDACTADO]") {
		t.Errorf("a coluna sensível tinha de sair redigida: %s", rows)
	}
	if !strings.Contains(rows, `"c1"`) {
		t.Errorf("a redação levou as outras colunas com ela: %s", rows)
	}
}

// TestTetoDeEnriquecimentosPorPassada: milhares de grupos colapsando de uma vez
// não podem virar milhares de consultas. O excedente sai SEM enriquecimento e
// com o motivo, nunca sem o alarme.
func TestTetoDeEnriquecimentosPorPassada(t *testing.T) {
	eng := engineWithInventory(t, [3]string{"c1", "OLT_A", "t1"})
	gs := make([]grupo, 0, maxEnrichmentsPerSweep+4)
	for i := int64(0); i < int64(maxEnrichmentsPerSweep)+4; i++ {
		gs = append(gs, grupo{olt: "OLT_A", slot: i, total: 50, live: true})
	}
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), gs...)
	r, sink, clock := runnerComEnriquecimento(t, f, eng, schema.EnrichSpec{
		Name: "servicos_da_olt",
		SQL:  "SELECT customerId FROM servicos WHERE olt = ? LIMIT 5",
		Bind: []string{"olt"},
	})
	dispara(t, r, clock)

	if len(sink.events) != len(gs) {
		t.Fatalf("%d eventos, quero %d: todo grupo tem de alarmar", len(sink.events), len(gs))
	}
	comLinhas, pulados := 0, 0
	for _, e := range sink.events {
		if len(e.Enrichment) != 1 {
			t.Fatalf("evento sem bloco de enriquecimento: %+v", e)
		}
		if e.Enrichment[0].Note == "" {
			comLinhas++
		} else {
			pulados++
		}
	}
	if comLinhas != maxEnrichmentsPerSweep {
		t.Errorf("%d enriquecidos, quero exatamente o teto de %d", comLinhas, maxEnrichmentsPerSweep)
	}
	if pulados != 4 {
		t.Errorf("%d pulados, quero 4", pulados)
	}
}

// TestResolucaoNaoEnriquece: o enriquecimento existe para dar contexto ao
// PROBLEMA. Na resolução o fato relevante é que ele passou, e executar as
// consultas de novo dobraria o custo para contar isso.
func TestResolucaoNaoEnriquece(t *testing.T) {
	eng := engineWithInventory(t, [3]string{"c1", "OLT_A", "t1"})
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 3, total: 50, live: true})
	r, sink, clock := runnerComEnriquecimento(t, f, eng, schema.EnrichSpec{
		Name: "servicos_da_olt",
		SQL:  "SELECT customerId FROM servicos WHERE olt = ? LIMIT 5",
		Bind: []string{"olt"},
	})
	dispara(t, r, clock)
	f.setTotal("OLT_A", 3, 500)
	clock.advance(time.Minute)
	passada(t, r)

	e := sink.last()
	if e.State != StateResolved {
		t.Fatalf("estado = %q", e.State)
	}
	if len(e.Enrichment) != 0 {
		t.Errorf("a resolução trouxe enriquecimento: %+v", e.Enrichment)
	}
}

// TestDoisEnriquecimentosNoMesmoDisparoNaoSeSobrescrevem: as consultas do mesmo
// alarme compartilham o buffer de montagem, e o evento sobrevive à passada. Sem
// a cópia, o bloco da primeira consulta passaria a apontar para os bytes da
// segunda — e o log sairia com o resultado errado sob o nome certo, que é o
// tipo de defeito que ninguém desconfia ao ler.
//
// A ORDEM das duas consultas é o que faz este teste valer, e a primeira versão
// dele estava errada: com o resultado MAIOR em segundo lugar, o append realoca
// o buffer e nada é sobrescrito — a mutação que remove a cópia passava verde. A
// consulta ampla vem PRIMEIRO, para que a estreita caiba no array dela e o
// reescreva por cima. A asserção de tamanho abaixo é a guarda dessa premissa.
func TestDoisEnriquecimentosNoMesmoDisparoNaoSeSobrescrevem(t *testing.T) {
	eng := engineWithInventory(t,
		[3]string{"aaa", "OLT_A", "t1"},
		[3]string{"bbb", "OLT_B", "t2"},
	)
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 3, total: 50, live: true})
	r, sink, clock := runnerComEnriquecimento(t, f, eng,
		schema.EnrichSpec{
			Name: "todas",
			SQL:  "SELECT customerId, olt FROM servicos LIMIT 5",
		},
		schema.EnrichSpec{
			Name: "da_olt",
			SQL:  "SELECT customerId FROM servicos WHERE olt = ? LIMIT 5",
			Bind: []string{"olt"},
		})
	dispara(t, r, clock)

	e := sink.last()
	if len(e.Enrichment) != 2 {
		t.Fatalf("%d blocos, quero 2: %+v", len(e.Enrichment), e.Enrichment)
	}
	if e.Enrichment[0].Name != "todas" || e.Enrichment[1].Name != "da_olt" {
		t.Fatalf("nomes fora de ordem: %q e %q", e.Enrichment[0].Name, e.Enrichment[1].Name)
	}
	todas, daOlt := string(e.Enrichment[0].Rows), string(e.Enrichment[1].Rows)
	// Guarda da premissa: o segundo resultado tem de CABER no primeiro, senão
	// o append realoca e o teste não exercita o aliasing.
	if len(daOlt) >= len(todas) {
		t.Fatalf("o segundo resultado (%d bytes) não é menor que o primeiro (%d): "+
			"o buffer seria realocado e este teste não provaria nada", len(daOlt), len(todas))
	}
	// As asserções são por PARSE, e não por substring: a sobrescrita corrompe
	// o PREFIXO do primeiro bloco e deixa a cauda intacta, então um
	// strings.Contains ainda encontraria o texto procurado no lixo. A primeira
	// versão deste teste falhava justamente por isso — passava verde com a
	// cópia removida.
	if got := linhasDe(t, "todas", todas); len(got) != 2 {
		t.Errorf("o primeiro bloco tem %d linhas, quero 2: %s", len(got), todas)
	} else if got[0]["customerId"] != "aaa" || got[1]["customerId"] != "bbb" {
		t.Errorf("o primeiro bloco veio com outro conteúdo: %s", todas)
	}
	if got := linhasDe(t, "da_olt", daOlt); len(got) != 1 || got[0]["customerId"] != "aaa" {
		t.Errorf("o segundo bloco não filtrou pela OLT: %s", daOlt)
	}
}

// linhasDe decodifica o array JSON de um bloco de enriquecimento. Um bloco que
// não parseia é corrupção — e é o sintoma exato de um buffer compartilhado.
func linhasDe(t *testing.T, nome, raw string) []map[string]any {
	t.Helper()
	var out []map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("o bloco %q não é JSON válido (%v): %s", nome, err, raw)
	}
	return out
}
