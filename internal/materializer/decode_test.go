package materializer

import (
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/jsonp"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// readField devolve o valor materializado de uma coluna pelo NOME.
//
// Lê do payload gravado, e não das entries do decode, porque é o payload que
// a consulta enxerga: um carimbo certo nas entries e errado no record seria
// invisível para um teste que olhasse só o buffer.
func readField(t *testing.T, c *schema.Compiled, st *fakestorage.Store, key, field string) (types.Value, bool) {
	t.Helper()
	cf, ok := c.FieldByName([]byte(field))
	if !ok {
		t.Fatalf("coluna %q não existe no schema compilado", field)
	}
	rec, _, presente, err := st.GetByKey(st.OpenSnapshot(), []byte(key), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !presente {
		t.Fatalf("key %q não materializada", key)
	}
	var r encoding.FieldReader
	r.Reset(rec.Payload)
	for {
		id, v, ok := r.Next()
		if !ok {
			return types.Value{}, false
		}
		if id == cf.ID {
			return v, true
		}
	}
}

// O carimbo de escrita é o da APLICAÇÃO, nunca o do decode.
//
// É a armadilha que a divisão do Apply abre, e ela é silenciosa: o decode pode
// rodar muito antes de a linha ser escrita — em regime, o tempo de fila mais o
// de decode; sob backpressure, uma fila inteira — e `_writtenAt` é o que
// governa o idle timeout e, desde a agenda de vencimentos (§57), o instante em
// que a linha vence. Carimbar no decode adiantaria o vencimento sem que nada
// reclamasse, e o sintoma em produção seria entidade sumindo antes da hora.
//
// O teste separa as duas metades no tempo de propósito: entre o Decode e o
// Aplica o relógio salta uma hora. Só o carimbo do lado certo sobrevive a isso.
func TestOCarimboDeEscritaEhODaAplicacaoNaoODoDecode(t *testing.T) {
	c, st := compiladoWithExpire(t, congeladaYAMLWithTTL)
	m := mustNew(t, c, st)

	decode := time.UnixMilli(1_700_000_000_000)
	aplica := decode.Add(time.Hour)
	agora := decode
	m.UsaRelogio(func() time.Time { return agora })

	var d Decodificado
	m.Decode(&d, []byte(`{"id":"a","sev":5}`))
	if d.err != nil {
		t.Fatal(d.err)
	}
	agora = aplica // o evento esperou na fila
	if r := m.Aplica(&d); r.Err != nil {
		t.Fatal(r.Err)
	}

	// As duas colunas de relógio: o campo de sistema e a congelada @writeTime.
	// A segunda importa tanto quanto a primeira e é mais fácil de esquecer —
	// ela sai do mesmo `agora` e por outro caminho (capturaCongeladas).
	for _, field := range []string{schema.WriteInstantFieldName, "criadoEm"} {
		v, ok := readField(t, c, st, "a", field)
		if !ok {
			t.Fatalf("%s não foi materializada", field)
		}
		if v.I64 != aplica.UnixMilli() {
			t.Errorf("%s = %d (%s), quero %d (%s): o carimbo é o do Aplica",
				field, v.I64, time.UnixMilli(v.I64).UTC(),
				aplica.UnixMilli(), aplica.UTC())
		}
	}
}

// O decode NÃO lê o relógio. Nenhuma vez.
//
// O par deste teste é TestOInstanteEhOMesmoParaOEventoInteiro, que cobra
// exatamente uma leitura por Apply: os dois juntos dizem onde a única leitura
// mora. Sem este, alguém devolveria o `m.agora()` ao Decode "para o Parse
// não receber zero" e o carimbo do Aplica passaria a sobrescrever um valor já
// plausível — a regressão voltaria com a suíte verde.
func TestODecodeNaoLeORelogio(t *testing.T) {
	c, st := compiladoWithExpire(t, yamlWithExpire)
	m := mustNew(t, c, st)
	leituras := 0
	m.UsaRelogio(func() time.Time {
		leituras++
		return time.UnixMilli(1_700_000_000_000)
	})

	var d Decodificado
	m.Decode(&d, []byte(`{"id":"A","severidade":3}`))
	if d.err != nil {
		t.Fatal(d.err)
	}
	if leituras != 0 {
		t.Fatalf("o decode leu o relógio %d vezes; tem de ser 0", leituras)
	}
	if r := m.Aplica(&d); r.Err != nil {
		t.Fatal(r.Err)
	}
	if leituras != 1 {
		t.Fatalf("a aplicação leu o relógio %d vezes; tem de ser 1", leituras)
	}
}

// O contador de eventos só anda na APLICAÇÃO.
//
// Ele é o sinal de progresso que o watchdog da ingestão consulta. Contado no
// decode, um writer morto com decodificadores ocupados publicaria um contador
// subindo, e o veredito do watchdog — que exige a conjunção de eventos
// parados, lag no broker e fila vazia — nunca se formaria: a ingestão pararia
// de vez sem ninguém recriar o consumidor.
//
// O teste decodifica três eventos sem aplicar nenhum. Se o contador estiver do
// lado errado, ele já vale 3 antes de qualquer linha existir.
func TestOContadorDeEventosSoAndaNaAplicacao(t *testing.T) {
	c, st := compiladoWithExpire(t, yamlWithExpire)
	m := mustNew(t, c, st)

	decs := make([]Decodificado, 3)
	for i := range decs {
		m.Decode(&decs[i], []byte(`{"id":"A","severidade":3}`))
		if decs[i].err != nil {
			t.Fatal(decs[i].err)
		}
	}
	if n := m.Metrics().EventsTotal; n != 0 {
		t.Fatalf("EventsTotal = %d depois de 3 decodes e 0 aplicações; quero 0", n)
	}

	if r := m.Aplica(&decs[0]); r.Err != nil {
		t.Fatal(r.Err)
	}
	if n := m.Metrics().EventsTotal; n != 1 {
		t.Fatalf("EventsTotal = %d depois de 1 aplicação; quero 1", n)
	}
}

// N Decodificados coexistem sem se corromper.
//
// É a propriedade que torna a Fase 4.3 possível, e ela é do TIPO, não do
// paralelismo: enquanto os buffers do parse viviam no Materializer, decodificar
// o segundo evento destruía o primeiro. O teste decodifica dois eventos
// DISTINTOS antes de aplicar qualquer um e cobra que o primeiro ainda descreva
// o primeiro evento — sem isto, a esteira aplicaria N cópias do último decode.
func TestDoisDecodificadosNaoSeAtrapalham(t *testing.T) {
	c, st := compiladoWithExpire(t, yamlWithExpire)
	m := mustNew(t, c, st)

	var a, b Decodificado
	m.Decode(&a, []byte(`{"id":"A","severidade":1}`))
	m.Decode(&b, []byte(`{"id":"B","severidade":2}`))
	if a.err != nil || b.err != nil {
		t.Fatalf("decode: %v / %v", a.err, b.err)
	}
	if string(a.key) != "A" || string(b.key) != "B" {
		t.Fatalf("keys = %q / %q, quero A / B", a.key, b.key)
	}

	if r := m.Aplica(&a); r.Action != ActionCreated {
		t.Fatalf("Aplica(a) = %v (%v)", r.Action, r.Err)
	}
	if r := m.Aplica(&b); r.Action != ActionCreated {
		t.Fatalf("Aplica(b) = %v (%v)", r.Action, r.Err)
	}
	for key, quero := range map[string]int64{"A": 1, "B": 2} {
		v, ok := readField(t, c, st, key, "severidade")
		if !ok {
			t.Fatalf("%s: severidade ausente", key)
		}
		if v.I64 != quero {
			t.Errorf("%s.severidade = %d, quero %d", key, v.I64, quero)
		}
	}
}

// O slot não segura o buffer do evento depois de aplicado.
//
// O evento é uma JANELA sobre o record que o franz-go entregou, e o record é
// uma janela sobre o buffer de fetch do lote. Um slot ocioso que guarde essa
// janela mantém vivo o lote INTEIRO — megabytes por causa de alguns KiB —, e o
// anel tem uma janela de slots. É memória que nenhum dos dois gates da fila
// conta, porque a fila já devolveu o orçamento de bytes no Recebe.
//
// São cinco referências e faltar uma é o mesmo que faltar todas: a direta, a do
// documento do pai, e o `data` dos três parsers. O teste cobra as cinco,
// porque uma sobrevivente segura o mesmo lote que as outras cinco juntas.
func TestOSlotNaoSeguraOBufferDoEventoDepoisDeAplicar(t *testing.T) {
	c, st := compiladoWithExpire(t, descobreYAML)
	_ = st
	m := mustNew(t, c, fakestorage.New(fakestorage.Indexes{}))

	var d Decodificado
	m.Decode(&d, []byte(`{"id":"A","doc":{"a":1,"b":"x"}}`))
	if r := m.Aplica(&d); r.Err != nil {
		t.Fatalf("Aplica: %v", r.Err)
	}
	// Antes do Release, o slot segura o evento — é o estado normal em voo.
	if d.event == nil {
		t.Fatal("o teste não alcançou o estado que ele mede: o evento já estava solto")
	}

	d.Release()

	if d.event != nil {
		t.Error("evento continua preso ao slot")
	}
	for i := range d.pais {
		if d.pais[i].value != nil || d.pais[i].field != nil {
			t.Errorf("pais[%d] continua apontando para o documento do evento", i)
		}
	}
	// O `clear` antes do truncamento é o que faz a asserção acima valer: com
	// `pais[:0]` puro, os ponteiros ficam além do comprimento e o coletor os
	// enxerga do mesmo jeito.
	for _, p := range append([]*jsonp.Parser{&d.parser, d.descobridor}, d.aninhados...) {
		if p == nil {
			continue
		}
		if p.RetainedInputBytes() != 0 {
			t.Errorf("um parser continua com %d bytes do evento", p.RetainedInputBytes())
		}
	}
}

// Encolher solta o arena DE VERDADE, e não só o cabeçalho dele.
//
// `key` é uma janela sobre o arena. Anular o arena e deixar a key faria o array
// continuar vivo por aquela referência — e o gauge, que soma `cap(arena)` e
// passou a ver zero, AFIRMARIA que a memória foi devolvida. Métrica mentindo é
// pior que memória presa: tira de quem opera a chance de descobrir.
func TestEncolherSoltaAKeyJuntoComOArena(t *testing.T) {
	c, _ := compiladoWithExpire(t, descobreYAML)
	m := mustNew(t, c, fakestorage.New(fakestorage.Indexes{}))

	var d Decodificado
	m.Decode(&d, []byte(`{"id":"A","doc":{"a":1}}`))
	if r := m.Aplica(&d); r.Err != nil {
		t.Fatalf("Aplica: %v", r.Err)
	}
	if len(d.key) == 0 {
		t.Fatal("o teste não alcançou o estado que ele mede: a key está vazia")
	}
	// Na ordem da produção: o anel chama Release e depois Shrink, e as duas
	// juntas é que devolvem o slot vazio. Chamar só uma delas mediria metade.
	d.Release()
	if !d.Shrink(1) { // limiar 1: encolhe sempre
		t.Fatal("Shrink não encolheu com limiar 1")
	}
	if d.key != nil {
		t.Error("key continua apontando para o arena que o Shrink diz ter soltado")
	}
	if d.arena != nil {
		t.Error("arena continua apontado")
	}
	if n := d.BytesRetidos(); n != 0 {
		t.Errorf("BytesRetidos = %d depois de encolher tudo, quero 0", n)
	}
}
