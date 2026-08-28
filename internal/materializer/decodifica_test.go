package materializer

import (
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// leCampo devolve o valor materializado de uma coluna pelo NOME.
//
// Lê do payload gravado, e não das entries do decode, porque é o payload que
// a consulta enxerga: um carimbo certo nas entries e errado no record seria
// invisível para um teste que olhasse só o buffer.
func leCampo(t *testing.T, c *schema.Compiled, st *fakestorage.Store, key, campo string) (types.Value, bool) {
	t.Helper()
	cf, ok := c.FieldByName([]byte(campo))
	if !ok {
		t.Fatalf("coluna %q não existe no schema compilado", campo)
	}
	rec, _, presente, err := st.GetByKey([]byte(key), nil)
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
// O teste separa as duas metades no tempo de propósito: entre o Decodifica e o
// Aplica o relógio salta uma hora. Só o carimbo do lado certo sobrevive a isso.
func TestOCarimboDeEscritaEhODaAplicacaoNaoODoDecode(t *testing.T) {
	c, st := compiladoComExpire(t, congeladaYAMLComTTL)
	m := mustNew(t, c, st)

	decode := time.UnixMilli(1_700_000_000_000)
	aplica := decode.Add(time.Hour)
	agora := decode
	m.UsaRelogio(func() time.Time { return agora })

	var d Decodificado
	m.Decodifica(&d, []byte(`{"id":"a","sev":5}`))
	if d.erro != nil {
		t.Fatal(d.erro)
	}
	agora = aplica // o evento esperou na fila
	if r := m.Aplica(&d); r.Err != nil {
		t.Fatal(r.Err)
	}

	// As duas colunas de relógio: o campo de sistema e a congelada @writeTime.
	// A segunda importa tanto quanto a primeira e é mais fácil de esquecer —
	// ela sai do mesmo `agora` e por outro caminho (capturaCongeladas).
	for _, campo := range []string{schema.CampoInstanteDeEscrita, "criadoEm"} {
		v, ok := leCampo(t, c, st, "a", campo)
		if !ok {
			t.Fatalf("%s não foi materializada", campo)
		}
		if v.I64 != aplica.UnixMilli() {
			t.Errorf("%s = %d (%s), quero %d (%s): o carimbo é o do Aplica",
				campo, v.I64, time.UnixMilli(v.I64).UTC(),
				aplica.UnixMilli(), aplica.UTC())
		}
	}
}

// O decode NÃO lê o relógio. Nenhuma vez.
//
// O par deste teste é TestOInstanteEhOMesmoParaOEventoInteiro, que cobra
// exatamente uma leitura por Apply: os dois juntos dizem onde a única leitura
// mora. Sem este, alguém devolveria o `m.agora()` ao Decodifica "para o Parse
// não receber zero" e o carimbo do Aplica passaria a sobrescrever um valor já
// plausível — a regressão voltaria com a suíte verde.
func TestODecodeNaoLeORelogio(t *testing.T) {
	c, st := compiladoComExpire(t, yamlComExpire)
	m := mustNew(t, c, st)
	leituras := 0
	m.UsaRelogio(func() time.Time {
		leituras++
		return time.UnixMilli(1_700_000_000_000)
	})

	var d Decodificado
	m.Decodifica(&d, []byte(`{"id":"A","severidade":3}`))
	if d.erro != nil {
		t.Fatal(d.erro)
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
	c, st := compiladoComExpire(t, yamlComExpire)
	m := mustNew(t, c, st)

	decs := make([]Decodificado, 3)
	for i := range decs {
		m.Decodifica(&decs[i], []byte(`{"id":"A","severidade":3}`))
		if decs[i].erro != nil {
			t.Fatal(decs[i].erro)
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
	c, st := compiladoComExpire(t, yamlComExpire)
	m := mustNew(t, c, st)

	var a, b Decodificado
	m.Decodifica(&a, []byte(`{"id":"A","severidade":1}`))
	m.Decodifica(&b, []byte(`{"id":"B","severidade":2}`))
	if a.erro != nil || b.erro != nil {
		t.Fatalf("decode: %v / %v", a.erro, b.erro)
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
		v, ok := leCampo(t, c, st, key, "severidade")
		if !ok {
			t.Fatalf("%s: severidade ausente", key)
		}
		if v.I64 != quero {
			t.Errorf("%s.severidade = %d, quero %d", key, v.I64, quero)
		}
	}
}
