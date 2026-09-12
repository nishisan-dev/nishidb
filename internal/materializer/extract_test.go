package materializer

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

const extractYAML = `version: 1
name: eventos
schemaName: telecom
materialization:
  key:
    field: id
schema:
  include: ["*"]
  unknownFields: include
  fields:
    id:
      type: string
    doc:
      type: json
      extract:
        - path: probeIpNode
          as: probe_ip
          type: string
        - path: tally
          type: int64
        - path: alvo.nome
          as: alvo_nome
          type: string
`

// A chave de dentro do documento chega à coluna derivada, com o tipo
// declarado — que é o que a torna comparável e indexável.
func TestExtraiChaveDeDentroDoDocumento(t *testing.T) {
	h := newHarness(t, extractYAML)
	h.apply(`{"id":"A","doc":{"probeIpNode":"10.0.0.1","tally":7}}`)
	st := h.state("A")
	h.wantString(st, "probe_ip", "10.0.0.1")
	h.wantInt(st, "tally", 7)
}

// Extrair não é MOVER: o documento original continua inteiro na coluna dele,
// e quem já lia o documento continua lendo.
func TestODocumentoOriginalContinuaInteiro(t *testing.T) {
	h := newHarness(t, extractYAML)
	h.apply(`{"id":"A","doc":{"probeIpNode":"10.0.0.1","outra":"fica"}}`)
	wantDoc(t, h.state("A"), `{"probeIpNode":"10.0.0.1","outra":"fica"}`)
}

// Caminho de mais de um nível.
func TestExtraiCaminhoAninhado(t *testing.T) {
	h := newHarness(t, extractYAML)
	h.apply(`{"id":"A","doc":{"alvo":{"nome":"sonda-3"}}}`)
	h.wantString(h.state("A"), "alvo_nome", "sonda-3")
}

// Caminho ausente é coluna AUSENTE, não erro: o documento simplesmente não
// trouxe aquela chave, o que é o caso comum num catálogo de integração.
func TestCaminhoAusenteDeixaAColunaAusente(t *testing.T) {
	h := newHarness(t, extractYAML)
	h.apply(`{"id":"A","doc":{"outra":"coisa"}}`)
	st := h.state("A")
	h.wantAbsent(st, "probe_ip")
	h.wantAbsent(st, "tally")
}

// O PAI PODE SER UM ARRAY legitimamente — o conversor de json aceita objeto e
// array. Abrir um parser de objeto sobre ele devolveria erro de JSON
// inválido, o evento inteiro cairia como INVALID_JSON e a política
// log-and-skip o descartaria: as dezenas de colunas de TOPO iriam junto, por
// causa de uma extração que só falhou.
func TestPaiArrayNaoDerrubaOEvento(t *testing.T) {
	h := newHarness(t, extractYAML)
	r := h.apply(`{"id":"A","doc":[1,2,3]}`)
	if r.Action != ActionCreated {
		t.Fatalf("resultado = %+v, quer Created", r)
	}
	st := h.state("A")
	wantDoc(t, st, `[1,2,3]`)
	h.wantAbsent(st, "probe_ip")
}

// Pai nulo: derivadas ausentes, evento gravado.
func TestPaiNuloNaoDerrubaOEvento(t *testing.T) {
	h := newHarness(t, extractYAML)
	r := h.apply(`{"id":"A","doc":null}`)
	if r.Action != ActionCreated {
		t.Fatalf("resultado = %+v, quer Created", r)
	}
	h.wantAbsent(h.state("A"), "probe_ip")
}

// Valor que é lista não vira coluna: indexar por posição seria pobre, e
// achatar em linhas é outra feature — não há JOIN para remontar. A coluna
// fica ausente e o evento entra.
func TestValorListaDeixaAColunaAusente(t *testing.T) {
	h := newHarness(t, extractYAML)
	r := h.apply(`{"id":"A","doc":{"probeIpNode":["a","b"]}}`)
	if r.Action != ActionCreated {
		t.Fatalf("resultado = %+v, quer Created", r)
	}
	h.wantAbsent(h.state("A"), "probe_ip")
}

// Valor com tipo incompatível com o declarado: a coluna fica ausente e o
// evento entra. Derrubar o evento por causa de uma chave interna com o tipo
// errado repetiria o defeito do array acima.
func TestValorDeTipoIncompativelDeixaAColunaAusente(t *testing.T) {
	h := newHarness(t, extractYAML)
	r := h.apply(`{"id":"A","doc":{"tally":"nao-e-numero","probeIpNode":"10.0.0.1"}}`)
	if r.Action != ActionCreated {
		t.Fatalf("resultado = %+v, quer Created", r)
	}
	st := h.state("A")
	h.wantAbsent(st, "tally")
	// A outra extração do mesmo pai não é contaminada.
	h.wantString(st, "probe_ip", "10.0.0.1")
}

// Chave de TOPO homônima da coluna derivada não escreve nela: a derivada só
// pode ser preenchida pela segunda passada, senão o produtor do evento
// controlaria uma coluna que o operador declarou como sendo de dentro do
// documento.
func TestChaveDeTopoHomonimaNaoEscreveNaDerivada(t *testing.T) {
	h := newHarness(t, extractYAML)
	r := h.apply(`{"id":"A","probe_ip":"veio-de-fora","doc":{"probeIpNode":"10.0.0.1"}}`)
	if r.Action != ActionCreated {
		t.Fatalf("resultado = %+v, quer Created", r)
	}
	h.wantString(h.state("A"), "probe_ip", "10.0.0.1")
}

// Pai duplicado no evento: vale a ÚLTIMA ocorrência, que é a mesma regra que
// a coluna json do próprio pai segue. Divergir faria a derivada contradizer o
// documento do mesmo record.
func TestPaiDuplicadoUsaAUltimaOcorrencia(t *testing.T) {
	h := newHarness(t, extractYAML)
	h.apply(`{"id":"A","doc":{"probeIpNode":"primeiro"},"doc":{"probeIpNode":"ultimo"}}`)
	st := h.state("A")
	h.wantString(st, "probe_ip", "ultimo")
	wantDoc(t, st, `{"probeIpNode":"ultimo"}`)
}

// wantDoc compara a coluna json pelos BYTES: o documento é guardado verbatim,
// e wantString exigiria tipo string.
func wantDoc(t *testing.T, st map[string]types.Value, quer string) {
	t.Helper()
	v, ok := st["doc"]
	if !ok {
		t.Fatal("campo \"doc\" ausente do estado")
	}
	if string(v.Bytes) != quer {
		t.Errorf("doc = %s, quer %s", v.Bytes, quer)
	}
}

// TestPaiArrayNaoCongelaAColunaAnterior fecha a errata de 25/08 pela porta que
// ficou aberta.
//
// Aquela errata corrigiu o VALOR de tipo divergente: a segunda passada saía com
// `continue` seco e, no modo merge, campo ausente do patch preserva o valor
// anterior — a coluna respondia pelo evento passado enquanto o documento da
// mesma linha trazia o novo. A correção emitiu remoção explícita.
//
// A FORMA divergente do pai ficou com o defeito. `doc` chegando como array não
// tem objeto onde procurar, e o `continue` de então fazia exatamente o que a
// errata tinha acabado de proibir.
//
// TestPaiArrayNaoDerrubaOEvento não pegava isto porque usa entidade NOVA, e
// sem valor anterior não há o que congelar.
func TestPaiArrayNaoCongelaAColunaAnterior(t *testing.T) {
	h := newHarness(t, extractYAML)
	h.apply(`{"id":"MESMA","doc":{"probeIpNode":"10.0.0.1"}}`)
	h.wantString(h.state("MESMA"), "probe_ip", "10.0.0.1")

	h.apply(`{"id":"MESMA","doc":[1,2,3]}`)
	st := h.state("MESMA")
	h.wantAbsent(st, "probe_ip")
	wantDoc(t, st, `[1,2,3]`)
}

// O mesmo para o pai nulo, que é o outro Kind que chega até aqui.
func TestPaiNuloNaoCongelaAColunaAnterior(t *testing.T) {
	h := newHarness(t, extractYAML)
	h.apply(`{"id":"MESMA","doc":{"probeIpNode":"10.0.0.1","tally":7}}`)
	st := h.state("MESMA")
	h.wantString(st, "probe_ip", "10.0.0.1")
	h.wantInt(st, "tally", 7)

	h.apply(`{"id":"MESMA","doc":null}`)
	st = h.state("MESMA")
	h.wantAbsent(st, "probe_ip")
	h.wantAbsent(st, "tally")
}

// TestPaiArrayRemoveTambemAsColunasDESCOBERTAS é o teste que decide se a
// correção serve ao dataset em que o defeito foi medido.
//
// `pai.DerivedColumns` guarda só as extrações DECLARADAS. O piloto declara ZERO — o
// campo `persistent` tem `unknownFields: include` e mais nada —, então uma
// correção que percorresse apenas as Derivadas passaria neste arquivo inteiro e
// não removeria uma coluna sequer lá. É a diferença entre consertar o defeito e
// consertar o exemplo dele.
func TestPaiArrayRemoveTambemAsColunasDESCOBERTAS(t *testing.T) {
	h := newHarness(t, descobreYAML)
	h.apply(`{"id":"MESMA","doc":{"podName":"pod-1","tally":9}}`)
	st := h.state("MESMA")
	h.wantString(st, "doc.podName", "pod-1")
	h.wantInt(st, "doc.tally", 9)

	h.apply(`{"id":"MESMA","doc":[1,2,3]}`)
	st = h.state("MESMA")
	h.wantAbsent(st, "doc.podName")
	h.wantAbsent(st, "doc.tally")
	if got := string(st["doc"].Bytes); got != `[1,2,3]` {
		t.Errorf("doc = %s, quer o documento do evento corrente", got)
	}
}

// A divergência de FORMA conta UMA vez, no pai — não uma por coluna removida.
//
// Contar por coluna afogaria o sinal: um documento de 1.650 colunas somaria
// 1.650 num contador que existe para dizer quais colunas deixaram de responder.
// E seria afirmação errada, porque as colunas não estavam no documento — não
// havia objeto onde procurá-las. O que divergiu foi UM documento.
func TestPaiComFormaDivergenteContaUmaVezSoNoPai(t *testing.T) {
	h := newHarness(t, descobreYAML)
	h.apply(`{"id":"MESMA","doc":{"podName":"pod-1","tally":9,"outra":"x"}}`)
	before := h.c.IncompatibleValueCount()

	h.apply(`{"id":"MESMA","doc":[1,2,3]}`)
	if got := h.c.IncompatibleValueCount() - before; got != 1 {
		t.Errorf("contador subiu %d com três colunas removidas, quer 1 (uma por documento divergente)", got)
	}
}

// O mesmo para a extração DECLARADA: null explícito limpa a coluna e não conta
// como divergência de tipo.
func TestNullExplicitoNaExtracaoDeclaradaLimpaSemContar(t *testing.T) {
	h := newHarness(t, extractYAML)
	h.apply(`{"id":"MESMA","doc":{"tally":7}}`)
	h.wantInt(h.state("MESMA"), "tally", 7)
	before := h.c.IncompatibleValueCount()

	h.apply(`{"id":"MESMA","doc":{"tally":null}}`)
	h.wantAbsent(h.state("MESMA"), "tally")
	if got := h.c.IncompatibleValueCount() - before; got != 0 {
		t.Errorf("contador subiu %d com um null explícito, quer 0", got)
	}
}
