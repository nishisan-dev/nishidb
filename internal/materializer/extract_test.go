package materializer

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

const extractYAML = `version: 1
name: eventos
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
