package materializer

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

const descobreYAML = `version: 1
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
      unknownFields: include
      extract:
        - path: probeIpNode
          as: probe_ip
          type: string
`

// A motivação do campo json é preservar TUDO para estudo e debug depois. A
// descoberta acrescenta a isso o poder de ESCREVER a pergunta: sem coluna,
// `WHERE doc.algo = 'x'` não é sequer expressável.
func TestDescobreChaveDeDentroDoDocumento(t *testing.T) {
	h := newHarness(t, descobreYAML)
	h.apply(`{"id":"A","doc":{"nativeProbableCause":"link-down","perceivedSeverity":3}}`)
	st := h.state("A")
	h.wantString(st, "doc.nativeProbableCause", "link-down")
	h.wantInt(st, "doc.perceivedSeverity", 3)
}

// O nome é QUALIFICADO porque é único por construção: uma chave interna
// chamada `id` não pode colidir com a key do dataset.
func TestChaveInternaHomonimaNaoColideComCampoDeTopo(t *testing.T) {
	h := newHarness(t, descobreYAML)
	h.apply(`{"id":"A","doc":{"id":"outro-id"}}`)
	st := h.state("A")
	h.wantString(st, "id", "A")
	h.wantString(st, "doc.id", "outro-id")
}

// Declaração VENCE descoberta: quem declarou `probeIpNode` como `probe_ip`
// escolheu o nome e o tipo, e a descoberta não pode criar uma segunda coluna
// para o mesmo caminho.
func TestDeclaracaoVenceDescoberta(t *testing.T) {
	h := newHarness(t, descobreYAML)
	h.apply(`{"id":"A","doc":{"probeIpNode":"10.0.0.1"}}`)
	st := h.state("A")
	h.wantString(st, "probe_ip", "10.0.0.1")
	h.wantAbsent(st, "doc.probeIpNode")
}

// A descoberta é de UM nível: objeto aninhado vira coluna json, inteiro. Quem
// precisa da chave lá dentro DECLARA o caminho. É o que impede o vocabulário
// de crescer por profundidade além de crescer por largura.
func TestDescobertaEDeUmNivelSo(t *testing.T) {
	h := newHarness(t, descobreYAML)
	h.apply(`{"id":"A","doc":{"alarme":{"id":"AL-1","grau":2}}}`)
	st := h.state("A")
	v, ok := st["doc.alarme"]
	if !ok {
		t.Fatal("doc.alarme ausente")
	}
	if v.Type != types.TypeJSON {
		t.Errorf("doc.alarme.Type = %v, quer json", v.Type)
	}
	if string(v.Bytes) != `{"id":"AL-1","grau":2}` {
		t.Errorf("doc.alarme = %s, quer o objeto inteiro", v.Bytes)
	}
	h.wantAbsent(st, "doc.alarme.id")
}

// O documento continua inteiro na coluna dele. Guardar os dois é decisão
// consciente: a coluna dá a pergunta, o documento dá a forma original —
// aninhamento, listas e ordem.
func TestODocumentoContinuaInteiroComDescoberta(t *testing.T) {
	h := newHarness(t, descobreYAML)
	h.apply(`{"id":"A","doc":{"a":1,"b":"x"}}`)
	st := h.state("A")
	if got := string(st["doc"].Bytes); got != `{"a":1,"b":"x"}` {
		t.Errorf("doc = %s, quer o documento original", got)
	}
	h.wantInt(st, "doc.a", 1)
}

// O tipo de uma chave descoberta é fixado na PRIMEIRA observação, como o de
// qualquer campo autodescoberto. Valor divergente depois vira coluna ausente —
// e o evento entra, porque o documento continua guardando a verdade.
func TestTipoDaChaveDescobertaEFixadoNaPrimeiraObservacao(t *testing.T) {
	h := newHarness(t, descobreYAML)
	h.apply(`{"id":"A","doc":{"podName":"texto"}}`)
	r := h.apply(`{"id":"B","doc":{"podName":true}}`)
	if r.Action != ActionCreated {
		t.Fatalf("segundo evento = %+v, quer Created", r)
	}
	h.wantString(h.state("A"), "doc.podName", "texto")
	h.wantAbsent(h.state("B"), "doc.podName")
	// ...mas o documento de B guarda o booleano.
	if got := string(h.state("B")["doc"].Bytes); got != `{"podName":true}` {
		t.Errorf("doc de B = %s; o documento tem de preservar o valor divergente", got)
	}
}

// Lista vira coluna json, preservada e projetável — não achatada em linhas.
func TestListaDescobertaViraColunaJson(t *testing.T) {
	h := newHarness(t, descobreYAML)
	h.apply(`{"id":"A","doc":{"aliasValue":["a","b"]}}`)
	v := h.state("A")["doc.aliasValue"]
	if v.Type != types.TypeJSON || string(v.Bytes) != `["a","b"]` {
		t.Errorf("doc.aliasValue = %+v, quer a lista como json", v)
	}
}

// Descoberta SEM nenhum extract declarado — o caso puro, e o que o piloto vai
// usar. A primeira versão desta feature só registrava o pai para a segunda
// passada quando ele tinha extração declarada, então a descoberta sozinha
// nunca rodava. O YAML dos testes acima traz um `extract` junto e mascarava o
// defeito; foi a verificação com o binário que o pegou.
const soDescobreYAML = `version: 1
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
      unknownFields: include
`

func TestDescobertaSemNenhumExtractDeclarado(t *testing.T) {
	h := newHarness(t, soDescobreYAML)
	h.apply(`{"id":"A","doc":{"tlCause":"LOS","perceivedSeverity":3}}`)
	st := h.state("A")
	h.wantString(st, "doc.tlCause", "LOS")
	h.wantInt(st, "doc.perceivedSeverity", 3)
}

// Valor que não cabe no tipo FIXADO precisa ser contado.
//
// O evento entra — isso é deliberado e certo, uma chave interna não derruba o
// resto. Mas a coluna fica nula naquela linha, e um filtro sobre ela passa a
// devolver menos linhas do que existem. Sem contador ninguém descobre: o
// `descartados` do painel conta EVENTOS, e o de campos conta o que o TETO
// recusou. A falha de conversão não tocava nenhum dos dois.
//
// Isso morde mais na descoberta do que na extração declarada, porque ali o
// tipo é decidido por qual evento chegou primeiro — não por quem escreveu o
// schema.
func TestValorIncompativelEContado(t *testing.T) {
	h := newHarness(t, descobreYAML)
	h.apply(`{"id":"A","doc":{"podName":"texto"}}`)
	if n := h.c.ValoresIncompativeis(); n != 0 {
		t.Fatalf("contador nasceu em %d", n)
	}
	h.apply(`{"id":"B","doc":{"podName":true}}`)
	h.apply(`{"id":"C","doc":{"podName":42}}`)
	if n := h.c.ValoresIncompativeis(); n != 2 {
		t.Errorf("ValoresIncompativeis = %d, quer 2", n)
	}
}

// O aviso sai uma vez, com o nome da coluna e os DOIS tipos: o fixado e o que
// chegou. Sem os dois o operador sabe que algo divergiu e não sabe o quê.
func TestValorIncompativelAvisaUmaVezComOsDoisTipos(t *testing.T) {
	h := newHarness(t, descobreYAML)
	var avisos []string
	h.c.AoValorIncompativel(func(coluna, fixado, veio string) {
		avisos = append(avisos, coluna+": fixado="+fixado+" veio="+veio)
	})
	h.apply(`{"id":"A","doc":{"podName":"texto"}}`)
	h.apply(`{"id":"B","doc":{"podName":true}}`)
	h.apply(`{"id":"C","doc":{"podName":42}}`)
	if len(avisos) != 1 {
		t.Fatalf("avisos = %v, quer exatamente um", avisos)
	}
	if avisos[0] != "doc.podName: fixado=string veio=boolean" {
		t.Errorf("aviso = %q", avisos[0])
	}
}

// Também na extração DECLARADA: lá o tipo foi escolhido por quem escreveu o
// schema, mas a origem pode mudar de forma do mesmo jeito.
func TestValorIncompativelEContadoNaExtracaoDeclarada(t *testing.T) {
	h := newHarness(t, extractYAML)
	h.apply(`{"id":"A","doc":{"tally":"nao-e-numero"}}`)
	if n := h.c.ValoresIncompativeis(); n != 1 {
		t.Errorf("ValoresIncompativeis = %d, quer 1", n)
	}
}
