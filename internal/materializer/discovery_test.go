package materializer

import (
	"errors"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
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
// O veículo da divergência é um OBJETO, e não o booleano que ele era: boolean e
// número numa coluna de texto passaram a ser coerção sem perda, e com eles o
// teste afirmaria o contrário do que o nome promete. Objeto continua divergindo
// de `string`, que é a divergência que sobrou nesse alvo.
func TestTipoDaChaveDescobertaEFixadoNaPrimeiraObservacao(t *testing.T) {
	h := newHarness(t, descobreYAML)
	h.apply(`{"id":"A","doc":{"podName":"texto"}}`)
	r := h.apply(`{"id":"B","doc":{"podName":{"a":1}}}`)
	if r.Action != ActionCreated {
		t.Fatalf("segundo evento = %+v, quer Created", r)
	}
	h.wantString(h.state("A"), "doc.podName", "texto")
	h.wantAbsent(h.state("B"), "doc.podName")
	// ...mas o documento de B guarda o objeto.
	if got := string(h.state("B")["doc"].Bytes); got != `{"podName":{"a":1}}` {
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
	h.apply(`{"id":"B","doc":{"podName":{"a":1}}}`)
	h.apply(`{"id":"C","doc":{"podName":[1,2]}}`)
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
	h.apply(`{"id":"B","doc":{"podName":{"a":1}}}`)
	h.apply(`{"id":"C","doc":{"podName":[1,2]}}`)
	if len(avisos) != 1 {
		t.Fatalf("avisos = %v, quer exatamente um", avisos)
	}
	if avisos[0] != "doc.podName: fixado=string veio=json" {
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

// Valor de tipo divergente sobre entidade JÁ EXISTENTE não pode congelar o
// valor anterior na coluna.
//
// No modo merge — o do piloto — campo ausente do patch PRESERVA o valor
// anterior (merge.go). A segunda passada fazia `continue` seco, sem emitir
// entry nenhuma, então a coluna ficava com o valor do evento passado enquanto
// o documento da MESMA linha já trazia o novo. Coluna e documento se
// contradiziam dentro do mesmo record, e a query devolvia valor velho com cara
// de corrente — não "menos linhas do que existem", mas LINHA ERRADA.
//
// O teste anterior não pegou porque usava chaves DIFERENTES nos dois eventos,
// e assim nunca passava pelo merge.
func TestValorDivergenteNaoCongelaOAnterior(t *testing.T) {
	h := newHarness(t, descobreYAML)
	h.apply(`{"id":"MESMA","doc":{"podName":"valor-antigo"}}`)
	h.wantString(h.state("MESMA"), "doc.podName", "valor-antigo")

	h.apply(`{"id":"MESMA","doc":{"podName":{"a":1}}}`)
	st := h.state("MESMA")
	h.wantAbsent(st, "doc.podName")
	// O documento continua sendo a verdade, e traz o valor novo.
	if got := string(st["doc"].Bytes); got != `{"podName":{"a":1}}` {
		t.Errorf("doc = %s, quer o documento do evento corrente", got)
	}
}

// O mesmo para a extração DECLARADA.
func TestValorDivergenteNaoCongelaOAnteriorNaExtracaoDeclarada(t *testing.T) {
	h := newHarness(t, extractYAML)
	h.apply(`{"id":"MESMA","doc":{"tally":7}}`)
	h.wantInt(h.state("MESMA"), "tally", 7)

	h.apply(`{"id":"MESMA","doc":{"tally":"nao-e-numero"}}`)
	h.wantAbsent(h.state("MESMA"), "tally")
}

// A chave que SOME do documento é outro caso, e continua seguindo o merge: a
// coluna preserva o último valor visto. Está travado aqui porque é semântica
// declarada, não descuido — corrigi-la exigiria emitir uma remoção por coluna
// conhecida e ausente, o que num documento de 11 chaves sobre 1.650 colunas
// seriam 1.639 entries por evento.
func TestChaveQueSomeDoDocumentoSegueOMerge(t *testing.T) {
	h := newHarness(t, descobreYAML)
	h.apply(`{"id":"MESMA","doc":{"podName":"visto"}}`)
	h.apply(`{"id":"MESMA","doc":{"outra":"coisa"}}`)
	st := h.state("MESMA")
	h.wantString(st, "doc.podName", "visto")
	if got := string(st["doc"].Bytes); got != `{"outra":"coisa"}` {
		t.Errorf("doc = %s", got)
	}
}

// TestFalhaDeIODoDicionarioNoCaminhoANINHADOEhFailed é o irmão de
// TestFalhaDeIODoDicionarioAoFixarTipoEhFailed, e a existência do irmão é o
// ponto: a doutrina dos três baldes valia no caminho de TOPO e não valia aqui.
//
// A descoberta fixa o tipo da coluna aninhada pelo mesmo FixAutoType, e o
// `continue` que estava neste ponto tratava disco cheio como "a coluna não
// existe neste evento": sem erro, sem log, sem métrica, com Failed=0 no painel.
// Pior no modo merge, onde a coluna ainda ficava congelada no valor anterior.
func TestFalhaDeIODoDicionarioNoCaminhoANINHADOEhFailed(t *testing.T) {
	s, err := schema.Parse([]byte(descobreYAML), schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	eio := errors.New(`fielddict: fsync do tipo de "doc.podName": input/output error`)
	c, err := s.Compile(dictFalhaAoFixar{inner: fakestorage.NewDict(), err: eio})
	if err != nil {
		t.Fatal(err)
	}
	m := mustNew(t, c, fakestorage.New(fakestorage.Indexes{}))

	r := m.Apply([]byte(`{"id":"A","doc":{"podName":"pod-1"}}`))

	if r.Action != ActionFailed {
		t.Fatalf("Action = %v (%v), want Failed: falha do dicionário não é evento ruim", r.Action, r.Err)
	}
	if r.Category != CategoryStorage {
		t.Errorf("Category = %v, want STORAGE_ERROR", r.Category)
	}
	if errors.Is(r.Err, ErrTypeConversion) {
		t.Errorf("erro = %v, want SEM ErrTypeConversion: é infra, não conversão", r.Err)
	}
	if !errors.Is(r.Err, eio) {
		t.Errorf("erro = %v, want preservar o erro do dicionário", r.Err)
	}
	if mt := m.Metrics(); mt.InvalidTotal != 0 || mt.FailedTotal != 1 {
		t.Errorf("métricas = %+v, want InvalidTotal=0 FailedTotal=1", mt)
	}
}

// CONFLITO de tipo no dicionário, no caminho aninhado, continua sendo DADO: o
// campo mudou de formato na origem.
//
// A diferença em relação ao caminho de topo é deliberada e é a regra desta
// passada: lá o conflito derruba o EVENTO (TYPE_CONVERSION), aqui ele derruba
// só a COLUNA. Derrubar o evento inteiro por causa de uma chave de dentro de um
// documento levaria junto as dezenas de colunas de topo, que é exatamente o que
// a segunda passada existe para não fazer.
func TestConflitoDeTipoNoCaminhoAninhadoRemoveAColunaSemDerrubarOEvento(t *testing.T) {
	h := newHarness(t, descobreYAML)
	h.apply(`{"id":"A","doc":{"podName":"texto"}}`)
	antes := h.c.ValoresIncompativeis()

	// O tipo já está fixado como string; um objeto no mesmo caminho diverge.
	r := h.m.Apply([]byte(`{"id":"A","doc":{"podName":{"a":1}}}`))
	if r.Err != nil {
		t.Fatalf("o evento caiu por causa de uma chave interna: %v", r.Err)
	}
	h.wantAbsent(h.state("A"), "doc.podName")
	if got := h.c.ValoresIncompativeis() - antes; got != 1 {
		t.Errorf("contador subiu %d, quer 1", got)
	}
}

// TestNullExplicitoNaoContaComoTipoIncompativel separa duas coisas que caíam no
// mesmo contador.
//
// Convert devolve Value{Type: TypeNull} SEM erro para o null do JSON, e a
// mesma condição que pega "o valor não coube no tipo declarado" pegava também
// "a origem mandou null". Não são a mesma coisa: null é a origem dizendo que
// não há valor, e a coluna ficar ausente é a resposta certa.
//
// Isso importa porque o contador é o número que decide quais chaves ganham tipo
// declarado. Inflado por null, ele aponta para colunas cujo problema não é o
// tipo — e a decisão sai errada exatamente onde deveria sair de medição.
func TestNullExplicitoNaoContaComoTipoIncompativel(t *testing.T) {
	h := newHarness(t, descobreYAML)
	h.apply(`{"id":"A","doc":{"podName":"pod-1"}}`)
	antes := h.c.ValoresIncompativeis()

	h.apply(`{"id":"A","doc":{"podName":null}}`)
	if got := h.c.ValoresIncompativeis() - antes; got != 0 {
		t.Errorf("contador subiu %d com um null explícito, quer 0", got)
	}

	// E a divergência de verdade continua contando, no mesmo dataset e na
	// mesma coluna — sem isto o teste acima passaria com o contador quebrado.
	antes = h.c.ValoresIncompativeis()
	h.apply(`{"id":"A","doc":{"podName":{"a":1}}}`)
	if got := h.c.ValoresIncompativeis() - antes; got != 1 {
		t.Errorf("contador subiu %d com tipo divergente, quer 1", got)
	}
}

// O null explícito CONTINUA limpando a coluna. É a metade que não pode mudar:
// sem a remoção, no modo merge a coluna seguiria mostrando o valor de um evento
// anterior enquanto o documento da mesma linha diz null.
func TestNullExplicitoAindaLimpaAColuna(t *testing.T) {
	h := newHarness(t, descobreYAML)
	h.apply(`{"id":"MESMA","doc":{"podName":"pod-1"}}`)
	h.wantString(h.state("MESMA"), "doc.podName", "pod-1")

	h.apply(`{"id":"MESMA","doc":{"podName":null}}`)
	h.wantAbsent(h.state("MESMA"), "doc.podName")
}
