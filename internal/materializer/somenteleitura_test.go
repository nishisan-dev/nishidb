package materializer

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// O decode não cria coluna. É a invariante inteira da Fase 4.2.
//
// LookupOrCreate atribui o FieldID na ordem de chamada, o FieldID é formato de
// disco e a posição da coluna no `SELECT *` é contrato do driver JDBC. Com N
// decodificadores, a ordem viraria a de quem ganha o mutex — a violação que
// TestDatasetsDoMesmoYAMLTemAMesmaOrdemDeColunas (internal/cli) fixa e que o
// comentário dele registra ter acontecido em produção.
//
// O teste decodifica sem aplicar e cobra as duas metades: o dicionário não
// cresceu, e o evento voltou marcado como pendente do writer.
func TestODecodeNaoCriaColuna(t *testing.T) {
	h := newHarness(t, descobreYAML)
	antes := h.c.CamposConhecidos()

	var d Decodificado
	h.m.Decodifica(&d, []byte(`{"id":"A","doc":{"chaveNova":1}}`))

	if got := h.c.CamposConhecidos(); got != antes {
		t.Errorf("o decode criou coluna: %d → %d", antes, got)
	}
	if !d.precisaDoWriter {
		t.Error("o decode não marcou o evento como pendente do writer")
	}
	if d.erro != nil {
		t.Errorf("a pendência virou erro de evento: %v", d.erro)
	}
	if _, ok := h.c.FieldByName([]byte("doc.chaveNova")); ok {
		t.Error("a coluna existe no schema depois de um decode somente-leitura")
	}

	// E a aplicação resolve: o writer refaz o evento pelo caminho completo.
	if r := h.m.Aplica(&d); r.Err != nil {
		t.Fatalf("Aplica: %v", r.Err)
	}
	if _, ok := h.c.FieldByName([]byte("doc.chaveNova")); !ok {
		t.Error("o writer não criou a coluna ao reprocessar")
	}
	h.wantInt(h.state("A"), "doc.chaveNova", 1)
}

// O tipo de uma coluna auto é o do MENOR offset, não o de quem chegou primeiro
// ao lock.
//
// Este é o teste que reprova quem "otimizar" a descoberta para dentro dos
// workers. A ordem de decodificação é INVERTIDA de propósito — o segundo evento
// é decodificado antes do primeiro, que é exatamente o que acontece quando um
// worker termina antes do outro —, e a aplicação segue a ordem de offset. Com o
// decode somente-leitura os dois voltam pendentes e quem fixa o tipo é o
// primeiro a ser APLICADO. Sem ele, o tipo sai do primeiro a ser DECODIFICADO,
// e a coluna do dataset passa a depender de escalonamento.
func TestOTipoSaiDoMenorOffsetEnaoDeQuemDecodificouPrimeiro(t *testing.T) {
	h := newHarness(t, descobreYAML)

	// offset N traz int, N+1 traz string. O decode acontece na ordem contrária.
	evN := []byte(`{"id":"A","doc":{"quantidade":7}}`)
	evN1 := []byte(`{"id":"B","doc":{"quantidade":"texto"}}`)

	var dN, dN1 Decodificado
	h.m.Decodifica(&dN1, evN1) // o worker do offset MAIOR terminou antes
	h.m.Decodifica(&dN, evN)

	if r := h.m.Aplica(&dN); r.Err != nil {
		t.Fatalf("Aplica(N): %v", r.Err)
	}
	if r := h.m.Aplica(&dN1); r.Err != nil {
		t.Fatalf("Aplica(N+1): %v", r.Err)
	}

	f, ok := h.c.FieldByName([]byte("doc.quantidade"))
	if !ok {
		t.Fatal("a coluna não foi criada")
	}
	if got := f.EffectiveType(); got != types.TypeInt64 {
		t.Errorf("tipo fixado = %v, quero int64 — o tipo é o do MENOR offset", got)
	}
}

// O evento reprocessado NÃO conta duas vezes.
//
// `_meta.fields` publica ocorrências, divergências e coerções por coluna, e a
// leitura que o operador faz com elas é uma RAZÃO — "2.315 de 2.383". O decode
// somente-leitura percorre parte do documento antes de parar na chave que teria
// de criar, e o writer refaz o evento inteiro: contar dentro do Parse dobraria
// o numerador de toda chave que veio ANTES da nova, e a decisão de qual coluna
// ganha tipo declarado sairia errada exatamente onde ela deveria sair de
// medição.
//
// O segundo evento é o que expõe o defeito: ele traz uma chave que já existe
// (`x`, contada pelo decode) e uma que não (`y`, que aborta o decode).
func TestOEventoReprocessadoNaoContaDuasVezes(t *testing.T) {
	h := newHarness(t, descobreYAML)
	h.apply(`{"id":"a","doc":{"x":1}}`)       // cria doc.x
	h.apply(`{"id":"b","doc":{"x":2,"y":3}}`) // conta x, aborta em y, reprocessa

	f, ok := h.c.FieldByName([]byte("doc.x"))
	if !ok {
		t.Fatal("doc.x não foi criada")
	}
	if got := f.Ocorrencias(); got != 2 {
		t.Errorf("ocorrências de doc.x = %d, quero 2 (uma por evento); "+
			"3 significa que o decode abortado contou junto com o reprocesso", got)
	}
	g, ok := h.c.FieldByName([]byte("doc.y"))
	if !ok {
		t.Fatal("doc.y não foi criada")
	}
	if got := g.Ocorrencias(); got != 1 {
		t.Errorf("ocorrências de doc.y = %d, quero 1", got)
	}
}

// O evento RUIM conta o que o Parse alcançou antes de falhar.
//
// Sempre foi assim — os contadores por coluna eram incrementados dentro do
// Parse, então uma chave vista antes do erro entrava na conta —, e a mudança
// para contagem adiada podia alterar isso sem que ninguém percebesse: bastava
// executar os incrementos depois do desvio do evento ruim. O denominador de
// `_meta.fields` mudaria em silêncio, e "2.315 de 2.383" passaria a comparar
// duas populações diferentes.
//
// O evento sem `id` é o veículo: ele percorre o documento inteiro — contando a
// chave — e só falha no fim, na checagem de key.
func TestEventoRuimContaOQueOParseAlcancou(t *testing.T) {
	h := newHarness(t, descobreYAML)
	h.apply(`{"id":"a","doc":{"x":1}}`)

	f, ok := h.c.FieldByName([]byte("doc.x"))
	if !ok {
		t.Fatal("doc.x não foi criada")
	}
	antes := f.Ocorrencias()

	r := h.m.Apply([]byte(`{"doc":{"x":2}}`)) // sem `id`: INVALID_KEY no fim do Parse
	if r.Category != CategoryInvalidKey {
		t.Fatalf("categoria = %v, quero INVALID_KEY", r.Category)
	}
	if got := f.Ocorrencias() - antes; got != 1 {
		t.Errorf("ocorrências de doc.x no evento ruim = %d, quero 1: a chave "+
			"APARECEU no documento, e o contador sempre contou a aparição", got)
	}
}

// O contador de decode serial bate com o número de eventos de DESCOBERTA, e
// com mais nada.
//
// É a medida do que a Fase 4.2 custa. Num corpus de contagem conhecida, só os
// eventos que trazem metadado novo pagam o parse dobrado; os demais atravessam
// o decode somente-leitura inteiro. Um contador que subisse com todo evento
// significaria que o decodificador está pedindo o writer por algo que não é
// descoberta — e o custo da fase seria o dobro do caminho quente, não zero.
func TestDecodeSerialContaSoOsEventosDeDescoberta(t *testing.T) {
	h := newHarness(t, descobreYAML)
	corpus := []struct {
		evento     string
		descobriu  bool
		porqueNota string
	}{
		{`{"id":"a","doc":{"x":1}}`, true, "cria doc.x e fixa o tipo"},
		{`{"id":"b","doc":{"x":2}}`, false, "doc.x já existe e já tem tipo"},
		{`{"id":"c","doc":{"x":3,"y":4}}`, true, "cria doc.y"},
		{`{"id":"d","doc":{"x":5,"y":6}}`, false, "nada novo"},
		{`{"id":"e","topoNovo":"z"}`, true, "campo de topo desconhecido"},
		{`{"id":"f","topoNovo":"w"}`, false, "topoNovo já existe"},
	}
	var querSerial int64
	for _, c := range corpus {
		antes := h.m.Metrics().DecodeSerialTotal
		h.apply(c.evento)
		depois := h.m.Metrics().DecodeSerialTotal
		if c.descobriu {
			querSerial++
		}
		if subiu := depois > antes; subiu != c.descobriu {
			t.Errorf("%s (%s): reprocessou=%v, quero %v",
				c.evento, c.porqueNota, subiu, c.descobriu)
		}
	}
	if got := h.m.Metrics().DecodeSerialTotal; got != querSerial {
		t.Errorf("DecodeSerialTotal = %d, quero %d — um por evento de descoberta",
			got, querSerial)
	}
	if got := h.m.Metrics().EventsTotal; got != int64(len(corpus)) {
		t.Errorf("EventsTotal = %d, quero %d: o reprocesso não pode contar evento a mais",
			got, len(corpus))
	}
}

// O reprocesso improdutivo avisa, uma vez.
//
// O caso conhecido é o dicionário no teto sob `onMaxCols: ignore`: o campo
// nunca vira coluna, então todo evento que o traga volta a ser reprocessado
// para sempre. É a degradação nomeada no plano — nunca pior que a vazão de
// antes, mas permanente —, e sem aviso ela é indistinguível de operação normal.
func TestReprocessoImprodutivoAvisaUmaVez(t *testing.T) {
	h := newHarness(t, descobreYAML)
	var avisos int
	var seguidos int64
	h.m.AoReprocessar(func(n int64, _ int) { avisos++; seguidos = n })

	// Uma coluna nova zera a contagem, e é isso que separa descoberta em curso
	// de reprocesso que não produz nada.
	h.apply(`{"id":"a","doc":{"x":1}}`)
	for i := 0; i < reprocessosInuteisParaAvisar-1; i++ {
		h.m.avaliaReprocessoInutil(false)
	}
	if avisos != 0 {
		t.Fatalf("avisou com %d reprocessos improdutivos, antes do limiar", reprocessosInuteisParaAvisar-1)
	}
	h.m.avaliaReprocessoInutil(false)
	if avisos != 1 {
		t.Fatalf("avisos = %d no limiar, quero 1", avisos)
	}
	if seguidos != reprocessosInuteisParaAvisar {
		t.Errorf("o aviso reportou %d seguidos, quero %d", seguidos, reprocessosInuteisParaAvisar)
	}
	for i := 0; i < reprocessosInuteisParaAvisar; i++ {
		h.m.avaliaReprocessoInutil(false)
	}
	if avisos != 1 {
		t.Errorf("avisos = %d; o aviso é de uma vez só", avisos)
	}
}
