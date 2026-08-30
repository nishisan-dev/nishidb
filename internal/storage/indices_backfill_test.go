package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// abreStoreIndexadoEm abre — ou REABRE — o dataset materializado em dir com o
// conjunto indexado declarado.
//
// Receber o diretório é o que permite fechar e reabrir o MESMO dataset dentro de
// um teste com OUTRO conjunto de campos, que é o ato inteiro do débito 22:
// acrescentar `index: true` a um campo de um dataset povoado, ou tirá-lo.
func abreStoreIndexadoEm(t *testing.T, dir string, campos []string, segSize int64) *Store {
	t.Helper()
	return openStore(t, Options{
		Dir:                    dir,
		SegmentSize:            segSize,
		CamposIndexadosPorNome: campos,
	})
}

// rodaReconstrucaoAteEsvaziar drena a fila do jeito que o laço de fundo (§2.4)
// vai drenar: pede os pendentes, reconstrói cada um e pergunta de novo.
//
// O teto de passadas não é decoração. A fila é um SUPERCONJUNTO do trabalho real
// — um segmento cujos records não trazem um campo declarado continua pendente
// depois de reconstruído, porque a seção que provaria o contrário não existe —,
// então um laço sem teto num caso desses roda para sempre. Aqui ele vira falha
// nomeada, que é o que separa "não esvaziou" de "o teste travou".
func rodaReconstrucaoAteEsvaziar(t *testing.T, s *Store) {
	t.Helper()
	for range 10 {
		pendentes, err := s.SegmentosPendentesDeReconstrucao(JanelaDeIndice{})
		if err != nil {
			t.Fatalf("SegmentosPendentesDeReconstrucao: %v", err)
		}
		if len(pendentes) == 0 {
			return
		}
		for _, seg := range pendentes {
			if err := s.ConstroiIndiceDoSegmento(t.Context(), seg, s.CamposIndexados()); err != nil {
				t.Fatalf("ConstroiIndiceDoSegmento(%d): %v", seg, err)
			}
		}
	}
	t.Fatal("a fila de reconstrução não esvaziou em 10 passadas")
}

// camposDoSelado lê os campos com seção no .sidx do segmento. É o oráculo por
// (segmento, campo): "tem arquivo?" deixou de ser a pergunta.
func camposDoSelado(t *testing.T, dir string, seg uint32) []types.FieldID {
	t.Helper()
	campos, err := segindex.CamposSelados(segindex.CaminhoSelado(filepath.Join(dir, "data"), seg))
	if err != nil {
		t.Fatalf("CamposSelados(%d): %v", seg, err)
	}
	return campos
}

// TestAcrescentarIndiceEmDatasetPovoadoAgendaReconstrucao é a metade do débito 22
// que esta tarefa entrega, de ponta a ponta: a recusa existia porque não havia
// backfill, e a reconstrução da §2.4 É o backfill — mesmo código, gatilho
// diferente. Manter a recusa seria ter a capacidade e negar o uso dela.
//
// Quem drena a fila aqui é o TESTE (rodaReconstrucaoAteEsvaziar), e não o
// produto: o laço de fundo é da §2.4 e ainda não existe, que é o que mantém o
// débito 22 aberto. O que este teste prova é que a fila agenda o segmento certo e
// que a reconstrução dela serve a consulta.
//
// O oráculo é o conjunto CRU de candidatos para um valor que nunca existiu:
// segmento que responde por índice devolve zero, segmento que enumera devolve as
// linhas dele. É ele que distingue "respondeu certo" de "respondeu certo
// varrendo o dataset inteiro", que é a diferença que este débito era.
func TestAcrescentarIndiceEmDatasetPovoadoAgendaReconstrucao(t *testing.T) {
	dir := t.TempDir()
	s := abreStoreIndexadoEm(t, dir, nil, 64<<10) // sem campo indexado nenhum
	alvo := s.curSegID.Load()
	poeLinhasAteRotacionar(t, s, alvo)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2 := abreStoreIndexadoEm(t, dir, []string{"node"}, 64<<10) // acrescenta agora
	campo := campoDoDicionario(t, s2, "node")
	pendentes, err := s2.SegmentosPendentesDeReconstrucao(JanelaDeIndice{})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(pendentes, alvo) {
		t.Fatalf("SegmentosPendentesDeReconstrucao = %v, quero o segmento %d: acrescentar índice num dataset povoado não agendou a reconstrução",
			pendentes, alvo)
	}
	if antes := candidatosCrus(t, s2, campo, strVal("valor-que-nunca-existiu")); len(antes) == 0 {
		t.Fatal("pré-condição: o segmento povoado já respondia por índice, e o teste não mede o backfill")
	}

	rodaReconstrucaoAteEsvaziar(t, s2)

	if depois := candidatosCrus(t, s2, campo, strVal("valor-que-nunca-existiu")); len(depois) != 0 {
		t.Errorf("depois do backfill o Lookup ainda enumerou %d candidatos: a reconstrução não passou a servir a consulta", len(depois))
	}
	// E o índice reconstruído tem de ACHAR o que está lá, não só parar de
	// enumerar: um .sidx vazio faria o teste acima passar pelo motivo errado.
	if got := lookupRevalidado(t, s2, campo, strVal("n1")); len(got) == 0 {
		t.Error("o backfill não populou o índice: o Lookup de um valor existente devolveu vazio")
	}
}

// TestSegmentoSeladoSemASecaoDoCampoNovoEntraNaFila é o coração da tarefa. O
// teste de pendência decidia por "tem .sidx?", e um segmento selado ANTES de o
// campo ser declarado tem o arquivo e não tem a seção dele — então ele não
// entrava na fila e o campo novo enumerava para sempre. A pendência passa a ser
// por (segmento, campo).
//
// A CONSULTA DE PRÉ-CONDIÇÃO NÃO É DECORAÇÃO, e é ela que faz este teste guardar
// duas coisas: ela aquece o cache de selados com o mapeamento do arquivo ANTIGO,
// e o mmap sobrevive ao rename da reescrita. Sem a evição que
// ConstroiIndiceDoSegmento faz, a asserção final falha com o segmento ainda
// enumerando — a reconstrução teria acontecido em disco e não na resposta. Um
// teste que consultasse só DEPOIS passaria sem exercer isso.
func TestSegmentoSeladoSemASecaoDoCampoNovoEntraNaFila(t *testing.T) {
	dir := t.TempDir()
	s := abreStoreIndexadoEm(t, dir, []string{"node"}, 64<<10)
	alvo := s.curSegID.Load()
	poeLinhasAteRotacionar(t, s, alvo)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// A pré-condição é justamente o estado que o teste antigo declarava PRONTO:
	// o segmento TEM .sidx, e ele cobre só o campo que era indexado quando o
	// segmento foi selado.
	if campos := camposDoSelado(t, dir, alvo); len(campos) != 1 {
		t.Fatalf("o .sidx do segmento %d tem %v; a pré-condição é uma seção só", alvo, campos)
	}

	s2 := abreStoreIndexadoEm(t, dir, []string{"node", "type"}, 64<<10)
	campoType := campoDoDicionario(t, s2, "type")
	inexistente := types.Value{Type: types.TypeInt64, I64: 99}

	pendentes, err := s2.SegmentosPendentesDeReconstrucao(JanelaDeIndice{})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(pendentes, alvo) {
		t.Fatalf("SegmentosPendentesDeReconstrucao = %v, quero o segmento %d: um .sidx sem a seção do campo declarado não entrou na fila",
			pendentes, alvo)
	}
	if antes := candidatosCrus(t, s2, campoType, inexistente); len(antes) == 0 {
		t.Fatal("pré-condição: o campo novo já era respondido por índice, e o teste não mede a reconstrução")
	}

	rodaReconstrucaoAteEsvaziar(t, s2)

	if campos := camposDoSelado(t, dir, alvo); len(campos) != 2 {
		t.Errorf("o .sidx do segmento %d ficou com %v; quero as duas seções", alvo, campos)
	}
	if depois := candidatosCrus(t, s2, campoType, inexistente); len(depois) != 0 {
		t.Errorf("depois da reconstrução o campo novo ainda enumerou %d candidatos", len(depois))
	}
}

// TestReconstruirSoOsCamposQueFaltamPreservaOsQueJaEstavam trava a metade da
// recusa afrouxada que não pode ser esquecida: o .sidx é IMUTÁVEL, então
// acrescentar uma seção é reescrever o arquivo inteiro — e um arquivo reescrito
// só com o campo pedido APAGARIA a seção que já estava lá. O falso negativo
// petrificado voltaria pela porta oposta, agora oscilando entre os dois campos a
// cada passada do laço.
//
// A segunda metade é o golden: reconstruir só o que falta tem de dar o MESMO
// arquivo que reconstruir tudo. Sem essa igualdade, "incremental idêntico a
// reconstruído" (§6.3) deixaria de valer para o caminho novo.
func TestReconstruirSoOsCamposQueFaltamPreservaOsQueJaEstavam(t *testing.T) {
	dir := t.TempDir()
	s := abreStoreIndexadoEm(t, dir, []string{"node"}, 64<<10)
	alvo := s.curSegID.Load()
	poeLinhasAteRotacionar(t, s, alvo)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2 := abreStoreIndexadoEm(t, dir, []string{"node", "type"}, 64<<10)
	campoNode := campoDoDicionario(t, s2, "node")
	campoType := campoDoDicionario(t, s2, "type")

	if err := s2.ConstroiIndiceDoSegmento(t.Context(), alvo, []types.FieldID{campoType}); err != nil {
		t.Fatalf("ConstroiIndiceDoSegmento(%d, só o campo que falta): %v", alvo, err)
	}
	esperado := []types.FieldID{campoNode, campoType}
	slices.Sort(esperado)
	if campos := camposDoSelado(t, dir, alvo); !slices.Equal(campos, esperado) {
		t.Fatalf("o .sidx ficou com %v, quero %v: reconstruir um campo apagou o outro", campos, esperado)
	}

	caminho := segindex.CaminhoSelado(filepath.Join(dir, "data"), alvo)
	porFaltante, err := os.ReadFile(caminho)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(caminho); err != nil {
		t.Fatal(err)
	}
	if err := s2.ConstroiIndiceDoSegmento(t.Context(), alvo, s2.CamposIndexados()); err != nil {
		t.Fatalf("ConstroiIndiceDoSegmento(%d, tudo): %v", alvo, err)
	}
	porInteiro, err := os.ReadFile(caminho)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(porFaltante, porInteiro) {
		t.Errorf("reconstruir só o campo que falta deu %d bytes e reconstruir tudo deu %d: os dois caminhos têm de publicar o mesmo arquivo",
			len(porFaltante), len(porInteiro))
	}
}

// TestRemoverIndiceApagaAsSecoes: tirar `index: true` de um campo é parar de
// escrever a seção E apagar as que existem. Os dois opt-out (AllowBitmapDrop,
// AllowHashDrop) existiam para tornar deliberada uma perda que agora não
// acontece — o dado nunca esteve no índice, e o índice é derivado.
//
// A seção que sobra não responde errado (o Lookup recusa campo não declarado
// antes de chegar ao arquivo), mas ocupa disco que o IndexBytes conta e que nada
// no produto explica — o mesmo modo de falha que o .widx órfão obrigou a fechar.
func TestRemoverIndiceApagaAsSecoes(t *testing.T) {
	dir := t.TempDir()
	s := abreStoreIndexadoEm(t, dir, []string{"node", "type"}, 64<<10)
	alvo := s.curSegID.Load()
	poeLinhasAteRotacionar(t, s, alvo)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if campos := camposDoSelado(t, dir, alvo); len(campos) != 2 {
		t.Fatalf("o .sidx do segmento %d tem %v; a pré-condição é as duas seções", alvo, campos)
	}

	s2 := abreStoreIndexadoEm(t, dir, []string{"node"}, 64<<10)
	campoNode := campoDoDicionario(t, s2, "node")

	pendentes, err := s2.SegmentosPendentesDeReconstrucao(JanelaDeIndice{})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(pendentes, alvo) {
		t.Fatalf("SegmentosPendentesDeReconstrucao = %v, quero o segmento %d: a seção do campo que deixou de ser indexado não agendou nada",
			pendentes, alvo)
	}

	rodaReconstrucaoAteEsvaziar(t, s2)

	if campos := camposDoSelado(t, dir, alvo); !slices.Equal(campos, []types.FieldID{campoNode}) {
		t.Errorf("o .sidx do segmento %d ficou com %v, quero só [%d]: a seção do campo removido não saiu",
			alvo, campos, campoNode)
	}
	// O campo que continua indexado tem de continuar respondendo: a remoção não
	// pode levar junto o índice de quem ficou.
	if got := lookupRevalidado(t, s2, campoNode, strVal("n1")); len(got) == 0 {
		t.Error("a reescrita sem a seção removida também perdeu o índice do campo que ficou")
	}
}

// TestConstruirComCampoNaoIndexadoEhRecusado é o que SOBRA da recusa de
// subconjunto, e é a metade dela que continua verdadeira. Pedir a reconstrução
// de um campo que o dataset não indexa gravaria uma seção que a ingestão não
// alimenta: o próximo record a entrar naquele segmento não teria posting, e o
// arquivo passaria a afirmar mais do que sabe. Pedir MENOS que o conjunto
// declarado é legítimo (o restante é preservado); pedir FORA dele, não.
func TestConstruirComCampoNaoIndexadoEhRecusado(t *testing.T) {
	campos := []string{"node"}
	s, dir := abreStoreIndexadoComSegmentoPequeno(t, campos, 64<<10)
	alvo := s.curSegID.Load()
	poeLinhasAteRotacionar(t, s, alvo)
	caminho := segindex.CaminhoSelado(filepath.Join(dir, "data"), alvo)
	antes, err := os.ReadFile(caminho)
	if err != nil {
		t.Fatalf("pré-condição: o segmento %d deveria estar selado: %v", alvo, err)
	}

	naoIndexado := campoDoDicionario(t, s, "type")
	err = s.ConstroiIndiceDoSegmento(t.Context(), alvo, []types.FieldID{naoIndexado})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("ConstroiIndiceDoSegmento(campo não indexado) = %v, quero ErrInvalidArgument", err)
	}
	depois, err := os.ReadFile(caminho)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(antes, depois) {
		t.Errorf("a recusa reescreveu o .sidx: %d bytes viraram %d", len(antes), len(depois))
	}
}

// TestSidxDeZeroBytesEhPendenteEmVezDeDerrubarAFila fecha a terceira porta da
// classificação de "índice ilegível". Um arquivo de zero bytes não chega ao
// parser: o mmap o recusa antes, com mmap.ErrArquivoVazio, e sem o sentinela na
// mesma lista ele cairia no ramo genérico de erro e derrubaria a passada INTEIRA
// da fila por uma falha de infra que não é falha de infra.
//
// Não é hipótese, e o gatilho é conhecido: o temporário da escrita ainda tem nome
// FIXO (segindex/escritor.go), então dois escritores sobre o mesmo `<sidx>.tmp` —
// o O_TRUNC de um contra o rename do outro — publicam um arquivo final vazio. É a
// corrida que a §2.4 emendada manda a Tarefa 14 fechar com nome único por
// escritor; quem a criar não pode encontrar uma fila que morre em vez de um
// arquivo que se refaz.
//
// A doutrina é a do godoc logo acima do segmentoPendente, e vale aqui sem
// emenda: dado derivado que não serve, derive de novo.
func TestSidxDeZeroBytesEhPendenteEmVezDeDerrubarAFila(t *testing.T) {
	dir := t.TempDir()
	s := abreStoreIndexadoEm(t, dir, []string{"node"}, 64<<10)
	alvo := s.curSegID.Load()
	poeLinhasAteRotacionar(t, s, alvo)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	caminho := segindex.CaminhoSelado(filepath.Join(dir, "data"), alvo)
	if _, err := os.Stat(caminho); err != nil {
		t.Fatalf("pré-condição: o segmento %d deveria estar selado: %v", alvo, err)
	}
	// Zero bytes, que é o que a corrida do temporário publica.
	if err := os.WriteFile(caminho, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	s2 := abreStoreIndexadoEm(t, dir, []string{"node"}, 64<<10)
	campo := campoDoDicionario(t, s2, "node")
	pendentes, err := s2.SegmentosPendentesDeReconstrucao(JanelaDeIndice{})
	if err != nil {
		t.Fatalf("SegmentosPendentesDeReconstrucao = %v: um .sidx vazio derrubou a fila inteira", err)
	}
	if !slices.Contains(pendentes, alvo) {
		t.Fatalf("SegmentosPendentesDeReconstrucao = %v, quero o segmento %d: um .sidx vazio não entrou na fila",
			pendentes, alvo)
	}

	// E a reconstrução tem de atravessar o arquivo vazio, não tropeçar nele: a
	// cobertura pergunta ao .sidx atual quais campos preservar, e é a MESMA
	// classificação que responde ali.
	rodaReconstrucaoAteEsvaziar(t, s2)

	if campos := camposDoSelado(t, dir, alvo); !slices.Equal(campos, []types.FieldID{campo}) {
		t.Errorf("o .sidx do segmento %d ficou com %v, quero [%d]: o arquivo vazio não foi refeito",
			alvo, campos, campo)
	}
	if got := lookupRevalidado(t, s2, campo, strVal("n1")); len(got) == 0 {
		t.Error("o índice refeito sobre o arquivo vazio não responde")
	}
}
