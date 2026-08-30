package storage

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// campoDoDicionario resolve o FieldID pelo dicionário do dataset, que é a
// única fonte de verdade sobre o mapeamento nome→id: os testes deste arquivo
// declaram campos indexados por NOME, e o Open os registra antes de qualquer
// outro, então fixar ids literais aqui amarraria o teste à ordem interna do
// Open em vez de ao contrato.
func campoDoDicionario(t testing.TB, s *Store, nome string) types.FieldID {
	t.Helper()
	id, err := s.FieldDict().LookupOrCreate([]byte(nome))
	if err != nil {
		t.Fatalf("LookupOrCreate(%s): %v", nome, err)
	}
	return id
}

// linhaIndexada monta um payload com id/node/type ordenado por FieldID — o
// formato exige ordem estritamente crescente, e a ordem dos ids depende de
// quem registrou o quê primeiro.
func linhaIndexada(t testing.TB, s *Store, key, node string, tipo int64) []byte {
	t.Helper()
	type campo struct {
		id types.FieldID
		v  types.Value
	}
	campos := []campo{
		{campoDoDicionario(t, s, "id"), types.Value{Type: types.TypeString, Bytes: []byte(key)}},
		{campoDoDicionario(t, s, "node"), types.Value{Type: types.TypeString, Bytes: []byte(node)}},
		{campoDoDicionario(t, s, "type"), types.Value{Type: types.TypeInt64, I64: tipo}},
	}
	slices.SortFunc(campos, func(a, b campo) int { return int(a.id) - int(b.id) })
	var p []byte
	for _, c := range campos {
		p = mustAppendField(t, p, c.id, c.v)
	}
	return p
}

// abreStoreIndexado abre um dataset materializado com os campos declarados
// indexados, e devolve também o diretório para que o teste possa olhar os
// arquivos do índice diretamente.
func abreStoreIndexado(t *testing.T, campos []string) (*Store, string) {
	t.Helper()
	return abreStoreIndexadoComSegmentoPequeno(t, campos, 1<<20)
}

// Os MESMOS campos entram nos dois motores antigos de propósito. O índice por
// segmento cobre a união dos dois (spec D5), e é essa declaração dupla que gateia
// a deduplicação: sem ela, camposIndexados sairia com cada campo duas vezes, o
// abreWIPsDoCorrente abriria o mesmo .widx duas vezes vazando um mapeamento, e
// o Set — que é append incondicional, sem varredura de dedup, e é isso que o
// torna O(1) — gravaria cada posting em duplicata. O Lookup compacta, então o
// RESULTADO sairia certo e o desperdício ficaria sem prova.
func abreStoreIndexadoComSegmentoPequeno(t *testing.T, campos []string, segSize int64) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	s := openStore(t, Options{
		Dir:                    dir,
		SegmentSize:            segSize,
		CamposIndexadosPorNome: campos,
	})
	return s, dir
}

// abreStoreHistoricoIndexado abre o par base/histórico: o histórico não tem
// dicionário próprio por desenho, e é nele que a retenção — a segunda dor da
// spec — acontece.
func abreStoreHistoricoIndexado(t *testing.T, campos []string, segSize int64) (*Store, string) {
	t.Helper()
	base := openStore(t, Options{})
	dir := t.TempDir()
	hist := openStore(t, Options{
		Dir:                    dir,
		Dataset:                "events_history",
		SegmentSize:            segSize,
		Modo:                   ModoHistorico,
		DictCompartilhado:      base.FieldDict(),
		CampoDeEscrita:         "_writtenAt",
		CamposIndexadosPorNome: campos,
	})
	return hist, dir
}

// poeLinhas grava n linhas distintas no dataset, pelo caminho que o modo dele
// exige — Put no materializado, Apenda no histórico.
func poeLinhas(t *testing.T, s *Store, n int) {
	t.Helper()
	for i := range n {
		key := "k" + strconv.Itoa(i)
		payload := linhaIndexada(t, s, key, "n"+strconv.Itoa(i%4), int64(i%3))
		if s.EhHistorico() {
			if _, err := s.Apenda(payload); err != nil {
				t.Fatalf("Apenda(%d): %v", i, err)
			}
			continue
		}
		mustPut(t, s, key, int64(i+1), payload, nil)
	}
}

// poeLinhasAteRotacionar grava até o segmento corrente deixar de ser o dado,
// com teto para que um erro de dimensionamento vire falha e não um teste
// eterno.
func poeLinhasAteRotacionar(t *testing.T, s *Store, seg uint32) {
	t.Helper()
	for i := range 100000 {
		poeUmaLinha(t, s, i)
		if s.curSegID.Load() != seg {
			return
		}
	}
	t.Fatalf("o segmento %d não rotacionou em 100000 linhas", seg)
}

func poeUmaLinha(t *testing.T, s *Store, i int) {
	t.Helper()
	key := "rot" + strconv.Itoa(i)
	payload := linhaIndexada(t, s, key, "n"+strconv.Itoa(i%4), int64(i%3))
	if s.EhHistorico() {
		if _, err := s.Apenda(payload); err != nil {
			t.Fatalf("Apenda(%d): %v", i, err)
		}
		return
	}
	mustPut(t, s, key, int64(i+1), payload, nil)
}

func TestOSegmentoCorrenteGanhaWIPPorCampoIndexado(t *testing.T) {
	s, dir := abreStoreIndexado(t, []string{"node", "type"})
	poeLinhas(t, s, 10)
	campos, err := segindex.ListaWIP(filepath.Join(dir, "data"), s.curSegID.Load())
	if err != nil {
		t.Fatal(err)
	}
	if len(campos) != 2 {
		t.Fatalf("ListaWIP = %v, quero um .widx por campo indexado", campos)
	}
	if !slices.Equal(campos, s.CamposIndexados()) {
		t.Fatalf("ListaWIP = %v, CamposIndexados = %v: os dois conjuntos têm de coincidir",
			campos, s.CamposIndexados())
	}
}

// TestARotacaoSelaOSegmentoAnterior: a selagem é disparada pelo Append que
// rotaciona, e não por um temporizador nem pelo checkpoint. Se ela dependesse
// de qualquer outra coisa, existiria um intervalo em que o segmento anterior
// não é mais o corrente e ainda tem .widx — e o Open o descartaria, jogando
// fora um índice inteiro por nada.
func TestARotacaoSelaOSegmentoAnterior(t *testing.T) {
	s, dir := abreStoreIndexadoComSegmentoPequeno(t, []string{"node"}, 64<<10)
	primeiro := s.curSegID.Load()
	poeLinhasAteRotacionar(t, s, primeiro)
	if s.curSegID.Load() == primeiro {
		t.Fatal("o segmento não rotacionou; o teste não exercita nada")
	}
	if _, err := os.Stat(segindex.CaminhoSelado(filepath.Join(dir, "data"), primeiro)); err != nil {
		t.Fatalf("o segmento %d rotacionou sem selar o índice: %v", primeiro, err)
	}
	campos, _ := segindex.ListaWIP(filepath.Join(dir, "data"), primeiro)
	if len(campos) != 0 {
		t.Errorf("o segmento selado %d ainda tem .widx: %v", primeiro, campos)
	}
	// E o segmento novo já nasce com o dele: sem isso a ingestão seguiria sem
	// índice até a próxima abertura, em silêncio.
	novos, err := segindex.ListaWIP(filepath.Join(dir, "data"), s.curSegID.Load())
	if err != nil {
		t.Fatal(err)
	}
	if len(novos) != 1 {
		t.Fatalf("ListaWIP do segmento novo = %v, quero um .widx por campo indexado", novos)
	}
}

// TestOWIPDeUmaExecucaoAnteriorEhDescartadoNaAbertura cobre a §2.5: o .widx é
// dado derivado e NUNCA é confiado através de um restart. O caso real não é
// exótico — o postCount só vai para o header no Fecha, então todo processo
// morto por SIGKILL deixa um header atrasado sobre um diretório adiantado, que
// o próprio segindex recusa. Adotá-lo trocaria uma abertura que funciona por um
// dataset inacessível.
func TestOWIPDeUmaExecucaoAnteriorEhDescartadoNaAbertura(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir, CamposIndexadosPorNome: []string{"node"}})
	poeLinhas(t, s, 50)
	seg := s.curSegID.Load()
	caminho := segindex.CaminhoWIP(filepath.Join(dir, "data"), seg, campoDoDicionario(t, s, "node"))
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	lixo := bytes.Repeat([]byte{0xAA}, 4096)
	if err := os.WriteFile(caminho, lixo, 0o644); err != nil {
		t.Fatal(err)
	}

	// openStore falha o teste se o Open recusar: é essa recusa que o descarte
	// existe para não acontecer.
	s2 := openStore(t, Options{Dir: dir, CamposIndexadosPorNome: []string{"node"}})
	depois, err := os.ReadFile(caminho)
	if err != nil {
		t.Fatalf("o segmento corrente ficou sem .widx depois da abertura: %v", err)
	}
	if bytes.Equal(depois, lixo) {
		t.Fatal("o .widx da execução anterior foi ADOTADO em vez de descartado")
	}
	// E o índice novo aceita escrita: um arquivo recriado que não recebesse
	// posting seria descarte sem substituição.
	poeLinhas(t, s2, 1)
}

// TestAAposentadoriaLevaOWIPOrfaoJunto cobre o terceiro caso da §2.5. Um crash
// entre a rotação e a selagem deixa .widx de um segmento que já NÃO é o
// corrente, e o descarte da abertura só alcança o corrente — então, sem esta
// remoção, o .dat e o .sidx sairiam na aposentadoria e o .widx ficaria em disco
// para sempre. E ele agora é CONTADO no IndexBytes: viraria disco visível que
// nada explica, que é o modo de falha que aquela contagem existe para eliminar.
func TestAAposentadoriaLevaOWIPOrfaoJunto(t *testing.T) {
	s, dir := abreStoreHistoricoIndexado(t, []string{"node"}, 64<<10)
	alvo := s.curSegID.Load()
	poeLinhasAteRotacionar(t, s, alvo)

	// O resíduo é gravado com bytes que NENHUM AbreWIP aceitaria, e isso é o
	// ponto: a limpeza tem de ser por nome, porque um .widx rasgado por crash é
	// justamente o que não se deixa abrir.
	orfao := segindex.CaminhoWIP(filepath.Join(dir, "data"), alvo, campoDoDicionario(t, s, "node"))
	if err := os.WriteFile(orfao, bytes.Repeat([]byte{0xAA}, 4096), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := s.RetencaoUmaPassada(context.Background(), LimitesDeRetencao{MaxBytes: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(orfao); !os.IsNotExist(err) {
		t.Errorf("Stat(%s) = %v, quero IsNotExist — o .widx órfão tem de sair com o segmento",
			filepath.Base(orfao), err)
	}
}

// TestOIndiceDeSegmentoEntraNoIndexBytes: o índice por segmento mora em data/,
// ao lado do .dat, e o DataBytes soma SÓ os segment-%08d.dat. Sem esta
// contagem, o dataset passa a consumir disco que NENHUMA das duas séries
// explica — que é o pior modo de falha possível para uma estatística que existe
// justamente para o operador ver para onde o disco foi.
func TestOIndiceDeSegmentoEntraNoIndexBytes(t *testing.T) {
	s, dir := abreStoreHistoricoIndexado(t, []string{"node"}, 64<<10)
	alvo := s.curSegID.Load()
	poeLinhasAteRotacionar(t, s, alvo)

	caminho := segindex.CaminhoSelado(filepath.Join(dir, "data"), alvo)
	info, err := os.Stat(caminho)
	if err != nil {
		t.Fatalf("pré-condição: o segmento %d deveria estar selado: %v", alvo, err)
	}

	antes := s.EstatisticasDeRetencao()
	if antes.IndexBytes <= 0 {
		t.Fatalf("IndexBytes = %d com um .sidx de %d bytes em disco", antes.IndexBytes, info.Size())
	}
	// O oráculo é a DIFERENÇA, e não um valor absoluto: o IndexBytes já somava
	// indexes/ e checkpoint/ antes desta tarefa, então um teste de "> 0"
	// passaria com o índice de segmento inteiro invisível. Remover o .sidx é a
	// única mudança entre as duas medidas.
	if err := os.Remove(caminho); err != nil {
		t.Fatal(err)
	}
	depois := s.EstatisticasDeRetencao()
	if queda := antes.IndexBytes - depois.IndexBytes; queda < info.Size() {
		t.Errorf("o IndexBytes caiu %d ao remover um .sidx de %d bytes: o índice em data/ não está sendo contado",
			queda, info.Size())
	}
	// E ele NÃO pode ter entrado no DataBytes: `history.maxBytes` governa DADO,
	// e o índice tem teto próprio. Somá-los mudaria o significado de uma chave
	// de configuração que já está em produção.
	if antes.DataBytes != depois.DataBytes {
		t.Errorf("o DataBytes mudou de %d para %d ao remover um índice: o teto de dado passou a contar índice",
			antes.DataBytes, depois.DataBytes)
	}
}

// TestAAposentadoriaLevaOIndiceJunto é a segunda dor da spec, e o teste que
// prova que ela foi resolvida por CONSTRUÇÃO. Antes, nada removia as postings
// de um segmento aposentado, e cada órfã alongava a cadeia de quem estava vivo.
func TestAAposentadoriaLevaOIndiceJunto(t *testing.T) {
	s, dir := abreStoreHistoricoIndexado(t, []string{"node"}, 64<<10)
	alvo := s.curSegID.Load()
	poeLinhasAteRotacionar(t, s, alvo)
	caminho := segindex.CaminhoSelado(filepath.Join(dir, "data"), alvo)
	if _, err := os.Stat(caminho); err != nil {
		t.Fatalf("pré-condição: o segmento %d deveria estar selado: %v", alvo, err)
	}
	if _, err := s.RetencaoUmaPassada(context.Background(), LimitesDeRetencao{MaxBytes: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(caminho); !os.IsNotExist(err) {
		t.Errorf("Stat(%s) = %v, quero IsNotExist — o índice tem de sair com o .dat", filepath.Base(caminho), err)
	}
}
