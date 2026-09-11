package storage

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// fieldOfDicionario resolve o FieldID pelo dicionário do dataset, que é a
// única fonte de verdade sobre o mapeamento nome→id: os testes deste arquivo
// declaram campos indexados por NOME, e o Open os registra antes de qualquer
// outro, então fixar ids literais aqui amarraria o teste à ordem interna do
// Open em vez de ao contrato.
func fieldOfDicionario(t testing.TB, s *Store, name string) types.FieldID {
	t.Helper()
	id, err := s.FieldDict().LookupOrCreate([]byte(name))
	if err != nil {
		t.Fatalf("LookupOrCreate(%s): %v", name, err)
	}
	return id
}

// rowIndexada monta um payload com id/node/type ordenado por FieldID — o
// formato exige ordem estritamente crescente, e a ordem dos ids depende de
// quem registrou o quê primeiro.
func rowIndexada(t testing.TB, s *Store, key, node string, tipo int64) []byte {
	t.Helper()
	type field struct {
		id types.FieldID
		v  types.Value
	}
	fields := []field{
		{fieldOfDicionario(t, s, "id"), types.Value{Type: types.TypeString, Bytes: []byte(key)}},
		{fieldOfDicionario(t, s, "node"), types.Value{Type: types.TypeString, Bytes: []byte(node)}},
		{fieldOfDicionario(t, s, "type"), types.Value{Type: types.TypeInt64, I64: tipo}},
	}
	slices.SortFunc(fields, func(a, b field) int { return int(a.id) - int(b.id) })
	var p []byte
	for _, c := range fields {
		p = mustAppendField(t, p, c.id, c.v)
	}
	return p
}

// openStoreIndexed abre um dataset materializado com os campos declarados
// indexados, e devolve também o diretório para que o teste possa olhar os
// arquivos do índice diretamente.
func openStoreIndexed(t *testing.T, fields []string) (*Store, string) {
	t.Helper()
	return openStoreIndexedWithSegmentSmall(t, fields, 1<<20)
}

// Os MESMOS campos entram nos dois motores antigos de propósito. O índice por
// segmento cobre a união dos dois (spec D5), e é essa declaração dupla que gateia
// a deduplicação: sem ela, fieldsIndexados sairia com cada campo duas vezes, o
// openWIPsOfCurrent abriria o mesmo .widx duas vezes vazando um mapeamento, e
// o Set — que é append incondicional, sem varredura de dedup, e é isso que o
// torna O(1) — gravaria cada posting em duplicata. O Lookup compacta, então o
// RESULTADO sairia certo e o desperdício ficaria sem prova.
func openStoreIndexedWithSegmentSmall(t *testing.T, fields []string, secSize int64) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	s := openStore(t, Options{
		Dir:                 dir,
		SegmentSize:         secSize,
		IndexedFieldsByName: fields,
	})
	return s, dir
}

// openStoreHistoryIndexed abre o par base/histórico: o histórico não tem
// dicionário próprio por desenho, e é nele que a retenção — a segunda dor da
// spec — acontece.
func openStoreHistoryIndexed(t *testing.T, fields []string, secSize int64) (*Store, string) {
	t.Helper()
	base := openStore(t, Options{})
	dir := t.TempDir()
	hist := openStore(t, Options{
		Dir:                   dir,
		Dataset:               "events_history",
		SegmentSize:           secSize,
		Mode:                  HistoryMode,
		SharedDictionary:      base.FieldDict(),
		WriteInstantFieldName: "_writtenAt",
		IndexedFieldsByName:   fields,
	})
	return hist, dir
}

// poeRows grava n linhas distintas no dataset, pelo caminho que o modo dele
// exige — Put no materializado, Apenda no histórico.
func poeRows(t *testing.T, s *Store, n int) {
	t.Helper()
	for i := range n {
		key := "k" + strconv.Itoa(i)
		payload := rowIndexada(t, s, key, "n"+strconv.Itoa(i%4), int64(i%3))
		if s.EhHistory() {
			if _, err := s.Apenda(payload); err != nil {
				t.Fatalf("Apenda(%d): %v", i, err)
			}
			continue
		}
		mustPut(t, s, key, int64(i+1), payload, nil)
	}
}

// poeRowsUntilRotacionar grava até o segmento corrente deixar de ser o dado,
// com teto para que um erro de dimensionamento vire falha e não um teste
// eterno.
func poeRowsUntilRotacionar(t *testing.T, s *Store, sec uint32) {
	t.Helper()
	for i := range 100000 {
		poeOneRow(t, s, i)
		if s.curSecID.Load() != sec {
			return
		}
	}
	t.Fatalf("o segmento %d não rotacionou em 100000 linhas", sec)
}

func poeOneRow(t *testing.T, s *Store, i int) {
	t.Helper()
	key := "rot" + strconv.Itoa(i)
	payload := rowIndexada(t, s, key, "n"+strconv.Itoa(i%4), int64(i%3))
	if s.EhHistory() {
		if _, err := s.Apenda(payload); err != nil {
			t.Fatalf("Apenda(%d): %v", i, err)
		}
		return
	}
	mustPut(t, s, key, int64(i+1), payload, nil)
}

func TestOSegmentoCorrenteGanhaWIPPorCampoIndexado(t *testing.T) {
	s, dir := openStoreIndexed(t, []string{"node", "type"})
	poeRows(t, s, 10)
	fields, err := segindex.ListWIP(filepath.Join(dir, "data"), s.curSecID.Load())
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != 2 {
		t.Fatalf("ListWIP = %v, quero um .widx por campo indexado", fields)
	}
	if !slices.Equal(fields, s.FieldsIndexados()) {
		t.Fatalf("ListWIP = %v, FieldsIndexados = %v: os dois conjuntos têm de coincidir",
			fields, s.FieldsIndexados())
	}
}

// TestARotacaoSelaOSegmentoAnterior: a selagem é disparada pelo Append que
// rotaciona, e não por um temporizador nem pelo checkpoint. Se ela dependesse
// de qualquer outra coisa, existiria um intervalo em que o segmento anterior
// não é mais o corrente e ainda tem .widx — e o Open o descartaria, jogando
// fora um índice inteiro por nada.
func TestARotacaoSelaOSegmentoAnterior(t *testing.T) {
	s, dir := openStoreIndexedWithSegmentSmall(t, []string{"node"}, 64<<10)
	primeiro := s.curSecID.Load()
	poeRowsUntilRotacionar(t, s, primeiro)
	if s.curSecID.Load() == primeiro {
		t.Fatal("o segmento não rotacionou; o teste não exercita nada")
	}
	if _, err := os.Stat(segindex.SealedPath(filepath.Join(dir, "data"), primeiro)); err != nil {
		t.Fatalf("o segmento %d rotacionou sem selar o índice: %v", primeiro, err)
	}
	fields, _ := segindex.ListWIP(filepath.Join(dir, "data"), primeiro)
	if len(fields) != 0 {
		t.Errorf("o segmento selado %d ainda tem .widx: %v", primeiro, fields)
	}
	// E o segmento novo já nasce com o dele: sem isso a ingestão seguiria sem
	// índice até a próxima abertura, em silêncio.
	novos, err := segindex.ListWIP(filepath.Join(dir, "data"), s.curSecID.Load())
	if err != nil {
		t.Fatal(err)
	}
	if len(novos) != 1 {
		t.Fatalf("ListWIP do segmento novo = %v, quero um .widx por campo indexado", novos)
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
	s := openStore(t, Options{Dir: dir, IndexedFieldsByName: []string{"node"}})
	poeRows(t, s, 50)
	sec := s.curSecID.Load()
	path := segindex.WIPPath(filepath.Join(dir, "data"), sec, fieldOfDicionario(t, s, "node"))
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	garbage := bytes.Repeat([]byte{0xAA}, 4096)
	if err := os.WriteFile(path, garbage, 0o644); err != nil {
		t.Fatal(err)
	}

	// openStore falha o teste se o Open recusar: é essa recusa que o descarte
	// existe para não acontecer.
	s2 := openStore(t, Options{Dir: dir, IndexedFieldsByName: []string{"node"}})
	depois, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("o segmento corrente ficou sem .widx depois da abertura: %v", err)
	}
	if bytes.Equal(depois, garbage) {
		t.Fatal("o .widx da execução anterior foi ADOTADO em vez de descartado")
	}
	// E o índice novo aceita escrita: um arquivo recriado que não recebesse
	// posting seria descarte sem substituição.
	poeRows(t, s2, 1)
}

// TestAAposentadoriaLevaOWIPOrfaoJunto cobre o terceiro caso da §2.5. Um crash
// entre a rotação e a selagem deixa .widx de um segmento que já NÃO é o
// corrente, e o descarte da abertura só alcança o corrente — então, sem esta
// remoção, o .dat e o .sidx sairiam na aposentadoria e o .widx ficaria em disco
// para sempre. E ele agora é CONTADO no IndexBytes: viraria disco visível que
// nada explica, que é o modo de falha que aquela contagem existe para eliminar.
func TestAAposentadoriaLevaOWIPOrfaoJunto(t *testing.T) {
	s, dir := openStoreHistoryIndexed(t, []string{"node"}, 64<<10)
	target := s.curSecID.Load()
	poeRowsUntilRotacionar(t, s, target)

	// O resíduo é gravado com bytes que NENHUM OpenWIP aceitaria, e isso é o
	// ponto: a limpeza tem de ser por nome, porque um .widx rasgado por crash é
	// justamente o que não se deixa abrir.
	orfao := segindex.WIPPath(filepath.Join(dir, "data"), target, fieldOfDicionario(t, s, "node"))
	if err := os.WriteFile(orfao, bytes.Repeat([]byte{0xAA}, 4096), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := s.RetentionOnePass(context.Background(), LimitesOfRetention{MaxBytes: 1}); err != nil {
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
	s, dir := openStoreHistoryIndexed(t, []string{"node"}, 64<<10)
	target := s.curSecID.Load()
	poeRowsUntilRotacionar(t, s, target)

	path := segindex.SealedPath(filepath.Join(dir, "data"), target)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("pré-condição: o segmento %d deveria estar selado: %v", target, err)
	}

	before := s.EstatisticasOfRetention()
	if before.IndexBytes <= 0 {
		t.Fatalf("IndexBytes = %d com um .sidx de %d bytes em disco", before.IndexBytes, info.Size())
	}
	// O oráculo é a DIFERENÇA, e não um valor absoluto: o IndexBytes já somava
	// indexes/ e checkpoint/ antes desta tarefa, então um teste de "> 0"
	// passaria com o índice de segmento inteiro invisível. Remover o .sidx é a
	// única mudança entre as duas medidas.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	depois := s.EstatisticasOfRetention()
	if queda := before.IndexBytes - depois.IndexBytes; queda < info.Size() {
		t.Errorf("o IndexBytes caiu %d ao remover um .sidx de %d bytes: o índice em data/ não está sendo contado",
			queda, info.Size())
	}
	// E ele NÃO pode ter entrado no DataBytes: `history.maxBytes` governa DADO,
	// e o índice tem teto próprio. Somá-los mudaria o significado de uma chave
	// de configuração que já está em produção.
	if before.DataBytes != depois.DataBytes {
		t.Errorf("o DataBytes mudou de %d para %d ao remover um índice: o teto de dado passou a contar índice",
			before.DataBytes, depois.DataBytes)
	}
}

// TestAAposentadoriaLevaOIndiceJunto é a segunda dor da spec, e o teste que
// prova que ela foi resolvida por CONSTRUÇÃO. Antes, nada removia as postings
// de um segmento aposentado, e cada órfã alongava a cadeia de quem estava vivo.
func TestAAposentadoriaLevaOIndiceJunto(t *testing.T) {
	s, dir := openStoreHistoryIndexed(t, []string{"node"}, 64<<10)
	target := s.curSecID.Load()
	poeRowsUntilRotacionar(t, s, target)
	path := segindex.SealedPath(filepath.Join(dir, "data"), target)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("pré-condição: o segmento %d deveria estar selado: %v", target, err)
	}
	if _, err := s.RetentionOnePass(context.Background(), LimitesOfRetention{MaxBytes: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("Stat(%s) = %v, quero IsNotExist — o índice tem de sair com o .dat", filepath.Base(path), err)
	}
}

// TestOSidxOrfaoDeSegmentoAposentadoSaiNaAbertura fecha a promessa da §2.3 que
// estava só em prosa: um crash entre o unlink do .dat e a remoção do índice na
// aposentadoria deixa .sidx de um segmento que não existe mais, e nenhuma
// enumeração que parta dos dados o alcança — sem a limpeza da abertura ele
// ficaria em disco PARA SEMPRE, contado pelo IndexBytes como disco que nada
// explica. O .widx órfão já saía; o .sidx era o irmão esquecido.
func TestOSidxOrfaoDeSegmentoAposentadoSaiNaAbertura(t *testing.T) {
	s, dir := openStoreIndexed(t, []string{"node"})
	poeRows(t, s, 5)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// O órfão: um .sidx de segmento que nunca existiu neste dataset — o estado
	// que o crash da aposentadoria deixa (o .dat saiu, o índice ficou).
	orfao := segindex.SealedPath(filepath.Join(dir, "data"), 77)
	e := segindex.NewWriter()
	e.Add(fieldOfDicionario(t, s, "node"), 42, 1)
	if err := e.WriteSealed(orfao, 77); err != nil {
		t.Fatal(err)
	}

	openStoreIndexedIn(t, dir, []string{"node"}, 1<<20)
	if _, err := os.Stat(orfao); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("o .sidx órfão sobreviveu à abertura (stat: %v): ele ficaria em disco para sempre", err)
	}
}

// TestOSidxOrfaoSaiMesmoSemCampoIndexadoNenhum prova o POSICIONAMENTO da
// limpeza: ela roda antes do early-return de campos vazios. Um dataset que
// deixou de indexar continua tendo órfão a limpar — com a limpeza depois do
// early-return, este teste ficaria vermelho e o resíduo viveria para sempre
// num dataset sem index: true nenhum.
func TestOSidxOrfaoSaiMesmoSemCampoIndexadoNenhum(t *testing.T) {
	s, dir := openStoreIndexed(t, []string{"node"})
	field := fieldOfDicionario(t, s, "node")
	poeRows(t, s, 5)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	orfao := segindex.SealedPath(filepath.Join(dir, "data"), 88)
	e := segindex.NewWriter()
	e.Add(field, 42, 1)
	if err := e.WriteSealed(orfao, 88); err != nil {
		t.Fatal(err)
	}

	// Reabre SEM campo indexado nenhum — o índice foi removido do YAML.
	openStore(t, Options{Dir: dir, SegmentSize: 1 << 20})
	if _, err := os.Stat(orfao); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("o .sidx órfão sobreviveu à abertura sem campos indexados (stat: %v): "+
			"a limpeza tem de rodar antes do early-return de campos vazios", err)
	}
}
