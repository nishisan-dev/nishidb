package query_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// stub é um Store instrumentado e SEM alocação por row: os payloads são
// pré-montados e o GetByRowID copia para o buffer do chamador. Serve para
// medir o executor sem o custo do dublê completo, e para observar o que ele
// pediu ao storage.
type stub struct {
	ids      []types.RowID
	payloads map[types.RowID][]byte
	keys     map[string]types.RowID
	mortas   map[types.RowID]bool

	erroEm      types.RowID // GetByRowID falha nesta row
	temErro     bool
	lentidao    time.Duration // atraso por candidato (teste de timeout)
	aoAvancar   func(n int)   // gancho: chamado a cada candidato consumido
	avancos     int
	gets        int
	getsPorKey  int
	scans       int
	fechamentos int
}

func (s *stub) GetByRowID(id types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	s.gets++
	if s.temErro && id == s.erroEm {
		return types.Record{}, buf, false, fmt.Errorf("disco caiu na row %d", id)
	}
	if s.mortas[id] {
		return types.Record{}, buf, false, nil
	}
	p, ok := s.payloads[id]
	if !ok {
		return types.Record{}, buf, false, nil
	}
	buf = append(buf[:0], p...)
	return types.Record{RowID: id, Payload: buf}, buf, true, nil
}

func (s *stub) GetByKey(key, buf []byte) (types.Record, []byte, bool, error) {
	s.getsPorKey++
	id, ok := s.keys[string(key)]
	if !ok {
		return types.Record{}, buf, false, nil
	}
	return s.GetByRowID(id, buf)
}

func (s *stub) ScanLive() types.RowIDIterator {
	s.scans++
	return &stubIter{s: s, ids: s.ids}
}

func (s *stub) LookupBitmap(types.FieldID, types.Value) (types.RowIDIterator, error) {
	return nil, fmt.Errorf("%w: stub sem bitmap", types.ErrInvalidArgument)
}

func (s *stub) HasBitmap(types.FieldID) bool { return false }

type stubIter struct {
	s      *stub
	ids    []types.RowID
	pos    int
	fechou bool
}

func (i *stubIter) Next() bool {
	if i.pos >= len(i.ids) {
		return false
	}
	i.pos++
	i.s.avancos++
	if i.s.lentidao > 0 {
		time.Sleep(i.s.lentidao)
	}
	if i.s.aoAvancar != nil {
		i.s.aoAvancar(i.s.avancos)
	}
	return true
}

func (i *stubIter) RowID() types.RowID { return i.ids[i.pos-1] }
func (i *stubIter) Err() error         { return nil }

func (i *stubIter) Close() error {
	if !i.fechou {
		i.fechou = true
		i.s.fechamentos++
	}
	return nil
}

// ---------------------------------------------------------------- fixture

const stubYAML = `
version: 1
name: linhas
materialization:
  key:
    field: id
schema:
  include:
    - "*"
  unknownFields: include
  fields:
    id:
      type: string
    nome:
      type: string
    n:
      type: int64
query:
  maxRows: %d
  timeout: %s
`

// montaStub cria n rows com nome="linha" e n=índice.
func montaStub(tb testing.TB, sch *schema.Compiled, n int) *stub {
	tb.Helper()
	id, _ := sch.FieldByName([]byte("id"))
	nome, _ := sch.FieldByName([]byte("nome"))
	num, _ := sch.FieldByName([]byte("n"))

	campos := []struct {
		f *schema.CompiledField
		v func(i int) types.Value
	}{
		{id, func(i int) types.Value {
			return types.Value{Type: types.TypeString, Bytes: []byte(fmt.Sprintf("K%05d", i))}
		}},
		{nome, func(int) types.Value { return types.Value{Type: types.TypeString, Bytes: []byte("linha")} }},
		{num, func(i int) types.Value { return types.Value{Type: types.TypeInt64, I64: int64(i)} }},
	}
	sort.SliceStable(campos, func(a, b int) bool { return campos[a].f.ID < campos[b].f.ID })

	s := &stub{
		payloads: make(map[types.RowID][]byte, n),
		keys:     make(map[string]types.RowID, n),
		mortas:   map[types.RowID]bool{},
	}
	for i := 0; i < n; i++ {
		var p []byte
		var err error
		for _, c := range campos {
			if p, err = encoding.AppendField(p, c.f.ID, c.v(i)); err != nil {
				tb.Fatal(err)
			}
		}
		rid := types.RowID(i)
		s.ids = append(s.ids, rid)
		s.payloads[rid] = p
		s.keys[fmt.Sprintf("K%05d", i)] = rid
	}
	return s
}

type stubHarness struct {
	tb     testing.TB
	sch    *schema.Compiled
	store  *stub
	engine *query.Engine
}

func novoStubHarness(t *testing.T, rows, maxRows int, timeout string) *stubHarness {
	t.Helper()
	return montaStubHarness(t, rows, maxRows, timeout)
}

// novoStubHarnessB é o mesmo para benchmarks (teto e timeout folgados).
func novoStubHarnessB(b *testing.B, rows int) *stubHarness {
	b.Helper()
	return montaStubHarness(b, rows, 10_000_000, "5m")
}

func montaStubHarness(tb testing.TB, rows, maxRows int, timeout string) *stubHarness {
	tb.Helper()
	spec, err := schema.Parse([]byte(fmt.Sprintf(stubYAML, maxRows, timeout)))
	if err != nil {
		tb.Fatal(err)
	}
	sch, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		tb.Fatal(err)
	}
	st := montaStub(tb, sch, rows)
	eng := query.NewEngine()
	if err := eng.Register("linhas", sch, st); err != nil {
		tb.Fatal(err)
	}
	return &stubHarness{tb: tb, sch: sch, store: st, engine: eng}
}

func (h *stubHarness) abre(ctx context.Context, sql string) executor.RowIterator {
	h.tb.Helper()
	it, err := h.engine.Execute(ctx, "linhas", sql, nil)
	if err != nil {
		h.tb.Fatalf("Execute(%q): %v", sql, err)
	}
	return it
}

func (h *stubHarness) conta(sql string) (int, error) {
	h.tb.Helper()
	it := h.abre(context.Background(), sql)
	defer it.Close()
	n := 0
	for it.Next() {
		n++
	}
	return n, it.Err()
}

// ---------------------------------------------------------------- LIMIT

// §65: o LIMIT INTERROMPE a varredura. Provar que "vieram 3 linhas" não basta
// — um executor que varresse o milhão inteiro e devolvesse as 3 primeiras
// passaria nisso e seria inutilizável.
func TestLimitEarlyExit(t *testing.T) {
	h := novoStubHarness(t, 1000, 100000, "30s")

	it := h.abre(context.Background(), `SELECT id FROM linhas LIMIT 3`)
	n := 0
	for it.Next() {
		n++
	}
	if n != 3 {
		t.Fatalf("linhas = %d, want 3", n)
	}
	if h.store.avancos != 3 {
		t.Errorf("candidatos consumidos = %d, want 3: o LIMIT tem de interromper a fonte", h.store.avancos)
	}
	if err := it.Err(); err != nil {
		t.Errorf("Err() = %v, want nil: LIMIT explícito não é truncamento", err)
	}
	_ = it.Close()
}

// A fonte é fechada NA HORA em que a última linha é emitida, não no Next
// seguinte: com LIMIT 3 sobre um milhão de candidatos, segurar o iterador do
// índice até o consumidor decidir avançar é o mesmo que não ter early-exit.
func TestLimitFechaFonteAoEmitirAUltimaLinha(t *testing.T) {
	h := novoStubHarness(t, 1000, 100000, "30s")
	it := h.abre(context.Background(), `SELECT id FROM linhas LIMIT 2`)
	defer it.Close()

	for i := 0; i < 2; i++ {
		if !it.Next() {
			t.Fatalf("Next() %d = false", i)
		}
	}
	if h.store.fechamentos != 1 {
		t.Errorf("fechamentos = %d, want 1 já no Next que emitiu a última linha", h.store.fechamentos)
	}
}

// LIMIT 0 não pode tocar o Store: o iterador nem chega a abrir a varredura.
func TestLimitZeroNaoTocaOStore(t *testing.T) {
	h := novoStubHarness(t, 100, 100000, "30s")
	it := h.abre(context.Background(), `SELECT id FROM linhas LIMIT 0`)
	defer it.Close()

	if it.Next() {
		t.Fatal("LIMIT 0 devolveu linha")
	}
	if it.Err() != nil {
		t.Errorf("Err() = %v, want nil", it.Err())
	}
	if h.store.avancos != 0 || h.store.gets != 0 {
		t.Errorf("avancos=%d gets=%d, want 0/0", h.store.avancos, h.store.gets)
	}
}

// Teto IMPLÍCITO: o resultado foi truncado e o cliente precisa saber. Um corte
// silencioso entrega resposta parcial com cara de resposta completa.
func TestMaxRowsImplicitoSinalizaTruncamento(t *testing.T) {
	h := novoStubHarness(t, 20, 5, "30s")
	n, err := h.conta(`SELECT id FROM linhas`)

	if n != 5 {
		t.Fatalf("linhas = %d, want 5", n)
	}
	if !errors.Is(err, executor.ErrMaxRowsExceeded) {
		t.Fatalf("Err() = %v, want ErrMaxRowsExceeded", err)
	}
	if !strings.Contains(err.Error(), "maxRows") {
		t.Errorf("mensagem = %q, want citar maxRows", err)
	}
	if got := h.engine.Metrics().Truncated.Load(); got != 1 {
		t.Errorf("Truncated = %d, want 1", got)
	}
}

// O alarme FALSO do teto exato, fixado como comportamento em vez de virar
// descoberta de produção: com exatamente maxRows linhas no resultado, o
// executor ainda reporta truncamento.
//
// É deliberado — distinguir "acabou" de "parou no teto" exigiria continuar
// avaliando o predicado até achar a próxima linha viva, potencialmente o
// dataset inteiro, que é o custo que o teto existe para evitar. Alarme falso é
// aceitável; truncamento silencioso não é. O teste existente usava 20 rows com
// teto 5, e por isso não distinguia os dois casos.
func TestTetoExatoAindaReportaTruncamento(t *testing.T) {
	h := novoStubHarness(t, 5, 5, "30s") // exatamente maxRows linhas
	n, err := h.conta(`SELECT id FROM linhas`)

	if n != 5 {
		t.Fatalf("linhas = %d, want 5", n)
	}
	if !errors.Is(err, executor.ErrMaxRowsExceeded) {
		t.Errorf("Err() = %v, want ErrMaxRowsExceeded mesmo com o resultado exato", err)
	}
}

// E o complemento: ABAIXO do teto não há alarme nenhum.
func TestAbaixoDoTetoNaoReportaTruncamento(t *testing.T) {
	h := novoStubHarness(t, 4, 5, "30s")
	n, err := h.conta(`SELECT id FROM linhas`)
	if n != 4 || err != nil {
		t.Errorf("linhas=%d err=%v, want 4/nil", n, err)
	}
}

// LIMIT explícito abaixo do teto NÃO é truncamento: o usuário pediu 3 e
// recebeu 3.
func TestLimitMenorQueMaxRowsNaoErra(t *testing.T) {
	h := novoStubHarness(t, 20, 5, "30s")
	n, err := h.conta(`SELECT id FROM linhas LIMIT 3`)
	if n != 3 || err != nil {
		t.Fatalf("linhas=%d err=%v, want 3/nil", n, err)
	}
	if got := h.engine.Metrics().Truncated.Load(); got != 0 {
		t.Errorf("Truncated = %d, want 0", got)
	}
}

// ---------------------------------------------------------------- ctx

// §91: o full scan é cancelável. A cadência no full scan é de UM candidato —
// um scan de 8M rows não pode ignorar o cancelamento por 256 leituras.
func TestCtxCanceladoNoMeioDoFullScan(t *testing.T) {
	h := novoStubHarness(t, 1000, 100000, "30s")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.store.aoAvancar = func(n int) {
		if n == 10 {
			cancel()
		}
	}

	it := h.abre(ctx, `SELECT id FROM linhas`)
	defer it.Close()
	n := 0
	for it.Next() {
		n++
	}
	if !errors.Is(it.Err(), context.Canceled) {
		t.Fatalf("Err() = %v, want context.Canceled", it.Err())
	}
	if !strings.Contains(it.Err().Error(), "query interrompida") {
		t.Errorf("mensagem = %q", it.Err())
	}
	if n > 11 {
		t.Errorf("emitiu %d linhas após o cancelamento no candidato 10", n)
	}
	if got := h.engine.Metrics().Canceled.Load(); got != 1 {
		t.Errorf("Canceled = %d, want 1", got)
	}
	if h.store.fechamentos != 1 {
		t.Errorf("fechamentos = %d, want 1: cancelar tem de fechar a fonte", h.store.fechamentos)
	}
}

// O timeout do DATASET (query.timeout do YAML) vale mesmo com ctx sem
// deadline: é o teto que impede uma query de segurar recursos para sempre.
func TestTimeoutDoDataset(t *testing.T) {
	h := novoStubHarness(t, 1000, 100000, "5ms")
	h.store.lentidao = time.Millisecond

	_, err := h.conta(`SELECT id FROM linhas`)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Err() = %v, want DeadlineExceeded", err)
	}
}

// ---------------------------------------------------------------- contratos

// O contrato zero-copy AFIRMADO, não só comentado: os bytes da row anterior
// são sobrescritos pelo Next seguinte.
func TestRowInvalidaAposNext(t *testing.T) {
	h := novoStubHarness(t, 3, 100000, "30s")
	it := h.abre(context.Background(), `SELECT id FROM linhas`)
	defer it.Close()

	if !it.Next() {
		t.Fatal("sem primeira linha")
	}
	capturado := it.Row().Col(0).Val.Bytes
	primeiro := string(capturado)
	if !it.Next() {
		t.Fatal("sem segunda linha")
	}
	if string(capturado) == primeiro {
		t.Errorf("o slice capturado continuou %q: o teste não está provando o zero-copy", primeiro)
	}
	if string(capturado) != string(it.Row().Col(0).Val.Bytes) {
		t.Errorf("slice capturado = %q, row corrente = %q",
			capturado, it.Row().Col(0).Val.Bytes)
	}
}

// O contrato "Val é zero quando Null" é o que separa uma coluna nula do VALOR
// DA ROW ANTERIOR: o coleta reseta só o `present`, nunca o `val` — de
// propósito, é o que dá o zero alocação —, então o slot de um campo ausente
// carrega literalmente o valor anterior. O zeramento na projeção é a única
// coisa entre os dois.
//
// Hoje isso é invisível porque todo consumidor checa Null antes de olhar Val.
// A máscara acaba no primeiro que fizer o contrário — e o candidato é o driver
// JDBC, onde getInt() devolve o valor e wasNull() devolve o flag, nessa ordem:
// o valor da row anterior sairia como dado plausível de um campo nulo.
func TestValEhZeradoQuandoAColunaEhNull(t *testing.T) {
	h := novoStubHarness(t, 2, 100000, "30s")
	// A ORDEM é o ponto: a row COM o campo vem PRIMEIRO, e é o valor dela que
	// fica no slot para vazar na segunda. Com a ausente primeiro o slot ainda
	// está zerado e o teste não prova nada — foi assim que a primeira versão
	// deste teste deixou a mutação passar.
	h.store.payloads[1] = payloadSemNome(t, h.sch)

	it := h.abre(context.Background(), `SELECT nome FROM linhas`)
	defer it.Close()

	if !it.Next() {
		t.Fatalf("sem a primeira row: %v", it.Err())
	}
	if c := it.Row().Col(0); c.Null || string(c.Val.Bytes) != "linha" {
		t.Fatalf("a primeira row deveria ter o campo: %+v", c)
	}

	if !it.Next() {
		t.Fatalf("sem a segunda row: %v", it.Err())
	}
	c := it.Row().Col(0)
	if !c.Null {
		t.Fatalf("a segunda row deveria ter a coluna ausente: %+v", c)
	}
	// types.Value contém []byte e não é comparável com ==; o zero é aferido
	// campo a campo.
	if c.Val.Type != types.TypeNull || c.Val.I64 != 0 || c.Val.Bytes != nil {
		t.Errorf("Val = %+v numa coluna Null, want zero: é o valor da row ANTERIOR vazando", c.Val)
	}
}

// payloadSemNome monta um payload com id e n, sem o campo `nome`.
func payloadSemNome(t *testing.T, sch *schema.Compiled) []byte {
	t.Helper()
	id, _ := sch.FieldByName([]byte("id"))
	num, _ := sch.FieldByName([]byte("n"))
	campos := []struct {
		f *schema.CompiledField
		v types.Value
	}{
		{id, types.Value{Type: types.TypeString, Bytes: []byte("K00001")}},
		{num, types.Value{Type: types.TypeInt64, I64: 1}},
	}
	sort.SliceStable(campos, func(a, b int) bool { return campos[a].f.ID < campos[b].f.ID })
	var p []byte
	var err error
	for _, c := range campos {
		if p, err = encoding.AppendField(p, c.f.ID, c.v); err != nil {
			t.Fatal(err)
		}
	}
	return p
}

func TestRowZeroAposFim(t *testing.T) {
	h := novoStubHarness(t, 2, 100000, "30s")
	it := h.abre(context.Background(), `SELECT id FROM linhas`)
	defer it.Close()
	for it.Next() {
	}
	if got := it.Row().Len(); got != 0 {
		t.Errorf("Row().Len() = %d após o fim, want 0", got)
	}
}

func TestCloseIdempotenteEFechaUpstream(t *testing.T) {
	h := novoStubHarness(t, 10, 100000, "30s")
	it := h.abre(context.Background(), `SELECT id FROM linhas`)
	it.Next()

	if err := it.Close(); err != nil {
		t.Fatalf("primeiro Close: %v", err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("segundo Close: %v", err)
	}
	if h.store.fechamentos != 1 {
		t.Errorf("fechamentos = %d, want 1", h.store.fechamentos)
	}
	if it.Next() {
		t.Error("Next() após Close devolveu linha")
	}
}

// Erro de leitura ABORTA a query: pular a row transformaria falha de disco em
// resultado silenciosamente incompleto.
func TestErroDeLeituraAbortaQuery(t *testing.T) {
	h := novoStubHarness(t, 10, 100000, "30s")
	h.store.temErro, h.store.erroEm = true, types.RowID(3)

	it := h.abre(context.Background(), `SELECT id FROM linhas`)
	defer it.Close()
	n := 0
	for it.Next() {
		n++
	}
	if n != 3 {
		t.Errorf("linhas antes do erro = %d, want 3 (as já emitidas continuam válidas)", n)
	}
	if it.Err() == nil || !strings.Contains(it.Err().Error(), "leitura da row") {
		t.Fatalf("Err() = %v, want erro de leitura", it.Err())
	}
	if !strings.Contains(it.Err().Error(), "disco caiu") {
		t.Errorf("erro = %v, want preservar a causa", it.Err())
	}
}

// Payload corrompido também aborta — e com o sentinela do encoding, para o
// operador distinguir corrupção de erro de I/O.
func TestPayloadCorrompidoAbortaQuery(t *testing.T) {
	h := novoStubHarness(t, 5, 100000, "30s")
	// Tag 0x00 é reservada e nunca aparece em payload válido.
	h.store.payloads[2] = []byte{0x01, 0x00, 0x00, 0xff}

	it := h.abre(context.Background(), `SELECT id FROM linhas`)
	defer it.Close()
	for it.Next() {
	}
	if !errors.Is(it.Err(), encoding.ErrCorrupt) {
		t.Fatalf("Err() = %v, want encoding.ErrCorrupt", it.Err())
	}
	if !strings.Contains(it.Err().Error(), "payload da row") {
		t.Errorf("mensagem = %q", it.Err())
	}
}

// Row sem location (o índice denso não a conhece) é pulada como a tombstoned.
func TestFullScanIgnoraRowsSemLocation(t *testing.T) {
	h := novoStubHarness(t, 10, 100000, "30s")
	for id := types.RowID(0); id < 10; id += 2 {
		delete(h.store.payloads, id)
	}
	n, err := h.conta(`SELECT id FROM linhas`)
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Errorf("linhas = %d, want 5", n)
	}
	if got := h.engine.Metrics().DeadSkipped.Load(); got != 5 {
		t.Errorf("DeadSkipped = %d, want 5", got)
	}
}

// A ordem de saída é a de rowId crescente. Não é contrato de API na v1 (não há
// ORDER BY), mas os testes dependem dela e uma mudança silenciosa aqui
// quebraria todos de uma vez.
func TestOrdemDeSaidaEhRowIDCrescente(t *testing.T) {
	h := novoStubHarness(t, 50, 100000, "30s")
	it := h.abre(context.Background(), `SELECT id FROM linhas`)
	defer it.Close()

	anterior := ""
	for it.Next() {
		atual := string(it.Row().Col(0).Val.Bytes)
		if atual <= anterior {
			t.Fatalf("ordem quebrada: %q após %q", atual, anterior)
		}
		anterior = atual
	}
}

// SELECT * projeta todos os campos conhecidos, em ordem de FieldID, com
// Null=true para os ausentes na row.
func TestProjecaoStar(t *testing.T) {
	h := novoStubHarness(t, 1, 100000, "30s")
	it := h.abre(context.Background(), `SELECT * FROM linhas`)
	defer it.Close()

	if !it.Next() {
		t.Fatal("sem linha")
	}
	nomes := make([]string, 0, it.Row().Len())
	for _, c := range it.Row().Cols() {
		nomes = append(nomes, c.Name)
	}
	if len(nomes) != 3 {
		t.Fatalf("colunas = %v, want id/nome/n", nomes)
	}
	// Ordem de FieldID: o dicionário atribui por nome ordenado no Compile.
	if !sort.SliceIsSorted(nomes, func(a, b int) bool { return nomes[a] < nomes[b] }) {
		t.Logf("ordem das colunas = %v (ordem de FieldID, não alfabética)", nomes)
	}
}
