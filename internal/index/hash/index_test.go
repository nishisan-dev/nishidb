package hash

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func abre(t *testing.T, field types.FieldID) (*Index, string) {
	t.Helper()
	dir := t.TempDir()
	ix, err := Open(Path(dir, field), field, minSlotCount)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// O Close é tolerante ao segundo fechamento aqui de propósito: vários
	// casos fecham o índice no meio para reabri-lo, e o cleanup só existe para
	// os que não fecham.
	t.Cleanup(func() { _ = ix.Close() })
	return ix, dir
}

func txt(s string) types.Value { return types.Value{Type: types.TypeString, Bytes: []byte(s)} }
func num(i int64) types.Value  { return types.Value{Type: types.TypeInt64, I64: i} }

// ausente é o valor que o Clear recebe quando o campo SAIU da row.
var ausente = types.Value{Type: types.TypeNull}

func colheIter(t *testing.T, ix *Index, v types.Value) []types.RowID {
	t.Helper()
	it, err := ix.Lookup(v)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	defer func() { _ = it.Close() }()
	var out []types.RowID
	for it.Next() {
		out = append(out, it.RowID())
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	return out
}

func TestSetELookup(t *testing.T) {
	ix, _ := abre(t, 7)
	for _, r := range []types.RowID{5, 1, 9} {
		if err := ix.Set(txt("core-01"), r); err != nil {
			t.Fatal(err)
		}
	}
	if err := ix.Set(txt("core-02"), 3); err != nil {
		t.Fatal(err)
	}
	if got := colheIter(t, ix, txt("core-01")); !slices.Equal(got, []types.RowID{1, 5, 9}) {
		t.Fatalf("core-01: %v", got)
	}
	if got := colheIter(t, ix, txt("core-02")); !slices.Equal(got, []types.RowID{3}) {
		t.Fatalf("core-02: %v", got)
	}
	if got := colheIter(t, ix, txt("nunca-visto")); len(got) != 0 {
		t.Fatalf("valor nunca visto devia devolver vazio: %v", got)
	}
}

// TestLookupDevolveCrescente é a INVARIANTE, não uma preferência de estilo.
//
// O intersectIter e o unionIter do executor assumem iteradores ascendentes, e
// o planner mistura termos hash e bitmap no mesmo IndexUnionPlan. Um iterador
// desordenado faria a interseção k-way parar cedo e devolver menos linhas do
// que existem — sem erro, sem log e sem métrica.
//
// A inserção é feita em ordem embaralhada de propósito: a cadeia de probe
// guarda os pares na ordem em que foram inseridos, então um Lookup que apenas
// varresse a cadeia devolveria exatamente essa ordem embaralhada.
func TestLookupDevolveCrescente(t *testing.T) {
	ix, _ := abre(t, 7)
	rng := rand.New(rand.NewSource(20260825))
	quer := make([]types.RowID, 0, 500)
	for i := 0; i < 500; i++ {
		quer = append(quer, types.RowID(i))
	}
	embaralhado := slices.Clone(quer)
	rng.Shuffle(len(embaralhado), func(i, j int) {
		embaralhado[i], embaralhado[j] = embaralhado[j], embaralhado[i]
	})
	for _, r := range embaralhado {
		if err := ix.Set(txt("mesmo-valor"), r); err != nil {
			t.Fatal(err)
		}
	}
	got := colheIter(t, ix, txt("mesmo-valor"))
	if !slices.Equal(got, quer) {
		if slices.IsSorted(got) {
			t.Fatalf("ordenado mas incompleto: %d de %d rows", len(got), len(quer))
		}
		t.Fatalf("Lookup devolveu fora de ordem: primeiros %v", got[:min(10, len(got))])
	}
}

// TestClearRemoveAPostingDoValorAntigo é o caso do update: a coluna muda de
// valor e a posting antiga TEM de sair, ou a query devolveria a row sob um
// valor que ela não tem mais.
func TestClearRemoveAPostingDoValorAntigo(t *testing.T) {
	ix, _ := abre(t, 7)
	if err := ix.Set(txt("Start"), 4); err != nil {
		t.Fatal(err)
	}
	// Update: a fase de Set roda antes do commit, a de Clear depois.
	if err := ix.Set(txt("Stop"), 4); err != nil {
		t.Fatal(err)
	}
	if err := ix.Clear(txt("Start"), txt("Stop"), 4); err != nil {
		t.Fatal(err)
	}
	if got := colheIter(t, ix, txt("Start")); len(got) != 0 {
		t.Fatalf("a posting do valor ANTIGO sobreviveu ao update: %v", got)
	}
	if got := colheIter(t, ix, txt("Stop")); !slices.Equal(got, []types.RowID{4}) {
		t.Fatalf("a posting do valor novo: %v", got)
	}
}

// TestClearDeCampoQueSaiUsaAusente cobre o outro caller do Clear: o campo
// desaparece da row e não há valor a preservar.
func TestClearDeCampoQueSaiUsaAusente(t *testing.T) {
	ix, _ := abre(t, 7)
	if err := ix.Set(txt("Start"), 4); err != nil {
		t.Fatal(err)
	}
	if err := ix.Clear(txt("Start"), ausente, 4); err != nil {
		t.Fatal(err)
	}
	if got := colheIter(t, ix, txt("Start")); len(got) != 0 {
		t.Fatalf("campo que saiu deixou posting: %v", got)
	}
}

// TestClearUsaLapideEmVezDeEsvaziarOSlot é a diferença estrutural em relação
// ao primary, e o teste ataca o modo de falha e não o mecanismo.
//
// Com a função de hash forçada a mandar TODOS os valores para o mesmo slot
// inicial, os três valores compartilham a cadeia de probe. Remover o do MEIO
// esvaziando o slot cortaria a cadeia, e o terceiro — que está depois dele —
// sumiria para sempre. É falso negativo permanente e silencioso, exatamente o
// que a lápide existe para impedir.
func TestClearUsaLapideEmVezDeEsvaziarOSlot(t *testing.T) {
	ix, _ := abre(t, 7)
	ix.injetaHashParaTeste(func([]byte) uint64 { return 2 })
	for i, v := range []string{"a", "b", "c"} {
		if err := ix.Set(txt(v), types.RowID(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := ix.Clear(txt("b"), ausente, 1); err != nil {
		t.Fatal(err)
	}
	// Com o hash colidido o Lookup devolve SUPERSET: todo valor da cadeia
	// responde por todos os rowIds ainda vivos nela. O que importa aqui é que
	// a row 2 continue ALCANÇÁVEL — ela está DEPOIS do slot removido.
	got := colheIter(t, ix, txt("c"))
	if !slices.Contains(got, 2) {
		t.Fatalf("a row depois do slot removido sumiu da cadeia: %v", got)
	}
	if ix.Lapides() != 1 {
		t.Fatalf("Lapides = %d, esperado 1 (a remoção tem de deixar lápide)", ix.Lapides())
	}
	if ix.Usados() != 2 {
		t.Fatalf("Usados = %d, esperado 2", ix.Usados())
	}
}

// TestClearNaoRemoveAPostingQueOSetAcabouDeAfirmar cobre o único falso
// NEGATIVO possível deste índice, e é o motivo de o Clear receber o valor
// ATUAL.
//
// Cenário: o valor velho e o novo de um update caem no MESMO slot. A fase de
// Set encontra a posting antiga onde a nova iria, conclui que o par já existe
// e não faz nada; sem a guarda, a fase de Clear removeria essa posting e a row
// ficaria invisível ao índice para sempre.
//
// A colisão é forçada pela injeção do hash porque uma colisão real de 64 bits
// não é construível num teste — e um caminho de correção sem oráculo é um
// caminho que a suíte não cobre.
func TestClearNaoRemoveAPostingQueOSetAcabouDeAfirmar(t *testing.T) {
	ix, _ := abre(t, 7)
	ix.injetaHashParaTeste(func([]byte) uint64 { return 2 })
	if err := ix.Set(txt("Start"), 4); err != nil {
		t.Fatal(err)
	}
	if err := ix.Set(txt("Stop"), 4); err != nil {
		t.Fatal(err)
	}
	if err := ix.Clear(txt("Start"), txt("Stop"), 4); err != nil {
		t.Fatal(err)
	}
	if got := colheIter(t, ix, txt("Stop")); !slices.Contains(got, 4) {
		t.Fatalf("a row perdeu a posting do valor CORRENTE num update entre valores que colidem: %v", got)
	}
}

// TestSetEhIdempotente: o replay do tail reaplica os mesmos records depois de
// um crash. Um Set que inserisse de novo deixaria a row duas vezes na cadeia,
// e o unionIter do executor emitiria a mesma linha duas vezes num OR.
func TestSetEhIdempotente(t *testing.T) {
	ix, _ := abre(t, 7)
	for i := 0; i < 5; i++ {
		if err := ix.Set(txt("core-01"), 3); err != nil {
			t.Fatal(err)
		}
	}
	if got := colheIter(t, ix, txt("core-01")); !slices.Equal(got, []types.RowID{3}) {
		t.Fatalf("Set repetido duplicou a posting: %v", got)
	}
	if ix.Usados() != 1 {
		t.Fatalf("Usados = %d, esperado 1", ix.Usados())
	}
}

// TestSetReusaLapideSemDuplicarOPar trava a interação entre as duas metades:
// a inserção prefere a lápide encontrada no caminho, mas só depois de varrer a
// cadeia INTEIRA. Reusar cedo criaria uma segunda posting do par que já estava
// adiante.
func TestSetReusaLapideSemDuplicarOPar(t *testing.T) {
	ix, _ := abre(t, 7)
	ix.injetaHashParaTeste(func([]byte) uint64 { return 2 })
	if err := ix.Set(txt("a"), 0); err != nil {
		t.Fatal(err)
	}
	if err := ix.Set(txt("b"), 1); err != nil {
		t.Fatal(err)
	}
	if err := ix.Clear(txt("a"), ausente, 0); err != nil { // lápide no primeiro slot
		t.Fatal(err)
	}
	if err := ix.Set(txt("b"), 1); err != nil { // o par já existe DEPOIS da lápide
		t.Fatal(err)
	}
	if ix.Usados() != 1 {
		t.Fatalf("Usados = %d, esperado 1: o par foi duplicado na lápide", ix.Usados())
	}
	if err := ix.Set(txt("c"), 2); err != nil { // este sim deve ocupar a lápide
		t.Fatal(err)
	}
	if ix.Lapides() != 0 {
		t.Fatalf("Lapides = %d, esperado 0: a inserção nova devia ter reusado a lápide", ix.Lapides())
	}
}

// TestOcupacaoEhRecontadaNoOpen: o header NÃO guarda contador de ocupação, e
// a razão está no layout — um contador em página mmap não é confiável depois
// de um crash. A recontagem por varredura é a fonte de verdade.
func TestOcupacaoEhRecontadaNoOpen(t *testing.T) {
	dir := t.TempDir()
	path := Path(dir, 7)
	ix, err := Open(path, 7, minSlotCount)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := ix.Set(txt(fmt.Sprintf("v-%d", i)), types.RowID(i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		if err := ix.Clear(txt(fmt.Sprintf("v-%d", i)), ausente, types.RowID(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := ix.Msync(); err != nil {
		t.Fatal(err)
	}
	if err := ix.Close(); err != nil {
		t.Fatal(err)
	}

	ix2, err := Open(path, 7, minSlotCount)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ix2.Close() }()
	if ix2.Usados() != 15 {
		t.Fatalf("Usados após reabrir = %d, esperado 15", ix2.Usados())
	}
	if ix2.Lapides() != 5 {
		t.Fatalf("Lapides após reabrir = %d, esperado 5", ix2.Lapides())
	}
	if got := colheIter(t, ix2, txt("v-10")); !slices.Equal(got, []types.RowID{10}) {
		t.Fatalf("posting não sobreviveu ao restart: %v", got)
	}
	if got := colheIter(t, ix2, txt("v-1")); len(got) != 0 {
		t.Fatalf("posting removida ressuscitou no restart: %v", got)
	}
}

// TestReconstrucaoPurgaLapidesSemCrescerAToa é o defeito de dimensionamento
// que este produto teria sentido primeiro: cada update faz um Set e um Clear,
// então uma coluna muito atualizada acumula LÁPIDES sem ganhar postings. Uma
// tabela que dobrasse a cada limiar cresceria sem limite com o número de
// linhas parado — 8 GiB de arquivo de índice para 1.000 postings vivas.
//
// O cenário é montado com hash INJETADO por uma razão de método: com o hash
// real, o reuso de lápide (medido no
// TestChurnDeUpdatesNaoAcumulaLapidesSemLimite) impede a ocupação efetiva de
// chegar ao limiar, e o caminho que se quer medir nunca roda. O hash injetado
// dá a cada valor um slot PRÓPRIO — 7 é primo com 1024, então v↦2+7v é uma
// bijeção sob a máscara —, de modo que nenhuma inserção reusa lápide e a
// ocupação efetiva sobe de um em um até o limiar, sempre no mesmo ponto.
func TestReconstrucaoPurgaLapidesSemCrescerAToa(t *testing.T) {
	ix, _ := abre(t, 7)
	ix.injetaHashParaTeste(func(b []byte) uint64 {
		return 2 + binary.LittleEndian.Uint64(b[1:9])*7
	})
	slotsIniciais := ix.SlotCount()

	// 700 postings: abaixo do limiar de 70% de 1024 slots.
	const preenchidas = 700
	for i := 0; i < preenchidas; i++ {
		if err := ix.Set(num(int64(i)), types.RowID(i)); err != nil {
			t.Fatal(err)
		}
	}
	if ix.SlotCount() != slotsIniciais {
		t.Fatalf("a tabela cresceu antes do limiar: %d slots", ix.SlotCount())
	}
	// Todas removidas: as postings viram lápides, e a OCUPAÇÃO EFETIVA não
	// muda — é essa a assimetria que faz um índice muito atualizado degradar.
	for i := 0; i < preenchidas; i++ {
		if err := ix.Clear(num(int64(i)), ausente, types.RowID(i)); err != nil {
			t.Fatal(err)
		}
	}
	if ix.Usados() != 0 || ix.Lapides() != preenchidas {
		t.Fatalf("estado esperado 0 vivas / %d lápides, obtido %d / %d", preenchidas, ix.Usados(), ix.Lapides())
	}
	// Agora as inserções empurram a ocupação efetiva até o limiar. Quem cruza
	// é a lápide, não a posting: só 17 estão vivas quando a reconstrução roda.
	for i := preenchidas; i < preenchidas+30; i++ {
		if err := ix.Set(num(int64(i)), types.RowID(i)); err != nil {
			t.Fatal(err)
		}
	}
	if ix.SlotCount() != slotsIniciais {
		t.Fatalf("a tabela cresceu de %d para %d slots com %d postings vivas: a reconstrução está dobrando por lápide",
			slotsIniciais, ix.SlotCount(), ix.Usados())
	}
	if ix.Lapides() >= preenchidas {
		t.Fatalf("Lapides = %d: a reconstrução não purgou as lápides", ix.Lapides())
	}
	for i := preenchidas; i < preenchidas+30; i++ {
		if got := colheIter(t, ix, num(int64(i))); !slices.Contains(got, types.RowID(i)) {
			t.Fatalf("posting %d se perdeu na faxina: %v", i, got)
		}
	}
	for i := 0; i < preenchidas; i++ {
		if got := colheIter(t, ix, num(int64(i))); len(got) != 0 {
			t.Fatalf("posting removida %d ressuscitou na faxina: %v", i, got)
		}
	}
}

// TestChurnDeUpdatesNaoAcumulaLapidesSemLimite mede o que o reuso de lápide
// entrega no perfil real deste produto: uma row trocando de valor sem parar.
//
// O número importa porque decide se a reconstrução é evento raro ou caminho
// quente. MEDIDO: 20.000 updates sobre uma tabela de 1.024 slots com UMA
// posting viva estabilizam em 408 lápides (~40% da tabela) e ZERO
// reconstruções — a inserção encontra lápide na própria cadeia e a reaproveita,
// e o sistema chega a um ponto fixo em que cada Set devolve à tabela a lápide
// que o Clear seguinte cria.
//
// A asserção é sobre o limiar, e não sobre o 408: o valor exato depende da
// dispersão do hash sobre estes valores concretos, e travá-lo faria o teste
// quebrar por uma mudança que não é defeito. O que não pode mudar é a
// CONCLUSÃO — churn puro não leva a tabela ao limiar.
func TestChurnDeUpdatesNaoAcumulaLapidesSemLimite(t *testing.T) {
	ix, _ := abre(t, 7)
	slots := ix.SlotCount()
	valor := func(i int) types.Value { return txt(fmt.Sprintf("estado-%d", i)) }
	if err := ix.Set(valor(0), 1); err != nil {
		t.Fatal(err)
	}
	const updates = 20000
	for i := 1; i <= updates; i++ {
		if err := ix.Set(valor(i), 1); err != nil {
			t.Fatal(err)
		}
		if err := ix.Clear(valor(i-1), valor(i), 1); err != nil {
			t.Fatal(err)
		}
	}
	if ix.SlotCount() != slots {
		t.Fatalf("a tabela cresceu de %d para %d slots com UMA posting viva", slots, ix.SlotCount())
	}
	if ix.Usados() != 1 {
		t.Fatalf("Usados = %d, esperado 1", ix.Usados())
	}
	if limite := slots * loadFactorNum / loadFactorDen; ix.Lapides() >= limite {
		t.Fatalf("Lapides = %d de %d slots: o reuso de lápide parou de funcionar (limiar %d)",
			ix.Lapides(), slots, limite)
	}
	if got := colheIter(t, ix, valor(updates)); !slices.Equal(got, []types.RowID{1}) {
		t.Fatalf("a posting corrente não sobreviveu ao churn: %v", got)
	}
}

// TestReconstrucaoCresceQuandoAsPostingsCrescem é a outra direção: com
// postings de verdade a tabela precisa dobrar, e nada pode se perder no
// caminho.
func TestReconstrucaoCresceQuandoAsPostingsCrescem(t *testing.T) {
	ix, _ := abre(t, 7)
	const n = 4000
	for i := 0; i < n; i++ {
		if err := ix.Set(num(int64(i)), types.RowID(i)); err != nil {
			t.Fatal(err)
		}
	}
	if ix.SlotCount() <= minSlotCount {
		t.Fatalf("SlotCount = %d com %d postings: a tabela não cresceu", ix.SlotCount(), n)
	}
	if ix.Usados() != n {
		t.Fatalf("Usados = %d, esperado %d", ix.Usados(), n)
	}
	for i := 0; i < n; i++ {
		if got := colheIter(t, ix, num(int64(i))); !slices.Contains(got, types.RowID(i)) {
			t.Fatalf("posting %d se perdeu na reconstrução: %v", i, got)
		}
	}
}

// TestReconstrucaoOrfaEhDescartadaNoOpen: um crash no meio da reconstrução
// deixa um ".new" para trás. Adotá-lo seria adotar uma tabela pela metade.
func TestReconstrucaoOrfaEhDescartadaNoOpen(t *testing.T) {
	dir := t.TempDir()
	path := Path(dir, 7)
	ix, err := Open(path, 7, minSlotCount)
	if err != nil {
		t.Fatal(err)
	}
	if err := ix.Set(txt("vive"), 1); err != nil {
		t.Fatal(err)
	}
	if err := ix.Msync(); err != nil {
		t.Fatal(err)
	}
	if err := ix.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".new", []byte("lixo de reconstrução interrompida"), 0o644); err != nil {
		t.Fatal(err)
	}
	ix2, err := Open(path, 7, minSlotCount)
	if err != nil {
		t.Fatalf("Open com .new órfão: %v", err)
	}
	defer func() { _ = ix2.Close() }()
	if _, err := os.Stat(path + ".new"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf(".new órfão sobreviveu ao Open: %v", err)
	}
	if got := colheIter(t, ix2, txt("vive")); !slices.Equal(got, []types.RowID{1}) {
		t.Fatalf("%v", got)
	}
}

// TestFalhaNoFsyncDoDiretorioSobe: o rename já aconteceu e o swap também, mas
// a entrada de diretório pode não estar durável. O erro tem de subir para que
// o Store entre em modo falho — engolir aqui deixaria o índice novo visível e
// não durável.
func TestFalhaNoFsyncDoDiretorioSobe(t *testing.T) {
	ix, _ := abre(t, 7)
	restaura := injetaSyncDirParaTeste(func(string) error { return errors.New("disco de mentira") })
	defer restaura()
	var err error
	for i := 0; i < 4000 && err == nil; i++ {
		err = ix.Set(num(int64(i)), types.RowID(i))
	}
	if err == nil {
		t.Fatal("a falha no fsync do diretório da reconstrução não subiu")
	}
}

// TestHeaderRecusaArquivoDeOutroCampo: dois arquivos trocados de lugar
// produziriam um índice respondendo com as rows do campo errado, e a
// revalidação do §44 filtraria tudo — a query devolveria vazio sem erro em
// camada nenhuma.
func TestHeaderRecusaArquivoDeOutroCampo(t *testing.T) {
	dir := t.TempDir()
	ix, err := Open(Path(dir, 7), 7, minSlotCount)
	if err != nil {
		t.Fatal(err)
	}
	if err := ix.Set(txt("x"), 1); err != nil {
		t.Fatal(err)
	}
	if err := ix.Msync(); err != nil {
		t.Fatal(err)
	}
	if err := ix.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(Path(dir, 7), Path(dir, 9)); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Path(dir, 9), 9, minSlotCount); err == nil {
		t.Fatal("o índice do campo 7 foi aceito como sendo do campo 9")
	}
}

// TestHeaderRecusaMagicEVersaoDesconhecidas cobre as demais adulterações do
// header.
func TestHeaderRecusaMagicEVersaoDesconhecidas(t *testing.T) {
	casos := []struct {
		nome  string
		muta  func([]byte)
		campo types.FieldID
	}{
		{"magic", func(b []byte) { b[0] = 'X' }, 7},
		{"version", func(b []byte) { b[4] = 9 }, 7},
		{"hashKind", func(b []byte) { b[5] = 9 }, 7},
		{"slotCount não potência de 2", func(b []byte) { binary.LittleEndian.PutUint64(b[8:16], 1000) }, 7},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			dir := t.TempDir()
			path := Path(dir, c.campo)
			ix, err := Open(path, c.campo, minSlotCount)
			if err != nil {
				t.Fatal(err)
			}
			if err := ix.Msync(); err != nil {
				t.Fatal(err)
			}
			if err := ix.Close(); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			c.muta(raw)
			if err := os.WriteFile(path, raw, 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(path, c.campo, minSlotCount); err == nil {
				t.Fatalf("header com %s adulterado foi aceito", c.nome)
			}
		})
	}
}

// TestArquivoNoDiscoTemOLayoutDeclarado ancora o header em bytes: ele é
// formato persistente, e mudá-lo torna todo índice existente ilegível.
func TestArquivoNoDiscoTemOLayoutDeclarado(t *testing.T) {
	dir := t.TempDir()
	ix, err := Open(Path(dir, 4242), 4242, minSlotCount)
	if err != nil {
		t.Fatal(err)
	}
	if err := ix.Msync(); err != nil {
		t.Fatal(err)
	}
	if err := ix.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "04242.hidx"))
	if err != nil {
		t.Fatalf("o nome do arquivo mudou: %v", err)
	}
	if string(raw[0:4]) != "NDBH" {
		t.Fatalf("magic: %q", raw[0:4])
	}
	if raw[4] != 1 {
		t.Fatalf("version: %d", raw[4])
	}
	if raw[5] != 1 {
		t.Fatalf("hashKind: %d", raw[5])
	}
	if got := binary.LittleEndian.Uint16(raw[6:8]); got != 4242 {
		t.Fatalf("fieldID: %d", got)
	}
	if got := binary.LittleEndian.Uint64(raw[8:16]); got != minSlotCount {
		t.Fatalf("slotCount: %d", got)
	}
	if int64(len(raw)) != fileSizeFor(minSlotCount) {
		t.Fatalf("tamanho do arquivo: %d, esperado %d", len(raw), fileSizeFor(minSlotCount))
	}
}

// TestValorNaoIndexavelEhRecusado: NULL é ausência, não valor. Aceitá-lo
// criaria uma posting para "sem valor", que nenhum predicado de igualdade
// procura.
func TestValorNaoIndexavelEhRecusado(t *testing.T) {
	ix, _ := abre(t, 7)
	if err := ix.Set(ausente, 1); err == nil {
		t.Fatal("Set de TypeNull devia ser recusado")
	}
	if _, err := ix.Lookup(ausente); err == nil {
		t.Fatal("Lookup de TypeNull devia ser recusado")
	}
}

// TestFieldIDZeroEhRecusado: o id 0 é reservado no dicionário de campos, e um
// índice sobre ele nomearia um campo que não existe.
func TestFieldIDZeroEhRecusado(t *testing.T) {
	if _, err := Open(Path(t.TempDir(), 0), 0, minSlotCount); err == nil {
		t.Fatal("fieldID 0 devia ser recusado")
	}
}

func BenchmarkSet(b *testing.B) {
	dir := b.TempDir()
	ix, err := Open(Path(dir, 7), 7, uint64(b.N)*2)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = ix.Close() }()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ix.Set(num(int64(i)), types.RowID(i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLookup(b *testing.B) {
	dir := b.TempDir()
	const n = 100000
	ix, err := Open(Path(dir, 7), 7, n*2)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = ix.Close() }()
	for i := 0; i < n; i++ {
		if err := ix.Set(num(int64(i)), types.RowID(i)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it, err := ix.Lookup(num(int64(i % n)))
		if err != nil {
			b.Fatal(err)
		}
		for it.Next() {
			_ = it.RowID()
		}
		_ = it.Close()
	}
}
