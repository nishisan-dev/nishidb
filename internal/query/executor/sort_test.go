package executor_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/query/planner"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// bqOrdenada é a query ligada mínima com ORDER BY sobre `nome`.
func bqOrdenada(t *testing.T, c *schema.Compiled, maxRows int64) *binder.BoundQuery {
	t.Helper()
	id, nome := campo(t, c, "id"), campo(t, c, "nome")
	return &binder.BoundQuery{
		Dataset: "t",
		Schema:  c,
		Columns: []binder.BoundColumn{{Field: id, Name: "id", Slot: 0}},
		Slots:   binder.Slots{IDs: []types.FieldID{id.ID, nome.ID}},
		OrderBy: []binder.BoundOrderKey{{
			Field: nome, Slot: 1, Kind: binder.CompareBytes, Want: types.TypeString}},
		MaxRows:        maxRows,
		EffectiveLimit: maxRows,
	}
}

// O orçamento de ordenação RECUSA a query, e não a trunca.
//
// Truncar a ENTRADA do sort daria "as N primeiras por rowId, depois ordenadas",
// que não é prefixo de resposta nenhuma — é resultado errado com cara de certo.
// O teto é forçado por Options porque essa é a única porta que exercita a
// recusa sem gerar um corpus de dezenas de megabytes.
func TestOrcamentoDeOrdenacaoRecusaENaoTrunca(t *testing.T) {
	c := compilaSchema(t)
	st := semeiaParaOrdenar(t, c)
	it, err := executor.Open(context.Background(), bqOrdenada(t, c, 100), planner.FullScan(),
		executor.Options{Store: st, SortMemory: 8})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := it.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	for it.Next() {
	}
	if it.Err() == nil {
		t.Fatal("o teto foi estourado e a query NÃO falhou: o resultado saiu truncado")
	}
	if !errors.Is(it.Err(), executor.ErrOrcamentoDeOrdenacao) {
		t.Errorf("erro = %v, quer ErrOrcamentoDeOrdenacao", it.Err())
	}
}

// A MESMA query com teto folgado responde inteira e ORDENADA. Sem este, o teste
// acima passaria com um Open quebrado — qualquer erro serviria.
func TestOrcamentoFolgadoOrdenaTudo(t *testing.T) {
	c := compilaSchema(t)
	st := semeiaParaOrdenar(t, c)
	it, err := executor.Open(context.Background(), bqOrdenada(t, c, 100), planner.FullScan(),
		executor.Options{Store: st, SortMemory: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := it.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	var ids []string
	for it.Next() {
		ids = append(ids, string(it.Row().Cols()[0].Val.Bytes))
	}
	if it.Err() != nil {
		t.Fatalf("Err: %v", it.Err())
	}
	// nome: c=alfa, a=beta, b=gama ⇒ ordem por nome é c, a, b.
	if len(ids) != 3 || ids[0] != "c" || ids[1] != "a" || ids[2] != "b" {
		t.Errorf("ids = %v, quer [c a b] (ordem por nome)", ids)
	}
}

func semeiaParaOrdenar(t *testing.T, c *schema.Compiled) *fakestorage.Store {
	t.Helper()
	st := fakestorage.New(fakestorage.Indexes{})
	id, nome := campo(t, c, "id"), campo(t, c, "nome")
	for _, r := range []struct{ k, n string }{{"a", "beta"}, {"b", "gama"}, {"c", "alfa"}} {
		var p []byte
		var err error
		primeiro, segundo := id, nome
		vp := types.Value{Type: types.TypeString, Bytes: []byte(r.k)}
		vs := types.Value{Type: types.TypeString, Bytes: []byte(r.n)}
		if segundo.ID < primeiro.ID {
			primeiro, segundo = segundo, primeiro
			vp, vs = vs, vp
		}
		if p, err = encoding.AppendField(p, primeiro.ID, vp); err != nil {
			t.Fatal(err)
		}
		if p, err = encoding.AppendField(p, segundo.ID, vs); err != nil {
			t.Fatal(err)
		}
		if _, _, err := st.Put([]byte(r.k), 1, 0, p, nil); err != nil {
			t.Fatal(err)
		}
	}
	return st
}

// storeComSpill é o dublê com a capability de transbordo: sem ela, o executor
// RECUSA ao estourar o orçamento em vez de degradar para disco.
type storeComSpill struct {
	*fakestorage.Store
	dir string
}

func (s storeComSpill) DiretorioDeSpill() string { return s.dir }

// TestSortExternoDaOMesmoResultadoQueOSortEmMemoria é o teste que decide se o
// transbordo está certo.
//
// A MESMA query, sobre os MESMOS dados, com dois orçamentos: um folgado, que
// ordena tudo em memória num lote só, e um minúsculo, que força vários runs em
// disco e um merge k-way. Os dois resultados têm de ser IDÊNTICOS, linha a
// linha — não "equivalentes", idênticos, porque o desempate por rowID torna a
// ordem total e determinística nos dois caminhos.
//
// Sem este diferencial, o sort externo seria um segundo produto: passaria nos
// testes próprios e divergiria do outro exatamente nos empates, que é onde
// ninguém olha.
func TestSortExternoDaOMesmoResultadoQueOSortEmMemoria(t *testing.T) {
	c := compilaSchema(t)
	st := semeiaMuitos(t, c, 200)

	emMemoria := colheOrdenado(t, c, st, "", 1<<20)

	// PROVA DE QUE O TETO DE 512 REALMENTE TRANSBORDA. Sem esta asserção o
	// diferencial abaixo passaria por vacuidade se o orçamento nunca fosse
	// estourado: os dois caminhos seriam o mesmo caminho. O mesmo teto, num
	// store SEM a capability de spill, tem de RECUSAR.
	itSem, err := executor.Open(context.Background(), bqOrdenada(t, c, 10_000), planner.FullScan(),
		executor.Options{Store: st, SortMemory: 512})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for itSem.Next() {
	}
	if !errors.Is(itSem.Err(), executor.ErrOrcamentoDeOrdenacao) {
		t.Fatalf("com teto de 512 e sem spill, o erro foi %v — o teto não é apertado o bastante "+
			"e o diferencial abaixo não exercitaria o transbordo", itSem.Err())
	}
	if err := itSem.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	comSpill := colheOrdenado(t, c, storeComSpill{Store: st, dir: t.TempDir()}, t.TempDir(), 512)

	if len(comSpill) != len(emMemoria) {
		t.Fatalf("spill devolveu %d linhas, memória devolveu %d", len(comSpill), len(emMemoria))
	}
	if len(emMemoria) != 200 {
		t.Fatalf("%d linhas, quer 200: a montagem não produziu o corpus", len(emMemoria))
	}
	for i := range emMemoria {
		if emMemoria[i] != comSpill[i] {
			t.Fatalf("divergiram na linha %d: memória=%q spill=%q", i, emMemoria[i], comSpill[i])
		}
	}
}

// E o transbordo REMOVE os arquivos ao fim, inclusive quando a query termina
// normalmente. Um run esquecido ficaria em disco até o restart do processo.
func TestSpillEhRemovidoAoFecharOIterador(t *testing.T) {
	c := compilaSchema(t)
	dir := t.TempDir()
	st := storeComSpill{Store: semeiaMuitos(t, c, 200), dir: dir}
	_ = colheOrdenado(t, c, st, dir, 512)

	entradas, err := os.ReadDir(filepath.Join(dir, "spill"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(entradas) != 0 {
		t.Errorf("%d arquivo(s) de spill sobraram: %v", len(entradas), entradas)
	}
}

// colheOrdenado executa a query ordenada e devolve os ids na ordem de saída.
func colheOrdenado(t *testing.T, c *schema.Compiled, st executor.Store, dirSpill string, teto int64) []string {
	t.Helper()
	it, err := executor.Open(context.Background(), bqOrdenada(t, c, 10_000), planner.FullScan(),
		executor.Options{Store: st, SortMemory: teto})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := it.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	var ids []string
	for it.Next() {
		ids = append(ids, string(it.Row().Cols()[0].Val.Bytes))
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Err (teto=%d, dir=%q): %v", teto, dirSpill, err)
	}
	return ids
}

// semeiaMuitos produz n linhas com nomes que NÃO seguem a ordem de rowID — sem
// isso o teste passaria com um sort que não ordena nada.
func semeiaMuitos(t *testing.T, c *schema.Compiled, n int) *fakestorage.Store {
	t.Helper()
	st := fakestorage.New(fakestorage.Indexes{})
	id, nome := campo(t, c, "id"), campo(t, c, "nome")
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%04d", i)
		// Nome em ordem INVERSA à da chave, com empates a cada 10 linhas para
		// exercitar o desempate por rowID nos dois caminhos.
		v := fmt.Sprintf("nome-%04d", (n-i)/10)
		var p []byte
		var err error
		primeiro, segundo := id, nome
		vp := types.Value{Type: types.TypeString, Bytes: []byte(k)}
		vs := types.Value{Type: types.TypeString, Bytes: []byte(v)}
		if segundo.ID < primeiro.ID {
			primeiro, segundo = segundo, primeiro
			vp, vs = vs, vp
		}
		if p, err = encoding.AppendField(p, primeiro.ID, vp); err != nil {
			t.Fatal(err)
		}
		if p, err = encoding.AppendField(p, segundo.ID, vs); err != nil {
			t.Fatal(err)
		}
		if _, _, err := st.Put([]byte(k), 1, 0, p, nil); err != nil {
			t.Fatal(err)
		}
	}
	return st
}
