package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/storage/rowindex"
	"github.com/nishisan-dev/nishidb/internal/storage/segment"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// openHistoryIndexedIn abre — ou REABRE — o dataset histórico em dir.
//
// Difere do openStoreHistoryIndexed por receber o diretório e o dataset de
// base, e as duas coisas são o que permite fechar e reabrir o MESMO dataset
// dentro de um teste: o histórico não tem dicionário próprio por desenho, então
// reabri-lo com um base novo daria outro mapeamento nome→id e as postings
// gravadas na primeira abertura passariam a responder por outro campo.
func openHistoryIndexedIn(t *testing.T, base *Store, dir string, fields []string, secSize int64) *Store {
	t.Helper()
	return openStore(t, Options{
		Dir:                   dir,
		Dataset:               "events_history",
		SegmentSize:           secSize,
		Mode:                  HistoryMode,
		SharedDictionary:      base.FieldDict(),
		WriteInstantFieldName: "_writtenAt",
		IndexedFieldsByName:   fields,
	})
}

// candidatosCrus drena o Lookup SEM revalidar nada.
//
// É o oráculo de "este segmento respondeu por índice ou por enumeração": o
// resultado revalidado é igual nos dois casos — é justamente a tese da tarefa —,
// e só o conjunto cru distingue "a seção respondeu zero linhas" de "o segmento
// inteiro virou candidato".
func candidatosCrus(t *testing.T, s *Store, field types.FieldID, v types.Value) []types.RowID {
	t.Helper()
	it, err := s.Lookup(field, v)
	if err != nil {
		t.Fatalf("Lookup(%d): %v", field, err)
	}
	return collectRows(t, it)
}

// lookupRevalidado drena o Lookup e aplica o MESMO filtro que o executor
// aplicaria (§44): liveness pelo GetByRowID e o predicado inteiro sobre o
// payload. É o que o produto realmente devolve.
func lookupRevalidado(t *testing.T, s *Store, field types.FieldID, v types.Value) []types.RowID {
	t.Helper()
	var out []types.RowID
	var buf []byte
	for _, id := range candidatosCrus(t, s, field, v) {
		if len(out) > 0 && out[len(out)-1] >= id {
			t.Fatalf("o Lookup devolveu %d depois de %d: a ordem tem de ser estritamente crescente",
				id, out[len(out)-1])
		}
		ok, b := rowCasa(t, s, id, field, v, buf)
		buf = b
		if ok {
			out = append(out, id)
		}
	}
	return out
}

// fullScanFiltrado é a resposta de referência: a varredura que o planner faria
// sem índice nenhum. Ela é o oráculo desta tarefa inteira — o Lookup revalidado
// tem de coincidir com ela, com índice ou sem.
func fullScanFiltrado(t *testing.T, s *Store, field types.FieldID, v types.Value) []types.RowID {
	t.Helper()
	var out []types.RowID
	var buf []byte
	it := s.ScanLive(s.presente())
	defer func() { _ = it.Close() }()
	for it.Next() {
		ok, b := rowCasa(t, s, it.RowID(), field, v, buf)
		buf = b
		if ok {
			out = append(out, it.RowID())
		}
	}
	if err := it.Err(); err != nil {
		t.Fatalf("ScanLive: %v", err)
	}
	return out
}

// rowCasa devolve se a row está viva e tem o campo com o valor.
func rowCasa(t *testing.T, s *Store, id types.RowID, field types.FieldID, v types.Value, buf []byte) (bool, []byte) {
	t.Helper()
	rec, out, ok, err := s.GetByRowID(s.presente(), id, buf)
	if err != nil {
		t.Fatalf("GetByRowID(%d): %v", id, err)
	}
	if !ok {
		return false, out
	}
	var r encoding.FieldReader
	r.Reset(rec.Payload)
	for {
		fid, val, ok := r.Next()
		if !ok {
			if err := r.Err(); err != nil {
				t.Fatalf("payload da row %d: %v", id, err)
			}
			return false, out
		}
		if fid == field && valueEqual(val, v) {
			return true, out
		}
	}
}

// TestSegmentoSemIndiceRespondePorEnumeracao é a resposta da spec à Q3: o
// contrato do Lookup já é superconjunto com revalidação, então enumerar as
// linhas de um segmento sem índice é uma resposta CORRETA — só lenta. No
// histórico isso torna "fora da janela" e "gravado por binário anterior" o
// mesmo estado, e é o que faz a migração não exigir recriar dataset.
func TestSegmentoSemIndiceRespondePorEnumeracao(t *testing.T) {
	base := openStore(t, Options{})
	dir := t.TempDir()
	s := openHistoryIndexedIn(t, base, dir, []string{"node"}, 8<<10)
	target := s.curSecID.Load()
	poeRowsUntilRotacionar(t, s, target)
	poeRows(t, s, 30)
	field := fieldOfDicionario(t, s, "node")
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simula o dataset antigo: o segmento existe, o índice não.
	if err := os.Remove(segindex.SealedPath(filepath.Join(dir, "data"), target)); err != nil {
		t.Fatal(err)
	}
	s2 := openHistoryIndexedIn(t, base, dir, []string{"node"}, 8<<10)

	expected := fullScanFiltrado(t, s2, field, strVal("n1"))
	if len(expected) == 0 {
		t.Fatal("pré-condição: o corpus não tem nenhuma linha com node=n1, o teste não exercita nada")
	}
	rows := lookupRevalidado(t, s2, field, strVal("n1"))
	if !slices.Equal(rows, expected) {
		t.Errorf("Lookup por enumeração = %v, quero o mesmo que o full scan filtrado = %v", rows, expected)
	}
}

// TestSecaoAusenteNoSeladoEnumeraOSegmento cobre a distinção que decide a
// correção da tarefa: o ok=false do Sealed.Lookup NÃO é "valor ausente", é
// "este segmento não responde por este campo". Aqui o .sidx existe, está
// íntegro e tem seção para OUTRO campo — o estado em que fica um segmento
// entre acrescentar um campo ao schema e a reconstrução alcançá-lo (§2.6).
//
// Tratar esse ok=false como zero linhas seria FALSO NEGATIVO, e é a única
// coisa que a revalidação do §44 não sabe consertar: ela só descarta candidato
// a mais.
func TestSecaoAusenteNoSeladoEnumeraOSegmento(t *testing.T) {
	base := openStore(t, Options{})
	dir := t.TempDir()
	s := openHistoryIndexedIn(t, base, dir, []string{"node"}, 8<<10)
	target := s.curSecID.Load()
	poeRowsUntilRotacionar(t, s, target)
	fieldNode := fieldOfDicionario(t, s, "node")
	fieldType := fieldOfDicionario(t, s, "type")
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Troca o .sidx do segmento por um que só tem seção de `type`: é o arquivo
	// que a reconstrução teria produzido antes de `node` entrar no schema.
	path := segindex.SealedPath(filepath.Join(dir, "data"), target)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	e := segindex.NewWriter()
	e.Add(fieldType, hashIndexed(t, types.Value{Type: types.TypeInt64, I64: 0}), 0)
	if err := e.WriteSealed(path, target); err != nil {
		t.Fatal(err)
	}

	s2 := openHistoryIndexedIn(t, base, dir, []string{"node"}, 8<<10)
	expected := fullScanFiltrado(t, s2, fieldNode, strVal("n1"))
	if len(expected) == 0 {
		t.Fatal("pré-condição: o corpus não tem nenhuma linha com node=n1")
	}
	rows := lookupRevalidado(t, s2, fieldNode, strVal("n1"))
	if !slices.Equal(rows, expected) {
		t.Errorf("com a seção de node ausente o Lookup deu %v, quero o full scan filtrado %v", rows, expected)
	}
}

// TestValorAusenteNaSecaoNaoEnumeraOSegmento é a outra metade da distinção
// acima, e sem ela a primeira passaria com um motor que enumera SEMPRE: seção
// presente e valor nunca visto é ok=true com fonte vazia, e o segmento
// contribui com ZERO candidatos.
//
// O observável é o conjunto CRU: revalidado, os dois desfechos são o mesmo
// vazio, e o teste não distinguiria nada.
func TestValorAusenteNaSecaoNaoEnumeraOSegmento(t *testing.T) {
	base := openStore(t, Options{})
	dir := t.TempDir()
	s := openHistoryIndexedIn(t, base, dir, []string{"node"}, 8<<10)
	target := s.curSecID.Load()
	poeRowsUntilRotacionar(t, s, target)
	field := fieldOfDicionario(t, s, "node")
	if _, err := os.Stat(segindex.SealedPath(filepath.Join(dir, "data"), target)); err != nil {
		t.Fatalf("pré-condição: o segmento %d deveria estar selado: %v", target, err)
	}

	if crus := candidatosCrus(t, s, field, strVal("nunca-visto")); len(crus) != 0 {
		t.Errorf("valor ausente devolveu %d candidatos: a seção existe e respondeu, ninguém devia enumerar", len(crus))
	}
}

// TestAEnumeracaoDoHistoricoUsaAFaixaDeRowIds: no histórico os appends são
// serializados pelo mutex do writer e o rowId é monotônico, então os segmentos
// particionam o espaço de rowId em faixas contíguas. A fonte de enumeração
// explora exatamente isso — a mesma propriedade que o primeiroRowIDOfSeguinte
// da retenção já usa —, e por isso ela não lê record nenhum.
//
// O oráculo isola a fonte perguntando por um valor que NÃO existe: os segmentos
// indexados respondem zero, e o que sobra é a faixa que o segmento sem índice
// enumerou.
func TestAEnumeracaoDoHistoricoUsaAFaixaDeRowIds(t *testing.T) {
	base := openStore(t, Options{})
	dir := t.TempDir()
	s := openHistoryIndexedIn(t, base, dir, []string{"node"}, 8<<10)
	target := s.curSecID.Load()
	poeRowsUntilRotacionar(t, s, target)
	poeRows(t, s, 30)
	field := fieldOfDicionario(t, s, "node")
	expected := rowsOfSegment(t, s, target)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := os.Remove(segindex.SealedPath(filepath.Join(dir, "data"), target)); err != nil {
		t.Fatal(err)
	}
	s2 := openHistoryIndexedIn(t, base, dir, []string{"node"}, 8<<10)

	crus := candidatosCrus(t, s2, field, strVal("valor-que-nunca-existiu"))
	if !slices.Equal(crus, expected) {
		t.Errorf("a enumeração do histórico deu %v, quero a faixa do segmento %v", crus, expected)
	}
}

// rowsOfSegment lê os rowIds que moram no segmento pelos PRÓPRIOS records. É
// o oráculo caro que a fonte de enumeração existe para não pagar: se a faixa
// derivada divergir disto, a premissa de contiguidade do histórico é falsa.
func rowsOfSegment(t *testing.T, s *Store, sec uint32) []types.RowID {
	t.Helper()
	var out []types.RowID
	if _, err := s.secR.ScanFrom(sec, int64(segment.HeaderSize), func(_ uint32, rec types.Record) error {
		out = append(out, rec.RowID)
		return nil
	}); err != nil {
		t.Fatalf("ScanFrom(%d): %v", sec, err)
	}
	return out
}

// TestAEnumeracaoDoMaterializadoColapsaNumaCaminhadaUnicaDoLocations: no
// materializado, UM segmento sem índice colapsa a consulta inteira numa
// caminhada só do locations.idx — cada rowId com location publicada sai UMA
// vez, em ordem, more onde morar. O colapso é a correção de uma corrida: a
// enumeração por dono da location decidia a pertinência no instante da
// iteração, enquanto o WIP do corrente era snapshot da construção, e a linha
// que um update concorrente movesse entre os dois instantes sumia das duas
// fontes — falso negativo, que a revalidação do §44 não conserta.
//
// O oráculo pergunta por um valor que NÃO existe em lugar nenhum: com o
// colapso, o conjunto cru é TODAS as linhas com location — a fonte de
// candidatos de um full scan —, e não só as do segmento sem índice.
func TestAEnumeracaoDoMaterializadoColapsaNumaCaminhadaUnicaDoLocations(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir, SegmentSize: 8 << 10, IndexedFieldsByName: []string{"node"}})
	target := s.curSecID.Load()
	poeRowsUntilRotacionar(t, s, target)
	// Os updates espalham rowIds antigos pelo segmento novo: é essa mistura que
	// torna a faixa contígua do histórico inválida aqui.
	for i := range 40 {
		key := "rot" + strconv.Itoa(i)
		prev, _, ok, err := s.GetByKey(s.presente(), []byte(key), nil)
		if err != nil || !ok {
			t.Fatalf("GetByKey(%s): %v (ok=%v)", key, err, ok)
		}
		mustPut(t, s, key, int64(1000+i), rowIndexada(t, s, key, "atualizado", int64(i%3)), &prev)
	}
	field := fieldOfDicionario(t, s, "node")
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := os.Remove(segindex.SealedPath(filepath.Join(dir, "data"), target)); err != nil {
		t.Fatal(err)
	}
	s2 := openStore(t, Options{Dir: dir, SegmentSize: 8 << 10, IndexedFieldsByName: []string{"node"}})

	noTarget := rowsWithLocationInSegment(t, s2, target)
	if len(noTarget) == 0 {
		t.Fatal("pré-condição: nenhuma location aponta para o segmento sem índice")
	}
	expected := todasAsRowsWithLocation(t, s2)
	refs, err := s2.listaSegmentos()
	if err != nil {
		t.Fatalf("listaSegmentos: %v", err)
	}
	beforeEnum, beforeIDx := s2.segByEnumeracao.Load(), s2.segByIndice.Load()
	crus := candidatosCrus(t, s2, field, strVal("valor-que-nunca-existiu"))
	if !slices.Equal(crus, expected) {
		t.Errorf("a enumeração colapsada deu %d candidatos, quero as %d rows com location publicada",
			len(crus), len(expected))
	}
	// A métrica do colapso: TODOS os segmentos contam como enumerados — é a
	// única visão da degradação, e contar os que "teriam" respondido por
	// índice a esconderia. Sem esta asserção, trocar o Add(len(refs)) por
	// Add(1) (ou por nada) passava pela suíte inteira.
	if got := s2.segByEnumeracao.Load() - beforeEnum; got != int64(len(refs)) {
		t.Errorf("secByEnumeration subiu %d no colapso, quero %d (todos os segmentos)", got, len(refs))
	}
	if got := s2.segByIndice.Load() - beforeIDx; got != 0 {
		t.Errorf("secByIndex subiu %d no colapso, quero 0", got)
	}
}

// rowsWithLocationInSegment diz quais rowIds moram no segmento — só serve à
// PRÉ-CONDIÇÃO dos testes (o segmento sem índice tem linha a perder); a fonte
// do produto não filtra por segmento desde o colapso.
func rowsWithLocationInSegment(t *testing.T, s *Store, sec uint32) []types.RowID {
	t.Helper()
	var out []types.RowID
	for id := s.RowIDBase(); id < types.RowID(s.nextRowID.Load()); id++ {
		packed, ok := s.locs.Get(id)
		if !ok {
			continue
		}
		if dono, _, _ := rowindex.Unpack(packed); dono == sec {
			out = append(out, id)
		}
	}
	return out
}

// todasAsRowsWithLocation é o conjunto de referência da enumeração colapsada:
// todo rowId com location publicada, em ordem crescente, lido pela MESMA
// estrutura que a fonte caminha.
func todasAsRowsWithLocation(t *testing.T, s *Store) []types.RowID {
	t.Helper()
	var out []types.RowID
	for id := s.RowIDBase(); id < types.RowID(s.nextRowID.Load()); id++ {
		if _, ok := s.locs.Get(id); ok {
			out = append(out, id)
		}
	}
	return out
}

// TestOResultadoNaoMudaComOSegmentoIndexadoOuNao é a afirmação mais forte da
// tarefa e o oráculo da migração: a MESMA consulta sobre o MESMO dado devolve o
// MESMO conjunto, indexado ou enumerado. A diferença é só de custo.
func TestOResultadoNaoMudaComOSegmentoIndexadoOuNao(t *testing.T) {
	for _, caso := range []struct {
		name    string
		history bool
	}{{"materializado", false}, {"historico", true}} {
		t.Run(caso.name, func(t *testing.T) {
			base := openStore(t, Options{})
			dir := t.TempDir()
			open := func() *Store {
				if caso.history {
					return openHistoryIndexedIn(t, base, dir, []string{"node"}, 8<<10)
				}
				return openStore(t, Options{Dir: dir, SegmentSize: 8 << 10, IndexedFieldsByName: []string{"node"}})
			}
			s := open()
			poeRowsUntilRotacionar(t, s, s.curSecID.Load())
			poeRowsUntilRotacionar(t, s, s.curSecID.Load())
			poeRows(t, s, 30)
			field := fieldOfDicionario(t, s, "node")

			values := []string{"n0", "n1", "n2", "n3", "nunca-visto"}
			withIndex := make([][]types.RowID, len(values))
			for i, v := range values {
				withIndex[i] = lookupRevalidado(t, s, field, strVal(v))
			}
			if err := s.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			// Apaga TODOS os índices selados: é o dataset gravado pelo binário
			// anterior, que a migração precisa responder sem recriar nada.
			deleteTodosOsSelados(t, filepath.Join(dir, "data"))
			s2 := open()
			for i, v := range values {
				withoutIndice := lookupRevalidado(t, s2, field, strVal(v))
				if !slices.Equal(withoutIndice, withIndex[i]) {
					t.Errorf("node=%s: sem índice deu %v, com índice deu %v", v, withoutIndice, withIndex[i])
				}
			}
			// E o corpus tem de exercitar alguma coisa: um dataset em que toda
			// consulta devolve vazio passaria neste teste sem provar nada.
			var total int
			for _, r := range withIndex {
				total += len(r)
			}
			if total == 0 {
				t.Fatal("pré-condição: nenhuma das consultas devolveu linha")
			}
		})
	}
}

// deleteTodosOsSelados remove todo .sidx do diretório de dados.
func deleteTodosOsSelados(t *testing.T, dir string) {
	t.Helper()
	entradas, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var apagados int
	for _, e := range entradas {
		if filepath.Ext(e.Name()) != ".sidx" {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
			t.Fatal(err)
		}
		apagados++
	}
	if apagados == 0 {
		t.Fatal("pré-condição: nenhum .sidx para apagar, o teste não compara nada")
	}
}

// TestUpdateQueCruzaSegmentoLevaAPostingDoCampoInalterado fecha o ÚNICO falso
// negativo que este motor poderia ter, e por isso é teste permanente e não
// sonda.
//
// A linha nasce no segmento N com node=estavel; um update que mexe SÓ no `type`
// a regrava no segmento N+1. Se o Set indexasse apenas o campo que mudou — que
// é o que o motor global fazia, porque lá a posting não pertence a segmento
// nenhum —, a única posting de node ficaria no segmento ANTIGO e morreria com
// ele na compaction ou na retenção. A linha sumiria de `WHERE node='estavel'`
// sem erro e sem log, e a revalidação do §44 não recupera candidato que o
// índice não devolveu.
//
// O oráculo é o índice do segmento de DESTINO, e não o resultado da consulta:
// enquanto o segmento de origem existe, a posting velha responde e a consulta
// passa mesmo com o defeito presente. É a diferença entre provar a propriedade e
// medir a coincidência.
func TestUpdateQueCruzaSegmentoLevaAPostingDoCampoInalterado(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir, SegmentSize: 8 << 10, IndexedFieldsByName: []string{"node", "type"}})
	fieldNode := fieldOfDicionario(t, s, "node")

	row, _ := mustPut(t, s, "viajante", 1, rowIndexada(t, s, "viajante", "estavel", 1), nil)
	source := s.curSecID.Load()
	poeRowsUntilRotacionar(t, s, source)
	destination := s.curSecID.Load()
	if destination == source {
		t.Fatal("pré-condição: o segmento não rotacionou, o update não cruza fronteira nenhuma")
	}

	// O update mexe SÓ no type; node continua "estavel".
	prev, _, ok, err := s.GetByKey(s.presente(), []byte("viajante"), nil)
	if err != nil || !ok {
		t.Fatalf("GetByKey: %v (ok=%v)", err, ok)
	}
	nw, criado := mustPut(t, s, "viajante", 2, rowIndexada(t, s, "viajante", "estavel", 9), &prev)
	if criado || nw != row {
		t.Fatalf("o update criou row nova (%d → %d, criado=%v)", row, nw, criado)
	}
	if s.curSecID.Load() != destination {
		t.Fatalf("o update rotacionou de novo (%d), o teste perdeu o alvo", s.curSecID.Load())
	}

	// O WIP do segmento de destino tem de conhecer a row pelo campo que NÃO
	// mudou. É aqui que o defeito apareceria.
	postings := s.wips[fieldNode].Lookup(hashIndexed(t, strVal("estavel")))
	if !slices.Contains(postings, row) {
		t.Errorf("o índice do segmento de destino %d não tem a row %d por node=estavel: %v — a posting do campo inalterado não migrou com o record",
			destination, row, postings)
	}
	// E de ponta a ponta, que é o que o produto devolve.
	if rows := lookupRevalidado(t, s, fieldNode, strVal("estavel")); !slices.Contains(rows, row) {
		t.Errorf("WHERE node='estavel' não devolveu a row %d: %v", row, rows)
	}
}

// TestAPostingDoCampoInalteradoSobreviveASelagemEAoRestart é a segunda metade:
// o segmento de destino é SELADO e o dataset reaberto, então a asserção deixa de
// ler o WIP em memória e passa a ler o .sidx em disco.
//
// A distinção importa porque as duas formas do índice são alimentadas por
// caminhos diferentes — o WIP pelo aplicaIndexSets da ingestão, o selado pelo
// Escritor da selagem —, e um defeito que existisse só na travessia WIP→selado
// seria invisível ao teste anterior.
func TestAPostingDoCampoInalteradoSobreviveASelagemEAoRestart(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir, SegmentSize: 8 << 10, IndexedFieldsByName: []string{"node", "type"}})
	fieldNode := fieldOfDicionario(t, s, "node")

	row, _ := mustPut(t, s, "viajante", 1, rowIndexada(t, s, "viajante", "estavel", 1), nil)
	origem := s.curSecID.Load()
	poeRowsUntilRotacionar(t, s, origem)
	destination := s.curSecID.Load()

	prev, _, ok, err := s.GetByKey(s.presente(), []byte("viajante"), nil)
	if err != nil || !ok {
		t.Fatalf("GetByKey: %v (ok=%v)", err, ok)
	}
	mustPut(t, s, "viajante", 2, rowIndexada(t, s, "viajante", "estavel", 9), &prev)
	if s.curSecID.Load() != destination {
		t.Fatalf("o update rotacionou de novo (%d), o teste perdeu o alvo", s.curSecID.Load())
	}

	// Rotaciona de novo: agora o segmento de destino está SELADO.
	poeRowsUntilRotacionar(t, s, destination)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	sel, err := segindex.OpenSealed(segindex.SealedPath(filepath.Join(dir, "data"), destination))
	if err != nil {
		t.Fatalf("o segmento de destino %d não ficou selado: %v", destination, err)
	}
	defer func() { _ = sel.Close() }()
	source, respondeu := sel.Lookup(fieldNode, hashIndexed(t, strVal("estavel")))
	if !respondeu {
		t.Fatalf("o .sidx do segmento %d não responde por node: %v", destination, sel.IntegrityErr())
	}
	var achou bool
	for source.Next() {
		if source.RowID() == row {
			achou = true
		}
	}
	if err := source.Err(); err != nil {
		t.Fatal(err)
	}
	if !achou {
		t.Errorf("o .sidx do segmento de destino %d não tem a row %d por node=estavel: a posting do campo inalterado não sobreviveu à selagem",
			destination, row)
	}

	// E o dataset reaberto continua devolvendo a linha.
	s2 := openStore(t, Options{Dir: dir, SegmentSize: 8 << 10, IndexedFieldsByName: []string{"node", "type"}})
	if rows := lookupRevalidado(t, s2, fieldNode, strVal("estavel")); !slices.Contains(rows, row) {
		t.Errorf("depois do restart, WHERE node='estavel' não devolveu a row %d: %v", row, rows)
	}
}

// TestAAposentadoriaNaoDesmapeiaOIndiceSobUmaVarreduraEmVoo alcança a janela que
// esta tarefa ABRIU: antes dela nada lia .sidx durante uma consulta, então
// desmontar o índice de um segmento aposentado era inofensivo.
//
// O cenário é o do piloto que motivou o subsistema: retenção de fundo passando
// enquanto uma consulta longa varre. A Fonte selada lê o mmap a CADA avanço, e a
// aposentadoria não passa pelo quiesce do Store — ela roda na goroutine de
// retenção. Sem a contagem de vidas do Sealed, o Next seguinte ao munmap não
// devolve resultado errado: mata o processo com SIGSEGV, que é sinal
// incapturável e não aponta para a linha que errou.
//
// O teste vale como oráculo porque o RESULTADO também é conferido: um iterador
// que parasse calado depois da aposentadoria passaria num teste que só afirma
// "não entrou em pânico".
func TestAAposentadoriaNaoDesmapeiaOIndiceSobUmaVarreduraEmVoo(t *testing.T) {
	base := openStore(t, Options{})
	dir := t.TempDir()
	s := openHistoryIndexedIn(t, base, dir, []string{"node"}, 8<<10)
	target := s.curSecID.Load()
	poeRowsUntilRotacionar(t, s, target)
	poeRowsUntilRotacionar(t, s, s.curSecID.Load())
	field := fieldOfDicionario(t, s, "node")
	if _, err := os.Stat(segindex.SealedPath(filepath.Join(dir, "data"), target)); err != nil {
		t.Fatalf("pré-condição: o segmento %d deveria estar selado: %v", target, err)
	}

	it, err := s.Lookup(field, strVal("n1"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = it.Close() }()

	// A varredura entra em voo: o primeiro avanço já resolveu a fonte do
	// segmento alvo e guardou o slice do mapeamento.
	if !it.Next() {
		t.Fatalf("pré-condição: o Lookup não devolveu candidato nenhum (Err=%v)", it.Err())
	}
	rows := []types.RowID{it.RowID()}

	// A retenção aposenta o segmento mais antigo — e com ele o .sidx que a
	// varredura está lendo.
	if _, err := s.RetentionOnePass(context.Background(), LimitesOfRetention{MaxBytes: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(segindex.SealedPath(filepath.Join(dir, "data"), target)); !os.IsNotExist(err) {
		t.Fatalf("pré-condição: o índice do segmento %d deveria ter saído: %v", target, err)
	}

	// Daqui para baixo, cada Next lê um mapeamento que a aposentadoria já
	// mandou desmontar.
	for it.Next() {
		rows = append(rows, it.RowID())
	}
	if err := it.Err(); err != nil {
		t.Fatalf("a varredura falhou depois da aposentadoria: %v", err)
	}
	if len(rows) < 2 {
		t.Fatalf("a varredura devolveu %d linhas: ela não chegou a atravessar a aposentadoria", len(rows))
	}
	for i := 1; i < len(rows); i++ {
		if rows[i-1] >= rows[i] {
			t.Fatalf("a varredura saiu de ordem ao atravessar a aposentadoria: %v", rows)
		}
	}
}

// TestSegmentoAposentadoEntreAListagemEOPreadViraFonteVazia cobre a corrida que
// o godoc do sourceOfFaixa descreve: a lista de segmentos é capturada UMA vez,
// fora do aposMu, e os preads acontecem depois. Uma passada de retenção no meio
// deixa a consulta com o nome de um arquivo que já saiu.
//
// O desfecho tem de ser fonte VAZIA, e não erro. Linha aposentada é linha morta,
// e é assim que o resto do executor já se comporta — ela some pela location, em
// silêncio. Propagar o erro faria a consulta inteira falhar por uma passada de
// retenção que aconteceu ao lado, onde o motor antigo devolvia resultado
// normalmente: seria uma regressão de disponibilidade nascida do índice novo.
func TestSegmentoAposentadoEntreAListagemEOPreadViraFonteVazia(t *testing.T) {
	base := openStore(t, Options{})
	dir := t.TempDir()
	s := openHistoryIndexedIn(t, base, dir, []string{"node"}, 8<<10)
	poeRowsUntilRotacionar(t, s, s.curSecID.Load())
	poeRowsUntilRotacionar(t, s, s.curSecID.Load())

	refs, err := s.listaSegmentos()
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) < 2 {
		t.Fatalf("pré-condição: %d segmentos, o teste precisa de ao menos 2", len(refs))
	}
	// A retenção passou entre a listagem e o pread. Encenar isso exige os DOIS
	// atos da aposentadoria, na ordem dela: fechar o descritor cacheado e só
	// então remover o arquivo. Só o unlink não basta — o descritor aberto mantém
	// o inode vivo e o pread continua funcionando, que é justamente por que os
	// blocos não voltam ao dispositivo sem o CloseSegment.
	if err := s.secR.CloseSegment(refs[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "data", fmt.Sprintf("segment-%08d.dat", refs[0].ID))); err != nil {
		t.Fatal(err)
	}

	f, err := s.sourceOfFaixa(refs, 0)
	if err != nil {
		t.Fatalf("sourceOfFaixa sobre segmento aposentado devolveu erro: %v — deveria ser fonte vazia", err)
	}
	defer func() { _ = f.Close() }()
	if f.MinRowID() <= f.MaxRowID() {
		t.Errorf("a fonte deveria estar vazia por declaração, tem [%d,%d]", f.MinRowID(), f.MaxRowID())
	}
	if f.Next() {
		t.Errorf("a fonte de um segmento aposentado emitiu a row %d", f.RowID())
	}
}

// TestOCrcEhConferidoAntesDeOSeladoEntrarNoCache fixa ONDE a conferência do
// crc32c acontece, e o lugar é uma propriedade de concorrência, não um detalhe.
//
// O selMu é o lock da ROTAÇÃO — o aposAppend o toma a cada segmento novo, sob o
// wmu, no caminho do Put e do Apenda. Com escritor pendente o RWMutex barra os
// RLock seguintes, então uma varredura de centenas de MB debaixo dele faria a
// primeira consulta a um .sidx grande e frio serializar consulta contra
// ingestão: o defeito que este ciclo inteiro existe para eliminar. O Lookup roda
// sob esse lock; o garanteSealed, não.
//
// O oráculo separa os dois momentos, que é a única forma de distingui-los de
// fora: chama o garanteSealed SOZINHO, corrompe o arquivo e só então pergunta.
// Como o mapeamento é MAP_SHARED, o Sealed passa a enxergar os bytes podres —
// então um crc ainda não conferido reprovaria agora. Ele não reprova, e é isso
// que prova que a conta já foi paga fora do lock.
//
// Uma consulta comum NÃO serve de oráculo aqui: ela chama o garanteSealed e o
// Lookup em sequência, e o sync.Once fica cumprido nos dois desenhos.
func TestOCrcEhConferidoAntesDeOSeladoEntrarNoCache(t *testing.T) {
	base := openStore(t, Options{})
	dir := t.TempDir()
	s := openHistoryIndexedIn(t, base, dir, []string{"node"}, 8<<10)
	target := s.curSecID.Load()
	poeRowsUntilRotacionar(t, s, target)
	poeRowsUntilRotacionar(t, s, s.curSecID.Load())
	field := fieldOfDicionario(t, s, "node")

	s.garanteSealed(target)
	s.selMu.RLock()
	sel := s.selados[target]
	s.selMu.RUnlock()
	if sel == nil {
		t.Fatalf("pré-condição: o segmento %d não entrou no cache de selados", target)
	}

	// Corrompe o corpo do .sidx em disco. MAP_SHARED: o Sealed já aberto passa
	// a ver estes bytes.
	path := segindex.SealedPath(filepath.Join(dir, "data"), target)
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xAA, 0xBB, 0xCC, 0xDD}, 64); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if err := sel.IntegrityErr(); err != nil {
		t.Fatalf("o crc reprovou DEPOIS da corrupção (%v): ele não foi conferido no garanteSealed, "+
			"e portanto seria pago no Lookup — que roda sob o selMu, o mesmo lock da rotação", err)
	}
	// E o efeito observável: o segmento continua respondendo pelo índice em vez
	// de degradar para enumeração.
	if crus := candidatosCrus(t, s, field, strVal("nunca-visto")); len(crus) != 0 {
		t.Errorf("o segmento passou a enumerar (%d candidatos) depois da corrupção", len(crus))
	}
}

// TestSnapshotVelhoDoCorrenteNaoRespondePeloWIPDoSegmentoNovo fecha a corrida
// da rotação contra a consulta em voo. fontesOfField tira UM snapshot do
// curSecID no início; se a rotação inteira (Swap → closeWIPs → Sela →
// openWIPsOfCurrent) completar antes de o laço alcançar o segmento antigo, os
// WIPs publicados já são do segmento NOVO. Sem a identidade wipsSec, o
// sourceOfWIP respondia ok=true com as postings do segmento errado, o .sidx
// recém-selado nunca era consultado e nenhuma fonte de enumeração cobria o
// segmento: as linhas dele sumiam dos candidatos — falso negativo, que a
// revalidação do §44 não conserta.
//
// O teste injeta o entrelaçamento em vez de correr atrás dele: chama
// sourceIndexada com `corrente` FORJADO no valor velho, depois de a rotação já
// ter acontecido — que é exatamente o estado que uma consulta em voo carrega.
func TestSnapshotVelhoDoCorrenteNaoRespondePeloWIPDoSegmentoNovo(t *testing.T) {
	s, _ := openStoreIndexedWithSegmentSmall(t, []string{"node"}, 16<<10)
	field := fieldOfDicionario(t, s, "node")
	seg1 := s.curSecID.Load()

	// Linhas com node="alvo" enquanto o seg1 é o corrente.
	var esperadas []types.RowID
	for i := range 8 {
		payload := rowIndexada(t, s, "alvo"+strconv.Itoa(i), "alvo", int64(i%3))
		row, _ := mustPut(t, s, "alvo"+strconv.Itoa(i), int64(i+1), payload, nil)
		esperadas = append(esperadas, row)
	}
	if got := s.curSecID.Load(); got != seg1 {
		t.Fatalf("as linhas do alvo já rotacionaram o segmento (%d → %d); aumente o SegmentSize do teste", seg1, got)
	}
	// Enchimento com OUTRO valor até rotacionar: o seg1 sela, e os WIPs
	// publicados passam a ser do segmento novo.
	poeRowsUntilRotacionar(t, s, seg1)
	if s.wipsSec == seg1 {
		t.Fatalf("a rotação não republicou os WIPs: wipsSec ainda é %d", seg1)
	}

	h := hashIndexed(t, strVal("alvo"))
	// O entrelaçamento: a consulta acredita que o seg1 AINDA é o corrente.
	f, ok := s.sourceIndexada(seg1, seg1, field, h)
	if !ok {
		t.Fatalf("sourceIndexada(seg1 com snapshot velho) = ok=false: o segmento recém-selado devia responder pelo .sidx")
	}
	got := collectRows(t, f)
	for _, want := range esperadas {
		if !slices.Contains(got, want) {
			t.Fatalf("a row %d (node=alvo, gravada no seg %d) sumiu dos candidatos: a fonte devolveu %v — "+
				"é o falso negativo da rotação, o WIP do segmento novo respondendo pelo antigo", want, seg1, got)
		}
	}
}

// TestUpdateDuranteAConsultaNaoSomeDaEnumeracaoColapsada encena o falso
// negativo que o colapso fecha. Na forma por segmento, a fonte de enumeração
// decidia a pertinência pelo DONO da location no instante da iteração, e o WIP
// do corrente era um snapshot da construção: um update que movesse a linha do
// segmento enumerado para o corrente ENTRE os dois instantes a fazia sumir das
// duas fontes — nem o índice do corrente (snapshot velho) nem a enumeração
// (dono já mudou) a emitiam, e a revalidação do §44 não recupera candidato que
// nunca chegou. Com a caminhada única, a location é lida UMA vez e a linha sai
// por onde quer que more.
func TestUpdateDuranteAConsultaNaoSomeDaEnumeracaoColapsada(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir, SegmentSize: 16 << 10, IndexedFieldsByName: []string{"node"}})
	field := fieldOfDicionario(t, s, "node")
	seg1 := s.curSecID.Load()
	payload := rowIndexada(t, s, "movel", "alvo", 1)
	rowMovel, _ := mustPut(t, s, "movel", 1, payload, nil)
	poeRowsUntilRotacionar(t, s, seg1)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// O seg1 perde o .sidx: é o segmento na janela de migração/crash — o estado
	// que obriga a enumeração.
	if err := os.Remove(segindex.SealedPath(filepath.Join(dir, "data"), seg1)); err != nil {
		t.Fatal(err)
	}
	s2 := openStore(t, Options{Dir: dir, SegmentSize: 16 << 10, IndexedFieldsByName: []string{"node"}})

	// A consulta constrói as fontes AGORA (o snapshot do WIP é deste instante)...
	it, err := s2.Lookup(field, strVal("alvo"))
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	// ...e o update move a linha para o corrente ANTES de a iteração alcançá-la.
	prev, _, ok, err := s2.GetByKey(s2.presente(), []byte("movel"), nil)
	if err != nil || !ok {
		t.Fatalf("GetByKey(movel): %v (ok=%v)", err, ok)
	}
	mustPut(t, s2, "movel", 2, rowIndexada(t, s2, "movel", "alvo", 2), &prev)

	crus := collectRows(t, it)
	if !slices.Contains(crus, rowMovel) {
		t.Fatalf("a row %d (node=alvo) sumiu dos candidatos depois de o update movê-la para o corrente: "+
			"é o falso negativo do cérebro dividido entre snapshot do WIP e dono da location", rowMovel)
	}
}

// TestAConsultaNaJanelaDaExpulsaoNaoPublicaMapeamentoDeArquivoRemovido fecha a
// terceira aresta da geração pelo lado que faltava. O selGeneration protege a
// consulta que abriu o arquivo ANTES da evição; a consulta que LÊ a geração
// depois do bump e ABRE o arquivo antes do unlink passava pelas duas guardas do
// publicaSealed e instalava o mapeamento de um arquivo já removido — sem
// ninguém para evictá-lo até a próxima evição deste segmento: inode preso e o
// segmento respondendo por um índice que a expulsão mandou embora. A correção é
// de ORDEM (unlink antes do esquecimento, removeSidxOfDisk), e o hook põe a
// consulta exatamente na janela entre os dois passos.
func TestAConsultaNaJanelaDaExpulsaoNaoPublicaMapeamentoDeArquivoRemovido(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir, SegmentSize: 16 << 10, IndexedFieldsByName: []string{"node"}})
	target := s.curSecID.Load()
	poeRowsUntilRotacionar(t, s, target)

	// A consulta entra NA JANELA: entre a operação de arquivo e o esquecimento.
	// Na ordem corrigida o arquivo já saiu do disco e a abertura falha; na ordem
	// antiga (esquecer primeiro) este mesmo instante tinha o arquivo AINDA em
	// disco e a geração JÁ bumpada — a publicação era aceita e ficava.
	s.entreRemoverEEsquecerHook = func(sec uint32) {
		if sec != target {
			return
		}
		s.selMu.RLock()
		generation := s.selGeneration
		s.selMu.RUnlock()
		sel, err := segindex.OpenSealed(segindex.SealedPath(s.dirOfData(), target))
		if err != nil {
			return // ordem corrigida: o arquivo já não existe, não há o que publicar
		}
		s.publicaSealed(target, sel, generation)
	}
	defer func() { s.entreRemoverEEsquecerHook = nil }()

	if _, err := s.removeSidxOfDisk(target); err != nil {
		t.Fatalf("removeSidxOfDisk: %v", err)
	}

	s.selMu.RLock()
	sobrou := s.selados[target]
	s.selMu.RUnlock()
	if sobrou != nil {
		t.Fatalf("o cache ficou com o mapeamento de um .sidx removido: o segmento responderia por um índice "+
			"que a expulsão mandou embora, com o inode preso até a próxima evição (selados[%d] != nil)", target)
	}
}
