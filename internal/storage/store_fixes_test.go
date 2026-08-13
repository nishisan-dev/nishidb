package storage

// Testes do fix round 1 da S10 (task-review-S10.md): C1 (reserva de rowId +
// writer sticky-failed), I3 (Set antes / Clear depois do commit), I1 (Stats
// concorrente), I4 (guarda do prev), I5 (fieldID conhecido no dict) e os
// minors M4/M5.

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// payloadSemStatus monta payload só com id+counter (campo status ausente).
func payloadSemStatus(t *testing.T, s *Store, key string, counter int64) []byte {
	t.Helper()
	for _, name := range []string{"id", "status", "counter"} {
		if _, err := s.FieldDict().LookupOrCreate([]byte(name)); err != nil {
			t.Fatal(err)
		}
	}
	var p []byte
	p = mustAppendField(t, p, fldID, types.Value{Type: types.TypeString, Bytes: []byte(key)})
	p = mustAppendField(t, p, fldCounter, types.Value{Type: types.TypeInt64, I64: counter})
	return p
}

func TestPutRejeitaPrevDeOutraKey(t *testing.T) {
	// I4: prev de outra key corromperia o dataset em silêncio (key some,
	// estado vai para a row errada) — vira erro determinístico.
	s := openStore(t, Options{})
	mustPut(t, s, "key-a", 1, statusPayload(t, s, "key-a", "Start", 1), nil)
	mustPut(t, s, "key-b", 1, statusPayload(t, s, "key-b", "Start", 1), nil)

	prevB, _, ok, err := s.GetByKey([]byte("key-b"), nil)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if _, _, err := s.Put([]byte("key-a"), 2, 0, statusPayload(t, s, "key-a", "Stop", 2), &prevB); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("prev de outra key: esperado ErrInvalidArgument, obtido %v", err)
	}
	// Nada mudou: as duas keys seguem visíveis com o estado original.
	for _, k := range []string{"key-a", "key-b"} {
		rec, _, ok, err := s.GetByKey([]byte(k), nil)
		if err != nil || !ok || rec.Sequence != 1 {
			t.Fatalf("estado de %q alterado: (%+v,%v,%v)", k, rec, ok, err)
		}
	}
}

func TestEventoInvalidoNaoQueimaRowID(t *testing.T) {
	// R-B: a validação de formato e de dicionário acontece ANTES da reserva
	// do rowId. Com fonte ruidosa (RADIUS/Kafka), validar depois queimaria um
	// id por evento inválido, inflando locations.idx e sequences.idx (arrays
	// densos indexados por rowId) e enchendo o ScanLive de buracos.
	s := openStore(t, Options{})
	okPayload := statusPayload(t, s, "x", "Start", 0)

	unordered := mustAppendField(t, nil, fldStatus, types.Value{Type: types.TypeInt64, I64: 1})
	unordered = mustAppendField(t, unordered, fldID, types.Value{Type: types.TypeInt64, I64: 2})
	if _, _, err := s.Put([]byte("key-falha"), 1, 0, unordered, nil); err == nil {
		t.Fatal("payload desordenado deveria falhar")
	}
	// Campo fora do dicionário: mesma regra.
	ghost := mustAppendField(t, nil, 999, types.Value{Type: types.TypeString, Bytes: []byte("fantasma")})
	if _, _, err := s.Put([]byte("key-ghost"), 1, 0, ghost, nil); err == nil {
		t.Fatal("fieldID desconhecido deveria falhar")
	}
	// Key acima do teto do formato: idem.
	enorme := bytes.Repeat([]byte("k"), encoding.MaxKeySize+1)
	if _, _, err := s.Put(enorme, 1, 0, okPayload, nil); err == nil {
		t.Fatal("key acima do teto deveria falhar")
	}

	id, created := mustPut(t, s, "key-ok", 1, okPayload, nil)
	if !created || id != 0 {
		t.Fatalf("nenhum id pode ter sido queimado pelos 3 Puts inválidos: esperado id=0, obtido %d", id)
	}
	if rows := collectRows(t, s.ScanLive()); len(rows) != 1 || rows[0] != 0 {
		t.Fatalf("ScanLive: esperado [0], obtido %v", rows)
	}
}

func TestRowIDNaoEhReutilizadoAposFalhaPosAppend(t *testing.T) {
	// C1(a) continua valendo do Append em diante: ali o id JÁ está queimado
	// e não pode voltar ao pool — reuso quebraria a bijeção rowId↔key no log
	// (ABA). O que mudou no R-B é só o que acontece ANTES da reserva.
	s := openStore(t, Options{BitmapFields: []types.FieldID{fldStatus}})
	s.SetSetErrHookForTest(func(types.FieldID) error { return errors.New("set falho simulado") })
	if _, _, err := s.Put([]byte("queima"), 1, 0, statusPayload(t, s, "queima", "Start", 1), nil); err == nil {
		t.Fatal("erro na fase de Set deveria falhar o Put")
	}
	// O id 0 foi consumido pelo Put que morreu depois do Append; o writer
	// está sticky, então a prova é o estado pós-reopen do rowId.
	if got := s.Stats().RowCount; got != 0 {
		t.Fatalf("o Put falho não pode contar a row: RowCount=%d", got)
	}
}

func TestWriterStickyFailedAposErroPosAppend(t *testing.T) {
	// C1(b) + I3: erro APÓS o Append (fase de Set do bitmap) põe o writer
	// em modo falho; e como o Clear roda só depois do commit, a posting
	// antiga fica intacta — nenhum falso negativo.
	s := openStore(t, Options{BitmapFields: []types.FieldID{fldStatus}})
	id, _ := mustPut(t, s, "k1", 1, statusPayload(t, s, "k1", "Start", 1), nil)

	prev, _, ok, err := s.GetByKey([]byte("k1"), nil)
	if err != nil || !ok {
		t.Fatal(err)
	}
	// Valor de bitmap com 70000 bytes excede o u16 do values.dict → erro na
	// fase de Set (passo 5), depois do Append.
	huge := strings.Repeat("v", 70000)
	_, _, err = s.Put([]byte("k1"), 2, 0, statusPayload(t, s, "k1", huge, 2), &prev)
	if err == nil {
		t.Fatal("Set de valor gigante deveria falhar")
	}

	// I3: a posting antiga NÃO foi limpa (Clear é pós-commit) — query no
	// valor antigo continua achando a row.
	it, lerr := s.LookupBitmap(fldStatus, strVal("Start"))
	if lerr != nil {
		t.Fatal(lerr)
	}
	rows := collectRows(t, it)
	if len(rows) != 1 || rows[0] != id {
		t.Fatalf("posting antiga deveria estar intacta (falso negativo eliminado): %v", rows)
	}
	// Visibilidade intacta: estado antigo (seq=1), location nunca publicada.
	rec, _, ok, err := s.GetByKey([]byte("k1"), nil)
	if err != nil || !ok || rec.Sequence != 1 {
		t.Fatalf("estado visível deveria ser o antigo: (%+v,%v,%v)", rec, ok, err)
	}

	// C1(b): writer FALHO — Put/Sync/Checkpoint devolvem o erro sticky.
	if _, _, err := s.Put([]byte("k2"), 1, 0, statusPayload(t, s, "k2", "Start", 1), nil); err == nil ||
		!strings.Contains(err.Error(), "modo falho") {
		t.Fatalf("Put pós-falha deveria devolver erro sticky, obtido %v", err)
	}
	if err := s.Sync(); err == nil || !strings.Contains(err.Error(), "modo falho") {
		t.Fatalf("Sync pós-falha: %v", err)
	}
	if err := s.Checkpoint(); err == nil || !strings.Contains(err.Error(), "modo falho") {
		t.Fatalf("Checkpoint pós-falha: %v", err)
	}
}

func TestStatsConcorrenteComPut(t *testing.T) {
	// I1: Stats não pode tocar o segment.Writer — corre com Put sob -race,
	// inclusive através de rotações de segmento.
	s := openStore(t, Options{SegmentSize: 400})
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var lastSeg uint32
		for {
			select {
			case <-stop:
				return
			default:
			}
			st := s.Stats()
			if st.CurrentSegmentID < lastSeg {
				t.Errorf("CurrentSegmentID regrediu: %d → %d", lastSeg, st.CurrentSegmentID)
				return
			}
			lastSeg = st.CurrentSegmentID
		}
	}()
	for i := 0; i < 300; i++ {
		k := fmt.Sprintf("st-conc-%d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	close(stop)
	wg.Wait()
	if st := s.Stats(); st.CurrentSegmentID < 2 {
		t.Fatalf("o teste deveria ter atravessado rotações: %+v", st)
	}
}

func TestBitmapCampoQueEntraESai(t *testing.T) {
	// M4: os ramos "campo que entra" (Set pré-commit) e "campo que sai"
	// (Clear pós-commit) do diff.
	s := openStore(t, Options{BitmapFields: []types.FieldID{fldStatus}})
	id, _ := mustPut(t, s, "es", 1, payloadSemStatus(t, s, "es", 1), nil)

	lookup := func() []types.RowID {
		it, err := s.LookupBitmap(fldStatus, strVal("Start"))
		if err != nil {
			t.Fatal(err)
		}
		return collectRows(t, it)
	}
	if rows := lookup(); len(rows) != 0 {
		t.Fatalf("sem campo status ainda: %v", rows)
	}

	prev, _, _, err := s.GetByKey([]byte("es"), nil)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, s, "es", 2, statusPayload(t, s, "es", "Start", 2), &prev) // campo entra
	if rows := lookup(); len(rows) != 1 || rows[0] != id {
		t.Fatalf("campo que entra: %v", rows)
	}

	prev2, _, _, err := s.GetByKey([]byte("es"), nil)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, s, "es", 3, payloadSemStatus(t, s, "es", 3), &prev2) // campo sai
	if rows := lookup(); len(rows) != 0 {
		t.Fatalf("campo que sai (Clear pós-commit): %v", rows)
	}
}

func TestValueEqualNumericoNoBitmap(t *testing.T) {
	// M4: valueEqual em tipo numérico (int64) — A→A não move a posting;
	// mudança de valor move.
	s := openStore(t, Options{BitmapFields: []types.FieldID{fldCounter}})
	i64 := func(v int64) types.Value { return types.Value{Type: types.TypeInt64, I64: v} }
	id, _ := mustPut(t, s, "num", 1, statusPayload(t, s, "num", "Start", 7), nil)

	lookup := func(v int64) []types.RowID {
		it, err := s.LookupBitmap(fldCounter, i64(v))
		if err != nil {
			t.Fatal(err)
		}
		return collectRows(t, it)
	}
	prev, _, _, err := s.GetByKey([]byte("num"), nil)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, s, "num", 2, statusPayload(t, s, "num", "Stop", 7), &prev) // counter 7→7
	if rows := lookup(7); len(rows) != 1 || rows[0] != id {
		t.Fatalf("7→7 deveria manter a posting: %v", rows)
	}
	prev2, _, _, err := s.GetByKey([]byte("num"), nil)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, s, "num", 3, statusPayload(t, s, "num", "Stop", 8), &prev2) // counter 7→8
	if rows := lookup(7); len(rows) != 0 {
		t.Fatalf("7→8 deveria limpar a posting antiga: %v", rows)
	}
	if rows := lookup(8); len(rows) != 1 || rows[0] != id {
		t.Fatalf("7→8 deveria criar a posting nova: %v", rows)
	}
}

func TestPayloadComFieldIDDesconhecido(t *testing.T) {
	// I5 (§15A item 3): fieldID fora do dicionário é rejeitado ANTES do
	// Append — nada gravado, writer segue utilizável.
	s := openStore(t, Options{})
	okPayload := statusPayload(t, s, "d", "Start", 0) // registra os campos 1..3

	ghost := mustAppendField(t, nil, 999, types.Value{Type: types.TypeString, Bytes: []byte("fantasma")})
	if _, _, err := s.Put([]byte("d"), 1, 0, ghost, nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("fieldID desconhecido: esperado ErrInvalidArgument, obtido %v", err)
	}
	if _, created := mustPut(t, s, "d", 1, okPayload, nil); !created {
		t.Fatal("writer deveria seguir utilizável (erro pré-Append não é sticky)")
	}
}

func TestVisibilidadeJanelaDeCriacao(t *testing.T) {
	// M5: a janela entre o primary.Insert (passo 4) e o Publish (passo 7)
	// numa CRIAÇÃO: leitores concorrentes veem ok=false sem erro, ou o
	// record completo e consistente — nunca erro, nunca estado parcial.
	s := openStore(t, Options{})
	const n = 200
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var buf []byte
			for {
				select {
				case <-stop:
					return
				default:
				}
				for i := 0; i < n; i += 17 {
					key := fmt.Appendf(nil, "cria-%d", i)
					rec, b, ok, err := s.GetByKey(key, buf)
					buf = b
					if err != nil {
						t.Errorf("GetByKey na janela de criação: %v", err)
						return
					}
					if ok && !bytes.Equal(rec.Key, key) {
						t.Errorf("record inconsistente para %q", key)
						return
					}
				}
			}
		}()
	}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("cria-%d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	close(stop)
	wg.Wait()
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("cria-%d", i)
		if _, _, ok, err := s.GetByKey([]byte(k), nil); err != nil || !ok {
			t.Fatalf("key %q deveria estar visível ao final: (%v,%v)", k, ok, err)
		}
	}
}
