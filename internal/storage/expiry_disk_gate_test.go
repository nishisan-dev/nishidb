package storage

import (
	"context"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// O GATE que importa: com a expiração ligada, o disco de um dataset cujo key
// set é ABANDONADO tem de CAIR.
//
// Ele existe porque os testes de unidade das partes não provam isto. Cada peça
// pode estar certa — o coletor marca, a compaction elege, a lápide é durável —
// e o disco continuar subindo, que foi exatamente o que aconteceu na entrega
// da compaction: passou nos testes e a curva de disco em regime ficou sem
// prova. Aqui a asserção é sobre a curva, não sobre as peças.
//
// O cenário é o pior caso real do piloto: entidades criadas, atualizadas por
// um tempo e depois ABANDONADAS. É o padrão que a compaction sozinha não
// segura, porque ela recupera versões superadas e não entidades inteiras.
func TestGateDiscoCaiQuandoOKeySetEhAbandonado(t *testing.T) {
	if testing.Short() {
		t.Skip("gate de disco: pesado demais para -short")
	}
	s := openStore(t, Options{
		Dir: t.TempDir(), SegmentSize: 64 << 10,
		ActiveTTL: true, FallbackDuration: time.Hour,
		WriteInstantFieldName: "_writtenAt", TTLFieldName: "_expiresIn",
	})
	rel := &relogioOfTest{t: time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)}
	s.UsaRelogio(rel.agora)

	recheio := make([]byte, 256)
	for i := range recheio {
		recheio[i] = 'x'
	}
	write := func(k string, seq int64) {
		t.Helper()
		p := payloadOrdenado(t, s, map[string]types.Value{
			"id":         {Type: types.TypeString, Bytes: []byte(k)},
			"status":     {Type: types.TypeString, Bytes: recheio},
			"_writtenAt": {Type: types.TypeTimestamp, I64: rel.t.UnixMilli()},
		})
		rec, _, pres, err := s.State([]byte(k), nil)
		if err != nil {
			t.Fatal(err)
		}
		var prev *types.Record
		if pres.Exists() {
			prev = &rec
		}
		if _, _, err := s.Put([]byte(k), seq, 0, p, prev); err != nil {
			t.Fatal(err)
		}
	}

	// 300 entidades, cada uma criada e atualizada 3 vezes, depois abandonadas.
	const entidades = 300
	seq := int64(1)
	for r := 0; r < 3; r++ {
		for i := 0; i < entidades; i++ {
			write("k"+string(rune('a'+i%26))+string(rune('a'+i/26)), seq)
			seq++
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	pico := s.Stats().StorageBytes
	vivasBefore := countVisiveis(t, s)
	if pico <= 0 || vivasBefore == 0 {
		t.Fatalf("cenário vazio: bytes=%d vivas=%d", pico, vivasBefore)
	}

	// Todas abandonadas: ninguém escreve mais nada.
	rel.advance(2 * time.Hour)

	// Regime: o coletor marca em passadas limitadas e a compaction recolhe.
	// As duas rodam alternadas, como no processo real.
	lim := LimitesOfExpiration{MaxByPassada: 100, TimeByPassada: time.Minute}
	for i := 0; i < 30; i++ {
		if _, err := s.ExpiraOnePass(context.Background(), lim); err != nil {
			t.Fatal(err)
		}
		if _, err := s.CompactaUmaVez(context.Background(), 0.5); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	final := s.Stats().StorageBytes
	vivasDepois := countVisiveis(t, s)

	t.Logf("disco: %d B -> %d B (%.0f%%) | visíveis: %d -> %d",
		pico, final, float64(final)/float64(pico)*100, vivasBefore, vivasDepois)

	if vivasDepois != 0 {
		t.Errorf("ainda há %d entidades visíveis: o coletor não deu conta do key set abandonado", vivasDepois)
	}
	// O disco não vai a zero: as lápides ficam, porque a primary ainda nomeia
	// cada key e esquecê-la exige o tombstoneRetention, que é outra fase. O
	// que se exige aqui é a QUEDA — se ela não vier, a expiração está
	// marcando sem recuperar.
	if final >= pico {
		t.Errorf("o disco não caiu: pico=%d final=%d", pico, final)
	}
}

func countVisiveis(t *testing.T, s *Store) int {
	t.Helper()
	it := s.ScanLive(s.presente())
	defer func() { _ = it.Close() }()
	n := 0
	for it.Next() {
		n++
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	return n
}
