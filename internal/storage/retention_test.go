package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// relogioFake avança sob controle do teste: um teste de prazo que dormisse
// levaria minutos ou provaria a expiração só com prazos irrealistas.
type relogioFake struct{ t time.Time }

func (r *relogioFake) agora() time.Time { return r.t }
func (r *relogioFake) advance(d time.Duration) {
	r.t = r.t.Add(d)
}

// pairWithRetention abre o par base/histórico com relógio controlado e segmentos
// pequenos, para que a rotação — e portanto a purga — caiba num teste.
func pairWithRetention(t *testing.T, secSize int64) (*Store, *Store, *relogioFake) {
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
	})
	rel := &relogioFake{t: time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)}
	hist.UsaRelogio(rel.agora)
	return base, hist, rel
}

// payloadHistory monta uma linha com _writtenAt no instante dado. Os campos
// saem ordenados por FieldID porque o formato exige ordem estritamente
// crescente, e a ordem dos ids depende de quem registrou o quê primeiro.
func payloadHistory(t *testing.T, s *Store, key string, ms int64) []byte {
	t.Helper()
	ids := map[string]types.FieldID{}
	for _, n := range []string{"id", "status", "counter", "_writtenAt"} {
		id, err := s.FieldDict().LookupOrCreate([]byte(n))
		if err != nil {
			t.Fatalf("LookupOrCreate(%s): %v", n, err)
		}
		ids[n] = id
	}
	type field struct {
		id types.FieldID
		v  types.Value
	}
	fields := []field{
		{ids["id"], types.Value{Type: types.TypeString, Bytes: []byte(key)}},
		{ids["status"], types.Value{Type: types.TypeString, Bytes: []byte("aberto")}},
		{ids["_writtenAt"], types.Value{Type: types.TypeTimestamp, I64: ms}},
	}
	slices.SortFunc(fields, func(a, b field) int { return int(a.id) - int(b.id) })
	var p []byte
	for _, c := range fields {
		p = mustAppendField(t, p, c.id, c.v)
	}
	return p
}

func apenda(t *testing.T, hist *Store, key string, quando time.Time) types.RowID {
	t.Helper()
	id, err := hist.Apenda(payloadHistory(t, hist, key, quando.UnixMilli()))
	if err != nil {
		t.Fatalf("Apenda: %v", err)
	}
	return id
}

func pass(t *testing.T, hist *Store, lim LimitesOfRetention) ResultOfRetention {
	t.Helper()
	res, err := hist.RetentionOnePass(context.Background(), lim)
	if err != nil {
		t.Fatalf("RetentionOnePass: %v", err)
	}
	return res
}

// A visibilidade não espera a purga: `expireAfterWrite: 1h` significa uma
// hora, e não "uma hora mais o resto do segmento".
func TestALinhaVencidaSomeAntesDeOSegmentoSerPurgado(t *testing.T) {
	_, hist, rel := pairWithRetention(t, 1<<20) // segmento grande: nada rotaciona
	for range 4 {
		apenda(t, hist, "a", rel.t)
		rel.advance(10 * time.Minute)
	}
	// 4 linhas escritas em t, t+10m, t+20m, t+30m. Agora são t+40m.
	if n := countVivas(t, hist); n != 4 {
		t.Fatalf("%d linhas antes da retenção, quer 4", n)
	}
	res := pass(t, hist, LimitesOfRetention{ExpireAfterWrite: 25 * time.Minute})
	// Corte em t+15m: as escritas em t e t+10m venceram.
	if res.MarkAvancou != 2 {
		t.Fatalf("a marca avançou %d, quer 2", res.MarkAvancou)
	}
	if n := countVivas(t, hist); n != 2 {
		t.Fatalf("%d linhas depois da retenção, quer 2", n)
	}
	if res.ByTempo != 0 {
		t.Fatalf("aposentou %d segmentos com um só no disco", res.ByTempo)
	}
}

// A busca binária depende de o _writtenAt ser não-decrescente por rowId, e é
// isso que a serialização do writer garante. Com muitas linhas, a fronteira
// tem de sair exata mesmo assim.
func TestAMarcaDaguaAchaAFronteiraExata(t *testing.T) {
	_, hist, rel := pairWithRetention(t, 1<<20)
	for range 50 {
		apenda(t, hist, "a", rel.t)
		rel.advance(time.Minute)
	}
	// 50 linhas, de t a t+49m. Agora são t+50m; corte em t+20m.
	res := pass(t, hist, LimitesOfRetention{ExpireAfterWrite: 30 * time.Minute})
	if res.MarkAvancou != 20 {
		t.Fatalf("a marca avançou %d, quer 20", res.MarkAvancou)
	}
	if n := countVivas(t, hist); n != 30 {
		t.Fatalf("%d linhas vivas, quer 30", n)
	}
}

// Linha sem _writtenAt nunca vence: presumir que venceu apagaria dado por
// omissão, e é a mesma regra do coletor do dataset materializado.
func TestInstanteDesconhecidoNuncaVence(t *testing.T) {
	base, hist, rel := pairWithRetention(t, 1<<20)
	if _, err := hist.Apenda(statusPayload(t, base, "sem", "instante", 1)); err != nil {
		t.Fatalf("Apenda: %v", err)
	}
	rel.advance(10 * time.Hour)
	res := pass(t, hist, LimitesOfRetention{ExpireAfterWrite: time.Hour})
	if res.MarkAvancou != 0 {
		t.Fatalf("a marca avançou %d sobre linha sem instante", res.MarkAvancou)
	}
	if n := countVivas(t, hist); n != 1 {
		t.Fatalf("%d linhas, quer 1", n)
	}
}

// O segmento inteiramente coberto pela marca sai do disco pelos mesmos quatro
// passos que a compaction usa.
func TestSegmentoInteiramenteVencidoEhAposentado(t *testing.T) {
	_, hist, rel := pairWithRetention(t, 512)
	for range 30 {
		apenda(t, hist, "a", rel.t)
		rel.advance(time.Minute)
	}
	before := countArquivosOfSegment(t, hist)
	if before < 3 {
		t.Fatalf("%d segmentos; o teste precisa de rotação", before)
	}
	rel.advance(10 * time.Hour)
	res := pass(t, hist, LimitesOfRetention{ExpireAfterWrite: time.Hour})
	if res.ByTempo == 0 {
		t.Fatal("nenhum segmento foi aposentado")
	}
	depois := countArquivosOfSegment(t, hist)
	if depois >= before {
		t.Fatalf("%d segmentos depois, %d antes", depois, before)
	}
	// O corrente nunca sai: é o arquivo em que o writer está apendando.
	if depois < 1 {
		t.Fatal("o segmento corrente foi aposentado")
	}
}

func countArquivosOfSegment(t *testing.T, s *Store) int {
	t.Helper()
	entradas, err := os.ReadDir(filepath.Join(s.opts.Dir, "data"))
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	n := 0
	for _, e := range entradas {
		if filepath.Ext(e.Name()) == ".dat" {
			n++
		}
	}
	return n
}

// O teto de tamanho é uma promessa sobre disco, e honrá-la significa deixar de
// enxergar linhas que o prazo ainda não tinha vencido.
func TestPurgaPorTamanhoAposentaOMaisAntigo(t *testing.T) {
	_, hist, rel := pairWithRetention(t, 512)
	for range 40 {
		apenda(t, hist, "a", rel.t)
		rel.advance(time.Second)
	}
	before, err := hist.bytesOfSegmentos()
	if err != nil {
		t.Fatal(err)
	}
	ceiling := before / 2
	res := pass(t, hist, LimitesOfRetention{MaxBytes: ceiling})
	if res.BySize == 0 {
		t.Fatal("nenhum segmento saiu pelo teto de tamanho")
	}
	depois, err := hist.bytesOfSegmentos()
	if err != nil {
		t.Fatal(err)
	}
	if depois > ceiling {
		t.Fatalf("%d bytes depois da purga, teto é %d", depois, ceiling)
	}
	// E a marca acompanhou: nenhuma location pode nomear um arquivo que já
	// não existe.
	if hist.RowIDBase() == 0 {
		t.Fatal("a purga por tamanho não avançou a marca-d'água")
	}
	if n := countVivas(t, hist); n == 40 {
		t.Fatal("a purga devolveu disco sem tirar nenhuma linha da consulta")
	}
}

// Com um segmento só no disco não há o que aposentar, e ficar acima do teto é
// o desenho: o corrente é o arquivo em que o writer está apendando.
func TestPurgaPorTamanhoNaoTocaOSegmentoCorrente(t *testing.T) {
	_, hist, rel := pairWithRetention(t, 1<<20)
	for range 5 {
		apenda(t, hist, "a", rel.t)
	}
	res := pass(t, hist, LimitesOfRetention{MaxBytes: 1})
	if res.BySize != 0 {
		t.Fatalf("aposentou %d segmentos com um só no disco", res.BySize)
	}
	if n := countVivas(t, hist); n != 5 {
		t.Fatalf("%d linhas, quer 5", n)
	}
}

func TestRetencaoSobreDatasetMaterializadoEhRecusada(t *testing.T) {
	base := openStore(t, Options{})
	_, err := base.RetentionOnePass(context.Background(), LimitesOfRetention{ExpireAfterWrite: time.Hour})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("RetentionOnePass = %v, quer ErrInvalidArgument", err)
	}
}

// A marca é durabilizada pelo primeiro dos quatro passos da aposentadoria,
// ANTES de qualquer byte sair do disco. Sem isso, um crash entre o unlink e o
// checkpoint deixaria locations vivas apontando para um arquivo removido.
func TestAMarcaEhDuravelDepoisDaPurga(t *testing.T) {
	base := openStore(t, Options{})
	dir := t.TempDir()
	hist, err := Open(Options{
		Dir: dir, Dataset: "events_history", SegmentSize: 512,
		Mode: HistoryMode, SharedDictionary: base.FieldDict(), WriteInstantFieldName: "_writtenAt",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	rel := &relogioFake{t: time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)}
	hist.UsaRelogio(rel.agora)
	for range 30 {
		apenda(t, hist, "a", rel.t)
		rel.advance(time.Minute)
	}
	rel.advance(10 * time.Hour)
	if res := pass(t, hist, LimitesOfRetention{ExpireAfterWrite: time.Hour}); res.ByTempo == 0 {
		t.Fatal("nada foi aposentado")
	}
	mark := hist.RowIDBase()
	vivas := countVivas(t, hist)
	if err := hist.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	back := openStore(t, Options{
		Dir: dir, Dataset: "events_history", SegmentSize: 512,
		Mode: HistoryMode, SharedDictionary: base.FieldDict(), WriteInstantFieldName: "_writtenAt",
	})
	if got := back.RowIDBase(); got != mark {
		t.Fatalf("marca = %d depois do restart, quer %d", got, mark)
	}
	if n := countVivas(t, back); n != vivas {
		t.Fatalf("%d linhas depois do restart, quer %d", n, vivas)
	}
}

// A goroutine sobe, faz a primeira passada imediatamente e o Close a cancela
// e AGUARDA — sem isso o Close desmapearia o índice sob os pés de quem está
// renomeando arquivo.
func TestGoroutineDeRetencaoSobeEPara(t *testing.T) {
	_, hist, rel := pairWithRetention(t, 512)
	for range 30 {
		apenda(t, hist, "a", rel.t)
		rel.advance(time.Minute)
	}
	rel.advance(10 * time.Hour)
	hist.StartRetention(context.Background(), LimitesOfRetention{ExpireAfterWrite: time.Hour})
	if !hist.RetentionActive() {
		t.Fatal("RetentionActive = false logo depois do Inicia")
	}
	waitUntil(t, func() bool { return hist.RowIDBase() > 0 }, "a primeira passada não avançou a marca")
	hist.ForRetention()
	if hist.RetentionActive() {
		t.Fatal("RetentionActive = true depois do Para")
	}
}

func waitUntil(t *testing.T, cond func() bool, oQueFalhou string) {
	t.Helper()
	prazo := time.Now().Add(5 * time.Second)
	for time.Now().Before(prazo) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal(oQueFalhou)
}

// Chamar sobre um dataset materializado não faz nada, e é o que permite ao
// servidor chamá-la sem perguntar.
func TestIniciaRetencaoNoMaterializadoEhInocuo(t *testing.T) {
	base := openStore(t, Options{})
	base.StartRetention(context.Background(), LimitesOfRetention{ExpireAfterWrite: time.Hour})
	if base.RetentionActive() {
		t.Fatal("a retenção subiu num dataset materializado")
	}
}
