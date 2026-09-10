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

// relogioFalso avança sob controle do teste: um teste de prazo que dormisse
// levaria minutos ou provaria a expiração só com prazos irrealistas.
type relogioFalso struct{ t time.Time }

func (r *relogioFalso) agora() time.Time { return r.t }
func (r *relogioFalso) avanca(d time.Duration) {
	r.t = r.t.Add(d)
}

// parComRetencao abre o par base/histórico com relógio controlado e segmentos
// pequenos, para que a rotação — e portanto a purga — caiba num teste.
func parComRetencao(t *testing.T, segSize int64) (*Store, *Store, *relogioFalso) {
	t.Helper()
	base := openStore(t, Options{})
	dir := t.TempDir()
	hist := openStore(t, Options{
		Dir:                   dir,
		Dataset:               "events_history",
		SegmentSize:           segSize,
		Mode:                  HistoryMode,
		SharedDictionary:      base.FieldDict(),
		WriteInstantFieldName: "_writtenAt",
	})
	rel := &relogioFalso{t: time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)}
	hist.UsaRelogio(rel.agora)
	return base, hist, rel
}

// payloadHistorico monta uma linha com _writtenAt no instante dado. Os campos
// saem ordenados por FieldID porque o formato exige ordem estritamente
// crescente, e a ordem dos ids depende de quem registrou o quê primeiro.
func payloadHistorico(t *testing.T, s *Store, key string, ms int64) []byte {
	t.Helper()
	ids := map[string]types.FieldID{}
	for _, n := range []string{"id", "status", "counter", "_writtenAt"} {
		id, err := s.FieldDict().LookupOrCreate([]byte(n))
		if err != nil {
			t.Fatalf("LookupOrCreate(%s): %v", n, err)
		}
		ids[n] = id
	}
	type campo struct {
		id types.FieldID
		v  types.Value
	}
	campos := []campo{
		{ids["id"], types.Value{Type: types.TypeString, Bytes: []byte(key)}},
		{ids["status"], types.Value{Type: types.TypeString, Bytes: []byte("aberto")}},
		{ids["_writtenAt"], types.Value{Type: types.TypeTimestamp, I64: ms}},
	}
	slices.SortFunc(campos, func(a, b campo) int { return int(a.id) - int(b.id) })
	var p []byte
	for _, c := range campos {
		p = mustAppendField(t, p, c.id, c.v)
	}
	return p
}

func apenda(t *testing.T, hist *Store, key string, quando time.Time) types.RowID {
	t.Helper()
	id, err := hist.Apenda(payloadHistorico(t, hist, key, quando.UnixMilli()))
	if err != nil {
		t.Fatalf("Apenda: %v", err)
	}
	return id
}

func passada(t *testing.T, hist *Store, lim LimitesDeRetencao) ResultadoDeRetencao {
	t.Helper()
	res, err := hist.RetencaoUmaPassada(context.Background(), lim)
	if err != nil {
		t.Fatalf("RetencaoUmaPassada: %v", err)
	}
	return res
}

// A visibilidade não espera a purga: `expireAfterWrite: 1h` significa uma
// hora, e não "uma hora mais o resto do segmento".
func TestALinhaVencidaSomeAntesDeOSegmentoSerPurgado(t *testing.T) {
	_, hist, rel := parComRetencao(t, 1<<20) // segmento grande: nada rotaciona
	for range 4 {
		apenda(t, hist, "a", rel.t)
		rel.avanca(10 * time.Minute)
	}
	// 4 linhas escritas em t, t+10m, t+20m, t+30m. Agora são t+40m.
	if n := contaVivas(t, hist); n != 4 {
		t.Fatalf("%d linhas antes da retenção, quer 4", n)
	}
	res := passada(t, hist, LimitesDeRetencao{ExpireAfterWrite: 25 * time.Minute})
	// Corte em t+15m: as escritas em t e t+10m venceram.
	if res.MarcaAvancou != 2 {
		t.Fatalf("a marca avançou %d, quer 2", res.MarcaAvancou)
	}
	if n := contaVivas(t, hist); n != 2 {
		t.Fatalf("%d linhas depois da retenção, quer 2", n)
	}
	if res.PorTempo != 0 {
		t.Fatalf("aposentou %d segmentos com um só no disco", res.PorTempo)
	}
}

// A busca binária depende de o _writtenAt ser não-decrescente por rowId, e é
// isso que a serialização do writer garante. Com muitas linhas, a fronteira
// tem de sair exata mesmo assim.
func TestAMarcaDaguaAchaAFronteiraExata(t *testing.T) {
	_, hist, rel := parComRetencao(t, 1<<20)
	for range 50 {
		apenda(t, hist, "a", rel.t)
		rel.avanca(time.Minute)
	}
	// 50 linhas, de t a t+49m. Agora são t+50m; corte em t+20m.
	res := passada(t, hist, LimitesDeRetencao{ExpireAfterWrite: 30 * time.Minute})
	if res.MarcaAvancou != 20 {
		t.Fatalf("a marca avançou %d, quer 20", res.MarcaAvancou)
	}
	if n := contaVivas(t, hist); n != 30 {
		t.Fatalf("%d linhas vivas, quer 30", n)
	}
}

// Linha sem _writtenAt nunca vence: presumir que venceu apagaria dado por
// omissão, e é a mesma regra do coletor do dataset materializado.
func TestInstanteDesconhecidoNuncaVence(t *testing.T) {
	base, hist, rel := parComRetencao(t, 1<<20)
	if _, err := hist.Apenda(statusPayload(t, base, "sem", "instante", 1)); err != nil {
		t.Fatalf("Apenda: %v", err)
	}
	rel.avanca(10 * time.Hour)
	res := passada(t, hist, LimitesDeRetencao{ExpireAfterWrite: time.Hour})
	if res.MarcaAvancou != 0 {
		t.Fatalf("a marca avançou %d sobre linha sem instante", res.MarcaAvancou)
	}
	if n := contaVivas(t, hist); n != 1 {
		t.Fatalf("%d linhas, quer 1", n)
	}
}

// O segmento inteiramente coberto pela marca sai do disco pelos mesmos quatro
// passos que a compaction usa.
func TestSegmentoInteiramenteVencidoEhAposentado(t *testing.T) {
	_, hist, rel := parComRetencao(t, 512)
	for range 30 {
		apenda(t, hist, "a", rel.t)
		rel.avanca(time.Minute)
	}
	antes := contaArquivosDeSegmento(t, hist)
	if antes < 3 {
		t.Fatalf("%d segmentos; o teste precisa de rotação", antes)
	}
	rel.avanca(10 * time.Hour)
	res := passada(t, hist, LimitesDeRetencao{ExpireAfterWrite: time.Hour})
	if res.PorTempo == 0 {
		t.Fatal("nenhum segmento foi aposentado")
	}
	depois := contaArquivosDeSegmento(t, hist)
	if depois >= antes {
		t.Fatalf("%d segmentos depois, %d antes", depois, antes)
	}
	// O corrente nunca sai: é o arquivo em que o writer está apendando.
	if depois < 1 {
		t.Fatal("o segmento corrente foi aposentado")
	}
}

func contaArquivosDeSegmento(t *testing.T, s *Store) int {
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
	_, hist, rel := parComRetencao(t, 512)
	for range 40 {
		apenda(t, hist, "a", rel.t)
		rel.avanca(time.Second)
	}
	antes, err := hist.bytesDosSegmentos()
	if err != nil {
		t.Fatal(err)
	}
	teto := antes / 2
	res := passada(t, hist, LimitesDeRetencao{MaxBytes: teto})
	if res.PorTamanho == 0 {
		t.Fatal("nenhum segmento saiu pelo teto de tamanho")
	}
	depois, err := hist.bytesDosSegmentos()
	if err != nil {
		t.Fatal(err)
	}
	if depois > teto {
		t.Fatalf("%d bytes depois da purga, teto é %d", depois, teto)
	}
	// E a marca acompanhou: nenhuma location pode nomear um arquivo que já
	// não existe.
	if hist.RowIDBase() == 0 {
		t.Fatal("a purga por tamanho não avançou a marca-d'água")
	}
	if n := contaVivas(t, hist); n == 40 {
		t.Fatal("a purga devolveu disco sem tirar nenhuma linha da consulta")
	}
}

// Com um segmento só no disco não há o que aposentar, e ficar acima do teto é
// o desenho: o corrente é o arquivo em que o writer está apendando.
func TestPurgaPorTamanhoNaoTocaOSegmentoCorrente(t *testing.T) {
	_, hist, rel := parComRetencao(t, 1<<20)
	for range 5 {
		apenda(t, hist, "a", rel.t)
	}
	res := passada(t, hist, LimitesDeRetencao{MaxBytes: 1})
	if res.PorTamanho != 0 {
		t.Fatalf("aposentou %d segmentos com um só no disco", res.PorTamanho)
	}
	if n := contaVivas(t, hist); n != 5 {
		t.Fatalf("%d linhas, quer 5", n)
	}
}

func TestRetencaoSobreDatasetMaterializadoEhRecusada(t *testing.T) {
	base := openStore(t, Options{})
	_, err := base.RetencaoUmaPassada(context.Background(), LimitesDeRetencao{ExpireAfterWrite: time.Hour})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("RetencaoUmaPassada = %v, quer ErrInvalidArgument", err)
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
	rel := &relogioFalso{t: time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)}
	hist.UsaRelogio(rel.agora)
	for range 30 {
		apenda(t, hist, "a", rel.t)
		rel.avanca(time.Minute)
	}
	rel.avanca(10 * time.Hour)
	if res := passada(t, hist, LimitesDeRetencao{ExpireAfterWrite: time.Hour}); res.PorTempo == 0 {
		t.Fatal("nada foi aposentado")
	}
	marca := hist.RowIDBase()
	vivas := contaVivas(t, hist)
	if err := hist.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	volta := openStore(t, Options{
		Dir: dir, Dataset: "events_history", SegmentSize: 512,
		Mode: HistoryMode, SharedDictionary: base.FieldDict(), WriteInstantFieldName: "_writtenAt",
	})
	if got := volta.RowIDBase(); got != marca {
		t.Fatalf("marca = %d depois do restart, quer %d", got, marca)
	}
	if n := contaVivas(t, volta); n != vivas {
		t.Fatalf("%d linhas depois do restart, quer %d", n, vivas)
	}
}

// A goroutine sobe, faz a primeira passada imediatamente e o Close a cancela
// e AGUARDA — sem isso o Close desmapearia o índice sob os pés de quem está
// renomeando arquivo.
func TestGoroutineDeRetencaoSobeEPara(t *testing.T) {
	_, hist, rel := parComRetencao(t, 512)
	for range 30 {
		apenda(t, hist, "a", rel.t)
		rel.avanca(time.Minute)
	}
	rel.avanca(10 * time.Hour)
	hist.IniciaRetencao(context.Background(), LimitesDeRetencao{ExpireAfterWrite: time.Hour})
	if !hist.RetencaoAtiva() {
		t.Fatal("RetencaoAtiva = false logo depois do Inicia")
	}
	esperaAte(t, func() bool { return hist.RowIDBase() > 0 }, "a primeira passada não avançou a marca")
	hist.ParaRetencao()
	if hist.RetencaoAtiva() {
		t.Fatal("RetencaoAtiva = true depois do Para")
	}
}

func esperaAte(t *testing.T, cond func() bool, oQueFalhou string) {
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
	base.IniciaRetencao(context.Background(), LimitesDeRetencao{ExpireAfterWrite: time.Hour})
	if base.RetencaoAtiva() {
		t.Fatal("a retenção subiu num dataset materializado")
	}
}
