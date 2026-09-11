package storage

import (
	"context"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// storeWithRegras abre um dataset com expiração e duração de fundo dada, com
// relógio controlado. Zero em fundo é o dataset que só tem regras
// condicionais: quem não tiver prazo gravado não expira nunca.
func storeWithRegras(t *testing.T, dir string, fundo time.Duration) (*Store, *relogioOfTest) {
	t.Helper()
	s := openStore(t, Options{
		Dir:                   dir,
		ActiveTTL:             true,
		FallbackDuration:      fundo,
		WriteInstantFieldName: "_writtenAt",
		TTLFieldName:          "_expiresIn",
	})
	rel := &relogioOfTest{t: time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)}
	s.UsaRelogio(rel.agora)
	return s, rel
}

// payloadWithPrazo monta o payload que o materializer produz num dataset com
// regras: o instante da escrita e a duração que a regra escolheu.
func payloadWithPrazo(t *testing.T, s *Store, key string, write time.Time, d time.Duration) []byte {
	t.Helper()
	return payloadOrdenado(t, s, map[string]types.Value{
		"id":         {Type: types.TypeString, Bytes: []byte(key)},
		"_writtenAt": {Type: types.TypeTimestamp, I64: write.UnixMilli()},
		"_expiresIn": {Type: types.TypeInt64, I64: d.Milliseconds()},
	})
}

// A duração da LINHA governa, e não uma configuração única do dataset: é o
// ponto inteiro do TTL condicional.
func TestPrazoVencidoUsaADuracaoDaLinha(t *testing.T) {
	s, rel := storeWithRegras(t, t.TempDir(), 7*24*time.Hour)
	curto, _ := mustPut(t, s, "curto", 1, payloadWithPrazo(t, s, "curto", rel.t, 3*time.Minute), nil)
	longo, _ := mustPut(t, s, "longo", 1, payloadWithPrazo(t, s, "longo", rel.t, 7*24*time.Hour), nil)

	rel.advance(4 * time.Minute)
	if !s.prazoVencido(curto, s.now()) {
		t.Error("a linha de 3m não venceu depois de 4m")
	}
	if s.prazoVencido(longo, s.now()) {
		t.Error("a linha de 7d venceu depois de 4m")
	}
}

// A prova da MIGRAÇÃO: a entidade materializada antes de as regras existirem
// não tem o campo no documento, e continua expirando pelo prazo de fundo.
func TestLinhaSemExpiresInCaiNaDuracaoDeFundo(t *testing.T) {
	s, rel := storeWithRegras(t, t.TempDir(), time.Hour)
	id, _ := mustPut(t, s, "A", 1, payloadWithWrite(t, s, "A", "Start", rel.t), nil)

	rel.advance(30 * time.Minute)
	if s.prazoVencido(id, s.now()) {
		t.Error("venceu antes da duração de fundo")
	}
	rel.advance(31 * time.Minute)
	if !s.prazoVencido(id, s.now()) {
		t.Error("a linha sem prazo gravado não caiu na duração de fundo")
	}
}

func TestLinhaSemExpiresInESemFundoEhImortal(t *testing.T) {
	s, rel := storeWithRegras(t, t.TempDir(), 0)
	id, _ := mustPut(t, s, "A", 1, payloadWithWrite(t, s, "A", "Start", rel.t), nil)

	rel.advance(365 * 24 * time.Hour)
	if s.prazoVencido(id, s.now()) {
		t.Error("sem regra de fundo, a linha sem prazo gravado não pode vencer")
	}
}

// O teste do SENTINELA, e o mais importante do conjunto: sem ele, um Get que
// colapse imortal com desconhecido passa despercebido — e a entidade que uma
// regra `never` declarou imortal herdaria o prazo de fundo.
func TestLinhaImortalNaoHerdaADuracaoDeFundo(t *testing.T) {
	s, rel := storeWithRegras(t, t.TempDir(), time.Hour)
	id, _ := mustPut(t, s, "A", 1, payloadWithPrazo(t, s, "A", rel.t, 0), nil)

	rel.advance(48 * time.Hour)
	if s.prazoVencido(id, s.now()) {
		t.Error("a linha imortal caiu na duração de fundo: o sentinela não está sendo distinguido")
	}
	if _, _, ok, err := s.GetByRowID(s.presente(), id, nil); err != nil || !ok {
		t.Errorf("a linha imortal sumiu da leitura: ok=%v err=%v", ok, err)
	}
}

// O índice é DERIVADO: a fonte da verdade é o campo no payload, e o replay do
// tail repõe as mesmas durações pelo mesmo applyRecord.
func TestReplayDoTailReconstroiOsPrazos(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                   dir,
		SegmentSize:           1 << 20,
		ActiveTTL:             true,
		FallbackDuration:      7 * 24 * time.Hour,
		WriteInstantFieldName: "_writtenAt",
		TTLFieldName:          "_expiresIn",
	}
	s, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	rel := &relogioOfTest{t: time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)}
	s.UsaRelogio(rel.agora)
	id, _ := mustPut(t, s, "A", 1, payloadWithPrazo(t, s, "A", rel.t, 3*time.Minute), nil)
	// Close SEM checkpoint: o índice em heap morre e só os records sobrevivem.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	rel2 := &relogioOfTest{t: rel.t.Add(4 * time.Minute)}
	s2.UsaRelogio(rel2.agora)
	if !s2.prazoVencido(id, s2.now()) {
		t.Error("o replay não repôs a duração de 3m: a linha ganhou o prazo de fundo")
	}
}

// A visibilidade NÃO depende do agendador: uma linha some assim que o prazo
// DELA passa, mesmo com o coletor parado.
func TestPrazoVencidoEscondeALinhaComOColetorParado(t *testing.T) {
	s, rel := storeWithRegras(t, t.TempDir(), 7*24*time.Hour)
	id, _ := mustPut(t, s, "A", 1, payloadWithPrazo(t, s, "A", rel.t, 3*time.Minute), nil)

	if _, _, ok, _ := s.GetByRowID(s.presente(), id, nil); !ok {
		t.Fatal("a linha nem apareceu")
	}
	rel.advance(4 * time.Minute)
	if _, _, ok, err := s.GetByRowID(s.presente(), id, nil); err != nil || ok {
		t.Errorf("a linha vencida continua visível: ok=%v err=%v", ok, err)
	}
	if rows := collectRows(t, s.ScanLive(s.presente())); len(rows) != 0 {
		t.Errorf("ScanLive devolveu a linha vencida: %v", rows)
	}
}

// A escrita seguinte reescreve o prazo, e é isso que CANCELA a expiração curta
// quando a condição deixa de valer.
func TestEscritaComOutroPrazoCancelaAExpiracaoCurta(t *testing.T) {
	s, rel := storeWithRegras(t, t.TempDir(), 7*24*time.Hour)
	id, _ := mustPut(t, s, "A", 1, payloadWithPrazo(t, s, "A", rel.t, 3*time.Minute), nil)

	rel.advance(90 * time.Second)
	rec, _, pres, err := s.State([]byte("A"), nil)
	if err != nil || pres != types.PresenceLive {
		t.Fatalf("pres=%v err=%v", pres, err)
	}
	if _, _, err := s.Put([]byte("A"), 2, 0, payloadWithPrazo(t, s, "A", rel.t, 7*24*time.Hour), &rec); err != nil {
		t.Fatal(err)
	}

	rel.advance(4 * time.Minute) // muito além dos 3m originais
	if s.prazoVencido(id, s.now()) {
		t.Error("a entidade morreu pelo prazo antigo depois de a condição deixar de valer")
	}
}

// A compaction descarta o record vencido pela regra da própria linha.
func TestCompactionDescartaRecordVencidoPorRegraCondicional(t *testing.T) {
	s, rel := storeWithRegras(t, t.TempDir(), 7*24*time.Hour)
	id, _ := mustPut(t, s, "A", 1, payloadWithPrazo(t, s, "A", rel.t, 3*time.Minute), nil)

	rel.advance(4 * time.Minute)
	r, err := s.ExpiraOnePass(context.Background(), LimitesOfExpiration{
		MaxByPass: 10, TempoByPass: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if r.Expiradas != 1 {
		t.Fatalf("passada expirou %d, quero 1 (%+v)", r.Expiradas, r)
	}
	if _, _, ok, _ := s.GetByRowID(s.presente(), id, nil); ok {
		t.Error("a linha continua legível depois da lápide")
	}
}

// A lápide preserva o instante de escrita de propósito, então a linha
// tombstonada continua "vencida" para sempre. Sem o pré-filtro, o coletor a
// reexaminaria a cada passada e chamaria Expira — que toma o wmu só para ver a
// flag e devolver false. Com prazo de MINUTOS isso é disputa contínua pelo
// lock de escrita com a ingestão.
func TestColetorNaoTentaExpirarLinhaJaTombstonada(t *testing.T) {
	s, rel := storeWithRegras(t, t.TempDir(), 7*24*time.Hour)
	mustPut(t, s, "A", 1, payloadWithPrazo(t, s, "A", rel.t, 3*time.Minute), nil)

	rel.advance(4 * time.Minute)
	lim := LimitesOfExpiration{MaxByPass: 10, TempoByPass: time.Second}
	primeira, err := s.ExpiraOnePass(context.Background(), lim)
	if err != nil {
		t.Fatal(err)
	}
	if primeira.Expiradas != 1 {
		t.Fatalf("primeira passada: %+v", primeira)
	}

	segunda, err := s.ExpiraOnePass(context.Background(), lim)
	if err != nil {
		t.Fatal(err)
	}
	if segunda.Expiradas != 0 || segunda.Renovadas != 0 {
		t.Errorf("segunda passada tocou a linha tombstonada: %+v", segunda)
	}
}

// A linha tombstonada não é sequer ENTREGUE, que é a forma forte da
// propriedade que o pré-filtro da varredura garantia de forma fraca.
//
// A lápide preserva o instante de escrita de propósito, então a linha
// tombstonada continua vencida para sempre pelo predicado. Na varredura isso
// obrigava um pré-filtro explícito — sem ele, cada volta chamava Expira em
// toda linha morta só para tomar o lock do writer e ver a flag. Com a agenda o
// pré-filtro some: a linha recebe o sentinela "nunca" no mesmo applyRecord que
// grava a lápide e sai da árvore.
func TestLinhaTombstonadaNaoEhEntreguePelaAgenda(t *testing.T) {
	s, rel := storeWithRegras(t, t.TempDir(), 7*24*time.Hour)
	for _, k := range []string{"A", "B", "C"} {
		mustPut(t, s, k, 1, payloadWithPrazo(t, s, k, rel.t, 3*time.Minute), nil)
	}
	rel.advance(4 * time.Minute)
	lim := LimitesOfExpiration{MaxByPass: 10, TempoByPass: 2 * time.Second}
	if r, err := s.ExpiraOnePass(context.Background(), lim); err != nil || r.Expiradas != 3 {
		t.Fatalf("primeira passada: %+v (err=%v)", r, err)
	}
	// A passada seguinte não tem o que examinar: as três saíram da agenda.
	r, err := s.ExpiraOnePass(context.Background(), lim)
	if err != nil {
		t.Fatal(err)
	}
	if r.Expiradas != 0 || r.Renovadas != 0 {
		t.Errorf("segunda passada tocou linha tombstonada: %+v", r)
	}
	if r.Examinadas != 0 {
		t.Errorf("a agenda entregou %d linhas tombstonadas; quer 0", r.Examinadas)
	}
}
