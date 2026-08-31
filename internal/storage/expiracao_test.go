package storage

import (
	"sort"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Os ids deste pacote são POSICIONAIS (ordem de LookupOrCreate), e o Open de
// um dataset com expiração registra o campo de sistema antes de qualquer
// outro. Em vez de fixar números — que mudariam junto com essa ordem —, os
// testes daqui resolvem cada id pelo dicionário do próprio store.
func idDoCampo(t *testing.T, s *Store, nome string) types.FieldID {
	t.Helper()
	id, err := s.FieldDict().LookupOrCreate([]byte(nome))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

type relogioDeTeste struct{ t time.Time }

func (r *relogioDeTeste) agora() time.Time       { return r.t }
func (r *relogioDeTeste) avanca(d time.Duration) { r.t = r.t.Add(d) }

// storeComExpiracao abre um dataset com idle timeout e relógio controlado.
func storeComExpiracao(t *testing.T, dir string, d time.Duration) (*Store, *relogioDeTeste) {
	t.Helper()
	s := openStore(t, Options{
		Dir:            dir,
		TTLAtivo:       true,
		DuracaoDeFundo: d,
		CampoDeEscrita: "_writtenAt",
		CampoDePrazo:   "_expiresIn",
	})
	rel := &relogioDeTeste{t: time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)}
	s.UsaRelogio(rel.agora)
	return s, rel
}

// payloadSemEscrita monta o payload de um dataset materializado ANTES de a
// expiração ser declarada: sem o campo de sistema.
func payloadSemEscrita(t *testing.T, s *Store, key string) []byte {
	t.Helper()
	return payloadOrdenado(t, s, map[string]types.Value{
		"id": {Type: types.TypeString, Bytes: []byte(key)},
	})
}

// payloadComEscrita monta um payload com o campo de sistema, como o
// materializer o produziria.
func payloadComEscrita(t *testing.T, s *Store, key, status string, escrita time.Time) []byte {
	t.Helper()
	return payloadOrdenado(t, s, map[string]types.Value{
		"id":         {Type: types.TypeString, Bytes: []byte(key)},
		"status":     {Type: types.TypeString, Bytes: []byte(status)},
		"_writtenAt": {Type: types.TypeTimestamp, I64: escrita.UnixMilli()},
	})
}

// payloadOrdenado resolve os ids pelo dicionário e emite os campos em ordem
// crescente — invariante que o Put revalida antes de tornar o record durável.
func payloadOrdenado(t *testing.T, s *Store, campos map[string]types.Value) []byte {
	t.Helper()
	type par struct {
		id types.FieldID
		v  types.Value
	}
	pares := make([]par, 0, len(campos))
	for nome, v := range campos {
		pares = append(pares, par{idDoCampo(t, s, nome), v})
	}
	sort.Slice(pares, func(i, j int) bool { return pares[i].id < pares[j].id })
	var p []byte
	for _, x := range pares {
		p = mustAppendField(t, p, x.id, x.v)
	}
	return p
}

// Sem o predicado de leitura, a visibilidade dependeria do agendador: uma row
// vencida continuaria aparecendo até o coletor passar, e `nishidb query` e
// `nishidb server` divergiriam sobre o mesmo dataset.
func TestRowVencidaSomeDaLeituraSemNenhumColetorTerRodado(t *testing.T) {
	s, rel := storeComExpiracao(t, t.TempDir(), time.Hour)
	id, _ := mustPut(t, s, "A", 1, payloadComEscrita(t, s, "A", "Start", rel.t), nil)

	if _, _, ok, err := s.GetByKey(s.AbreSnapshot(), []byte("A"), nil); err != nil || !ok {
		t.Fatalf("antes do prazo: ok=%v err=%v", ok, err)
	}
	if rows := collectRows(t, s.ScanLive(s.AbreSnapshot())); len(rows) != 1 {
		t.Fatalf("ScanLive antes do prazo: %v", rows)
	}

	rel.avanca(time.Hour + time.Minute)

	if _, _, ok, err := s.GetByKey(s.AbreSnapshot(), []byte("A"), nil); err != nil || ok {
		t.Fatalf("depois do prazo GetByKey: ok=%v err=%v", ok, err)
	}
	if _, _, ok, err := s.GetByRowID(s.AbreSnapshot(), id, nil); err != nil || ok {
		t.Fatalf("depois do prazo GetByRowID: ok=%v err=%v", ok, err)
	}
	if rows := collectRows(t, s.ScanLive(s.AbreSnapshot())); len(rows) != 0 {
		t.Fatalf("ScanLive depois do prazo: %v", rows)
	}
	// Mas a ingestão continua vendo a entidade, e é isso que impede a
	// ressurreição por evento antigo.
	if _, _, pres, err := s.Estado([]byte("A"), nil); err != nil || pres != types.PresencaExpirada {
		t.Fatalf("Estado: %v, err=%v; quer expirada", pres, err)
	}
	if _, pres := s.Sequence(id); pres != types.PresencaExpirada {
		t.Fatalf("Sequence: %v; quer expirada", pres)
	}
}

// Toda escrita renova: alarme que recebe update sem parar nunca expira.
func TestEscritaNovaRenovaOPrazo(t *testing.T) {
	s, rel := storeComExpiracao(t, t.TempDir(), time.Hour)
	id, _ := mustPut(t, s, "A", 1, payloadComEscrita(t, s, "A", "Start", rel.t), nil)

	rel.avanca(50 * time.Minute)
	rec, _, pres, err := s.Estado([]byte("A"), nil)
	if err != nil || pres != types.PresencaViva {
		t.Fatalf("pres=%v err=%v", pres, err)
	}
	if _, _, err := s.Put([]byte("A"), 2, 0, payloadComEscrita(t, s, "A", "Alive", rel.t), &rec); err != nil {
		t.Fatal(err)
	}

	rel.avanca(50 * time.Minute) // 100 min do primeiro Put, 50 do segundo
	if _, _, ok, err := s.GetByRowID(s.AbreSnapshot(), id, nil); err != nil || !ok {
		t.Fatalf("a escrita não renovou o prazo: ok=%v err=%v", ok, err)
	}
}

// Zero é DESCONHECIDO. Row anterior à ativação da expiração — ou que perdeu o
// índice — não pode sumir por falta de informação.
func TestRowSemInstanteDeEscritaNaoVence(t *testing.T) {
	s, rel := storeComExpiracao(t, t.TempDir(), time.Hour)
	// payload SEM o campo de sistema, como um dataset materializado antes de
	// a expiração ser declarada.
	id, _ := mustPut(t, s, "A", 1, payloadSemEscrita(t, s, "A"), nil)

	rel.avanca(100 * time.Hour)
	if _, _, ok, err := s.GetByRowID(s.AbreSnapshot(), id, nil); err != nil || !ok {
		t.Fatalf("row com prazo desconhecido sumiu: ok=%v err=%v", ok, err)
	}
}

// A lápide é um RECORD, não um bit: applyRecord deriva a flag publicada do
// próprio record, e o replay reconstrói as locations a partir deles. Uma
// lápide só na location seria apagada pelo primeiro replay.
func TestExpiraPublicaLapideDuravelEBaixaALiveness(t *testing.T) {
	dir := t.TempDir()
	s, rel := storeComExpiracao(t, dir, time.Hour)
	id, _ := mustPut(t, s, "A", 7, payloadComEscrita(t, s, "A", "Start", rel.t), nil)

	rel.avanca(2 * time.Hour)
	expirou, err := s.Expira(id)
	if err != nil {
		t.Fatal(err)
	}
	if !expirou {
		t.Fatal("Expira devolveu false para row vencida")
	}
	// Idempotente.
	if again, err := s.Expira(id); err != nil || again {
		t.Fatalf("segunda chamada: %v, %v", again, err)
	}

	// A lápide preserva o lastSequence do §58.
	if seq, pres := s.Sequence(id); pres != types.PresencaExpirada || seq != 7 {
		t.Fatalf("lastSequence = %d, %v; quer 7, expirada", seq, pres)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reabre: a lápide sobreviveu porque é record.
	s2, rel2 := storeComExpiracao(t, dir, time.Hour)
	rel2.avanca(3 * time.Hour)
	if _, _, ok, err := s2.GetByRowID(s2.AbreSnapshot(), id, nil); err != nil || ok {
		t.Fatalf("a row voltou a ser visível depois de reabrir: ok=%v err=%v", ok, err)
	}
	if _, _, pres, err := s2.Estado([]byte("A"), nil); err != nil || pres != types.PresencaExpirada {
		t.Fatalf("Estado após reabrir: %v, err=%v", pres, err)
	}
}

// A corrida com o Put não tem janela: os dois tomam o wmu, e o Expira relê a
// location E o prazo lá dentro.
func TestExpiraAbortaSemEscreverQuandoARowFoiRenovada(t *testing.T) {
	s, rel := storeComExpiracao(t, t.TempDir(), time.Hour)
	id, _ := mustPut(t, s, "A", 1, payloadComEscrita(t, s, "A", "Start", rel.t), nil)

	rel.avanca(2 * time.Hour)
	rec, _, pres, err := s.Estado([]byte("A"), nil)
	if err != nil || pres != types.PresencaExpirada {
		t.Fatalf("pres=%v err=%v", pres, err)
	}
	// Renova ANTES de o coletor chamar Expira.
	if _, _, err := s.Put([]byte("A"), 2, 0, payloadComEscrita(t, s, "A", "Alive", rel.t), &rec); err != nil {
		t.Fatal(err)
	}
	expirou, err := s.Expira(id)
	if err != nil {
		t.Fatal(err)
	}
	if expirou {
		t.Fatal("o coletor expirou uma row renovada")
	}
	if _, _, ok, err := s.GetByRowID(s.AbreSnapshot(), id, nil); err != nil || !ok {
		t.Fatalf("a row renovada sumiu: ok=%v err=%v", ok, err)
	}
}

// O índice é DERIVADO: applyRecord o preenche do payload, e é o mesmo
// applyRecord que o replay do tail usa. Sem isso, a row nascida no tail
// ficaria com prazo desconhecido para sempre depois de um crash.
func TestExpiryIdxEhReconstruidoPeloReplayDoTail(t *testing.T) {
	dir := t.TempDir()
	s, rel := storeComExpiracao(t, dir, time.Hour)
	escrita := rel.t
	id, _ := mustPut(t, s, "A", 1, payloadComEscrita(t, s, "A", "Start", escrita), nil)
	// SEM checkpoint: o record está no segmento e o manifest não o conhece.
	// É a janela que o replay do tail cobre.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, rel2 := storeComExpiracao(t, dir, time.Hour)
	// O instante reconstruído tem de ser o do PAYLOAD, não o do replay: se
	// viesse do relógio, o prazo teria sido empurrado para frente.
	rel2.t = escrita.Add(2 * time.Hour)
	if _, _, ok, err := s2.GetByRowID(s2.AbreSnapshot(), id, nil); err != nil || ok {
		t.Fatalf("o prazo foi reancorado no replay: ok=%v err=%v", ok, err)
	}
	rel2.t = escrita.Add(30 * time.Minute)
	if _, _, ok, err := s2.GetByRowID(s2.AbreSnapshot(), id, nil); err != nil || !ok {
		t.Fatalf("antes do prazo a row sumiu: ok=%v err=%v", ok, err)
	}
}

// FieldID 0 é reservado: sem o campo, nada teria de onde tirar o instante e
// a expiração nunca aconteceria, em silêncio.
func TestExpiracaoSemCampoDeEscritaEhRecusadaNaAbertura(t *testing.T) {
	_, err := Open(Options{Dir: t.TempDir(), TTLAtivo: true})
	if err == nil {
		t.Fatal("aberto sem CampoDeEscrita")
	}
}

func TestExpiraEmDatasetSemExpiracaoEhErro(t *testing.T) {
	s := openStore(t, Options{Dir: t.TempDir()})
	id, _ := mustPut(t, s, "A", 1, statusPayload(t, s, "A", "Start", 0), nil)
	if _, err := s.Expira(id); err == nil {
		t.Fatal("Expira aceito num dataset sem expiração")
	}
}

// instanteDeEscrita para no primeiro FieldID maior: o payload é ordenado, e
// varrer o resto seria custo por row sem ganho.
func TestInstanteDeEscritaLeOCampoCertoOuNadaInforma(t *testing.T) {
	var p []byte
	p = mustAppendFieldSolto(t, p, 3, types.Value{Type: types.TypeInt64, I64: 10})
	p = mustAppendFieldSolto(t, p, 9, types.Value{Type: types.TypeTimestamp, I64: 1787878442000})
	p = mustAppendFieldSolto(t, p, 12, types.Value{Type: types.TypeInt64, I64: 20})

	if ms, ok := instanteDeEscrita(p, 9); !ok || ms != 1787878442000 {
		t.Fatalf("ms=%d ok=%v", ms, ok)
	}
	if _, ok := instanteDeEscrita(p, 7); ok {
		t.Error("campo ausente foi reportado como conhecido")
	}
	if _, ok := instanteDeEscrita(p, 20); ok {
		t.Error("campo além do fim foi reportado como conhecido")
	}
	// Tipo errado no campo não vira instante plausível.
	var q []byte
	q = mustAppendFieldSolto(t, q, 9, types.Value{Type: types.TypeInt64, I64: 5})
	if _, ok := instanteDeEscrita(q, 9); ok {
		t.Error("campo com tipo não-timestamp foi aceito")
	}
}

func mustAppendFieldSolto(t *testing.T, dst []byte, id types.FieldID, v types.Value) []byte {
	t.Helper()
	out, err := encoding.AppendField(dst, id, v)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
