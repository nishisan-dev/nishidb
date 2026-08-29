package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// hashDir é o diretório dos arquivos de índice hash do dataset — um por
// campo. Os testes de recovery o regridem ao estado do último checkpoint, que
// é o que um power loss deixaria.
func hashDir(dir string) string { return filepath.Join(dir, "indexes", "hash") }

func chaveDe(i int) string { return fmt.Sprintf("CMP-V0001PPCLP-%03d", i) }

// valorDoCampo extrai um campo do payload. Existe para que a asserção sobre a
// revalidação do §44 fale do ESTADO CORRENTE da row, e não do que o teste
// acha que gravou.
func valorDoCampo(payload []byte, campo types.FieldID) (types.Value, bool) {
	var r encoding.FieldReader
	r.Reset(payload)
	for {
		id, v, ok := r.Next()
		if !ok {
			return types.Value{}, false
		}
		if id == campo {
			return v, true
		}
	}
}

// TestHasHashEhACapabilityQueOPlannerEsperava é a razão de esta frente
// existir. O `index: hash` era aceito no YAML e o planner tinha a regra R2,
// mas nenhum Store implementava a capability: `p.hash` ficava nil e toda
// igualdade sobre a coluna degradava para FullScan em silêncio.
func TestHasHashEhACapabilityQueOPlannerEsperava(t *testing.T) {
	s := openStore(t, Options{HashFieldNames: []string{"id", "status", "counter"}})
	for _, campo := range []types.FieldID{fldID, fldStatus, fldCounter} {
		if !s.HasHash(campo) {
			t.Fatalf("HasHash(%d) = false num campo declarado", campo)
		}
	}
	if s.HasHash(999) {
		t.Fatal("HasHash de campo não declarado devia ser false")
	}
	if got := s.HashFields(); !slices.Equal(got, []types.FieldID{fldID, fldStatus, fldCounter}) {
		t.Fatalf("HashFields = %v", got)
	}
}

// TestLookupHashEncontraAsRowsDoValor cobre o caminho feliz ponta a ponta no
// Store, incluindo a ORDEM crescente que o executor assume.
func TestLookupHashEncontraAsRowsDoValor(t *testing.T) {
	s := openStore(t, Options{HashFields: []types.FieldID{fldStatus}})
	var esperadas []types.RowID
	for i, estado := range []string{"Start", "Stop", "Start", "Interim", "Start"} {
		k := chaveDe(i)
		row, _ := mustPut(t, s, k, int64(i), statusPayload(t, s, k, estado, int64(i)), nil)
		if estado == "Start" {
			esperadas = append(esperadas, row)
		}
	}
	it, err := s.LookupHash(fldStatus, strVal("Start"))
	if err != nil {
		t.Fatal(err)
	}
	got := collectRows(t, it)
	slices.Sort(esperadas)
	if !slices.Equal(got, esperadas) {
		t.Fatalf("LookupHash(Start) = %v, esperado %v", got, esperadas)
	}
	if !slices.IsSorted(got) {
		t.Fatalf("LookupHash devolveu fora de ordem: %v", got)
	}
}

// TestLookupHashDeCampoNaoIndexadoEhErro: o planner degrada quando a
// capability falta, então chegar aqui é bug de plan-time — e ele tem de doer.
func TestLookupHashDeCampoNaoIndexadoEhErro(t *testing.T) {
	s := openStore(t, Options{})
	if _, err := s.LookupHash(fldStatus, strVal("Start")); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("esperado ErrInvalidArgument, obtido %v", err)
	}
}

// TestUpdateRemoveAPostingDoValorAntigo mede hoje o que SUBSTITUIU a remoção
// que dá nome a ele. O nome fica porque `docs/debitos.md` o cita como prova do
// débito 19, e renomeá-lo quebraria a conferência do
// TestEvidenciasCitadasNosDocumentosExistem.
//
// O índice por segmento não tem fase de CLEAR (spec §5.2), e a ausência é o
// desenho: a posting do valor antigo mora no índice do segmento ANTIGO, vira
// falso positivo e morre quando a compaction ou a retenção aposenta aquele
// segmento. Com o Clear somem a lápide e o falso negativo que ela existia para
// evitar.
//
// Então a asserção sobe um nível: o que a query devolve — o resultado
// pós-revalidação (§44) — continua exato, e é ele que este teste passa a
// afirmar. A posting do valor NOVO segue exigida com igualdade: perdê-la seria
// falso negativo, e nenhuma camada acima recupera candidato que o índice não
// devolveu.
func TestUpdateRemoveAPostingDoValorAntigo(t *testing.T) {
	s := openStore(t, Options{HashFields: []types.FieldID{fldStatus}})
	k := "CMP-V0001PPCLP-013"
	row, _ := mustPut(t, s, k, 1, statusPayload(t, s, k, "Start", 1), nil)

	prev, _, ok, err := s.GetByKey([]byte(k), nil)
	if err != nil || !ok {
		t.Fatalf("GetByKey: (%v,%v)", ok, err)
	}
	mustPut(t, s, k, 2, statusPayload(t, s, k, "Stop", 2), &prev)

	itNovo, err := s.LookupHash(fldStatus, strVal("Stop"))
	if err != nil {
		t.Fatal(err)
	}
	if got := collectRows(t, itNovo); !slices.Equal(got, []types.RowID{row}) {
		t.Fatalf("LookupHash(Stop) = %v, esperado [%d]", got, row)
	}
	if got := lookupRevalidado(t, s, fldStatus, strVal("Start")); len(got) != 0 {
		t.Fatalf("pós-revalidação, a row não pode mais responder pelo valor ANTIGO: %v", got)
	}
}

// TestPostingSobreviveAoRestart: o índice é arquivo, não cache. Um restart que
// perdesse as postings faria a primeira query depois do reinício devolver
// menos linhas do que existem.
func TestPostingSobreviveAoRestart(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, HashFields: []types.FieldID{fldStatus}}
	s := openStore(t, opts)
	k := "CMP-V0001PPCLP-013"
	row, _ := mustPut(t, s, k, 1, statusPayload(t, s, k, "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := openStore(t, opts)
	it, err := s2.LookupHash(fldStatus, strVal("Start"))
	if err != nil {
		t.Fatal(err)
	}
	if got := collectRows(t, it); !slices.Equal(got, []types.RowID{row}) {
		t.Fatalf("posting não sobreviveu ao restart: %v", got)
	}
}

// TestReplayDoTailReconstroiAsPostings é o caso de CRASH, e é diferente do
// restart limpo acima: o índice em disco fica no estado do último checkpoint, e
// tudo o que veio depois só existe no tail dos segmentos.
//
// O índice é restaurado de um backup tirado no ponto do checkpoint — que é
// exatamente o que um power loss deixaria, já que o msync do índice acontece
// ali — e a abertura seguinte tem de reconstruir as postings replayando os
// records. Sem isso, a query depois do crash omitiria as rows escritas no
// intervalo, sem erro em camada nenhuma.
func TestReplayDoTailReconstroiAsPostings(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, HashFields: []types.FieldID{fldStatus}}
	s := openStore(t, opts)

	k0 := chaveDe(0)
	row0, _ := mustPut(t, s, k0, 1, statusPayload(t, s, k0, "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// Estado do índice NO ponto S.
	backup := filepath.Join(t.TempDir(), "hash-em-S")
	copiaDir(t, hashDir(dir), backup)

	// Escritas depois do checkpoint: só o tail as conhece.
	var depois []types.RowID
	for i := 1; i <= 3; i++ {
		k := chaveDe(i)
		row, _ := mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
		depois = append(depois, row)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Volta o índice para o estado de S: é o que o crash teria deixado.
	copiaDir(t, backup, hashDir(dir))

	s2 := openStore(t, opts)
	it, err := s2.LookupHash(fldStatus, strVal("Start"))
	if err != nil {
		t.Fatal(err)
	}
	got := collectRows(t, it)
	quer := append([]types.RowID{row0}, depois...)
	slices.Sort(quer)
	if !slices.Equal(got, quer) {
		t.Fatalf("o replay do tail não reconstruiu as postings: %v, esperado %v", got, quer)
	}
}

// TestReplayReaplicaOClearDoUpdate mantém o nome porque `docs/debitos.md` o
// cita no débito 19, mas o Clear que ele media não existe mais no índice por
// segmento (spec §5.2) — e, com ele, sumiu a categoria inteira de "o replay
// esqueceu de limpar". Um replay que só SOMA é agora o comportamento correto:
// não há lápide a reaplicar, e a posting sobrante é o falso positivo que o §44
// descarta.
//
// O que continua sendo propriedade de produto, e é o que este teste afirma: o
// replay religa a posting do valor NOVO (falso negativo nunca) e a query
// pós-restart devolve o mesmo conjunto que devolveria sem o crash.
func TestReplayReaplicaOClearDoUpdate(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, HashFields: []types.FieldID{fldStatus}}
	s := openStore(t, opts)

	k := "CMP-V0001PPCLP-013"
	mustPut(t, s, k, 1, statusPayload(t, s, k, "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(t.TempDir(), "hash-em-S")
	copiaDir(t, hashDir(dir), backup)

	prev, _, ok, err := s.GetByKey([]byte(k), nil)
	if err != nil || !ok {
		t.Fatalf("GetByKey: (%v,%v)", ok, err)
	}
	mustPut(t, s, k, 2, statusPayload(t, s, k, "Stop", 2), &prev)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	copiaDir(t, backup, hashDir(dir))

	s2 := openStore(t, opts)
	it, err := s2.LookupHash(fldStatus, strVal("Stop"))
	if err != nil {
		t.Fatal(err)
	}
	if got := collectRows(t, it); len(got) != 1 {
		t.Fatalf("o replay não religou a posting do valor novo: %v", got)
	}
	if got := lookupRevalidado(t, s2, fldStatus, strVal("Start")); len(got) != 0 {
		t.Fatalf("pós-replay e pós-revalidação, o valor ANTIGO não pode devolver a row: %v", got)
	}
}

// TestDeclararHashEmDatasetComLinhasEhRecusado: o índice nasceria cobrindo só
// o que vier daqui pra frente, e a query indexada omitiria as rows antigas —
// sem erro, sem log e sem métrica. Backfill não existe (débito 22).
func TestDeclararHashEmDatasetComLinhasEhRecusado(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir})
	k := "CMP-V0001PPCLP-013"
	mustPut(t, s, k, 1, statusPayload(t, s, k, "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	_, err := Open(Options{Dir: dir, HashFields: []types.FieldID{fldStatus}})
	if !errors.Is(err, ErrDatasetCorrupt) {
		t.Fatalf("esperado ErrDatasetCorrupt ao declarar hash sobre dataset com linhas, obtido %v", err)
	}
}

// TestAbandonarHashDeclaradoNoManifestEhRecusado e a autorização explícita:
// abrir sem o campo faria o índice parar de receber sets e clears, e ele
// reapareceria INCOMPLETO se o campo voltasse.
func TestAbandonarHashDeclaradoNoManifestEhRecusado(t *testing.T) {
	dir := t.TempDir()
	comHash := Options{Dir: dir, HashFields: []types.FieldID{fldStatus}}
	s := openStore(t, comHash)
	k := "CMP-V0001PPCLP-013"
	mustPut(t, s, k, 1, statusPayload(t, s, k, "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(Options{Dir: dir}); !errors.Is(err, ErrDatasetCorrupt) {
		t.Fatalf("abrir sem o campo indexado devia ser recusado, obtido %v", err)
	}
	s2, err := Open(Options{Dir: dir, AllowHashDrop: true})
	if err != nil {
		t.Fatalf("AllowHashDrop devia autorizar a abertura: %v", err)
	}
	defer func() { _ = s2.Close() }()
	if s2.HasHash(fldStatus) {
		t.Fatal("com o campo dropado o Store não pode declarar a capability")
	}
}

// TestManifestPublicaOConjuntoDeHashes: é dele que a abertura seguinte tira o
// conjunto contra o qual decide abandono e backfill. Um manifest que não o
// nomeasse tornaria as duas guardas acima inúteis.
func TestManifestPublicaOConjuntoDeHashes(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir, HashFields: []types.FieldID{fldStatus, fldCounter}})
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	m, err := checkpoint.Load(filepath.Join(dir, "checkpoint", "manifest.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Hashes) != 2 {
		t.Fatalf("Hashes no manifest: %+v", m.Hashes)
	}
	// Ordem determinística: dois checkpoints do mesmo estado produzem
	// manifests idênticos byte a byte, e um mapa Go itera sorteado.
	if m.Hashes[0].FieldID != fldStatus || m.Hashes[1].FieldID != fldCounter {
		t.Fatalf("ordem não canônica: %+v", m.Hashes)
	}
	if m.Hashes[0].SlotCount == 0 {
		t.Fatalf("SlotCount não foi publicado: %+v", m.Hashes)
	}
	// Um dataset SEM índice hash não pode ganhar cauda: é o que preserva o
	// rollback do formato do manifest.
	semHash := t.TempDir()
	s2 := openStore(t, Options{Dir: semHash})
	if err := s2.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(semHash, "checkpoint", "manifest.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if raw[4] != 1 {
		t.Fatalf("dataset sem hash gravou manifest v%d; o rollback exige v1", raw[4])
	}
}

// TestValorAcimaDoTetoEmCampoComHashEhRecusadoAntesDoAppend: descobrir a
// violação depois do append é fatal — o record fica durável, o replay do tail
// tenta reindexá-lo a cada abertura, falha sempre, e o dataset inteiro passa a
// ser INABRÍVEL.
func TestValorAcimaDoTetoEmCampoComHashEhRecusadoAntesDoAppend(t *testing.T) {
	s := openStore(t, Options{HashFields: []types.FieldID{fldStatus}})
	k := "CMP-V0001PPCLP-013"
	grande := string(make([]byte, types.MaxIndexedValueBytes+1))
	_, _, err := s.Put([]byte(k), 1, 0, statusPayload(t, s, k, grande, 1), nil)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("esperado ErrInvalidArgument, obtido %v", err)
	}
	if !errors.Is(err, types.ErrTooLarge) {
		t.Fatalf("o sentinela de culpa do EVENTO se perdeu: %v", err)
	}
	// E o dataset continua utilizável: nada foi appendado.
	if _, _, err := s.Put([]byte(k), 2, 0, statusPayload(t, s, k, "Start", 1), nil); err != nil {
		t.Fatalf("o Put recusado deixou o writer falho: %v", err)
	}
}

// TestPostingOrfaEhSupersetEOExecutorFiltra prova o contrato do §44 no motor
// hash: o índice devolve candidato a MAIS, nunca a menos, e quem decide é a
// revalidação.
//
// A posting órfã é forçada aqui pela falha injetada na fase de Clear, que é o
// caminho tolerado do §10.1 — e é a MESMA forma de falso positivo que uma
// colisão de hash produz, já que o índice não valida colisão por desenho (o
// godoc de LookupHash diz isso). Forçar uma colisão real de 64 bits não é
// construível num teste; forçar o Clear a falhar é, e o candidato resultante é
// indistinguível para o executor.
func TestPostingOrfaEhSupersetEOExecutorFiltra(t *testing.T) {
	s := openStore(t, Options{HashFields: []types.FieldID{fldStatus}})
	s.SetClearErrHookForTest(func(types.FieldID) error { return errors.New("clear de mentira") })

	k := "CMP-V0001PPCLP-013"
	row, _ := mustPut(t, s, k, 1, statusPayload(t, s, k, "Start", 1), nil)
	prev, _, ok, err := s.GetByKey([]byte(k), nil)
	if err != nil || !ok {
		t.Fatalf("GetByKey: (%v,%v)", ok, err)
	}
	mustPut(t, s, k, 2, statusPayload(t, s, k, "Stop", 2), &prev)

	it, err := s.LookupHash(fldStatus, strVal("Start"))
	if err != nil {
		t.Fatal(err)
	}
	if got := collectRows(t, it); !slices.Equal(got, []types.RowID{row}) {
		t.Fatalf("o Clear falho devia deixar a posting órfã (superset): %v", got)
	}
	// A revalidação é do executor, e ela parte do record CORRENTE: a row
	// existe, está viva, e o valor dela já é o novo. É por isso que o
	// candidato a mais nunca vira linha a mais.
	rec, _, vivo, err := s.GetByRowID(row, nil)
	if err != nil || !vivo {
		t.Fatalf("GetByRowID: (%v,%v)", vivo, err)
	}
	if v, achou := valorDoCampo(rec.Payload, fldStatus); !achou || string(v.Bytes) != "Stop" {
		t.Fatalf("o estado corrente da row devia ser Stop: %q (achou=%v)", v.Bytes, achou)
	}
}

// TestCloseNoMeioDaVarreduraDoHashNaoDaPanic: o iterador SOBREVIVE à chamada
// que o criou, e um Close concorrente não pode virar panic — a política de
// log-and-skip da ingestão não captura panic, ela derruba o processo.
func TestCloseNoMeioDaVarreduraDoHashNaoDaPanic(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir, SegmentSize: 1 << 20, HashFields: []types.FieldID{fldStatus}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		k := chaveDe(i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	it, err := s.LookupHash(fldStatus, strVal("Start"))
	if err != nil {
		t.Fatal(err)
	}
	if !it.Next() {
		t.Fatal("o iterador devia ter ao menos uma row")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if it.Next() {
		t.Fatal("a varredura continuou depois do Close do dataset")
	}
	if !errors.Is(it.Err(), ErrDatasetClosed) {
		t.Fatalf("Err() = %v, esperado ErrDatasetClosed", it.Err())
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestLapideLimpaAsPostingsDeHash mantém o nome — `docs/debitos.md` o cita —
// mas o que ele trava mudou de lugar, e o próprio comentário original já
// antecipava a razão: a limpeza da lápide nunca foi corretude, e sim custo.
// "A revalidação do §44 a filtraria, então o custo não seria resultado errado:
// seria candidato morto varrido para sempre."
//
// No índice por segmento não há Clear (spec §5.2), então a posting da row morta
// sobrevive — e o custo que ela representa tem prazo, ao contrário do que tinha
// no motor global: ela morre quando a compaction ou a retenção aposenta o
// segmento em que mora. É a segunda dor da spec resolvida por construção.
//
// O que este teste afirma agora é a propriedade de PRODUTO: depois da lápide, a
// row expirada não é devolvida pela consulta — o que é asserção sobre o
// resultado pós-revalidação, e não sobre a estreiteza do superconjunto.
func TestLapideLimpaAsPostingsDeHash(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{
		Dir:            dir,
		TTLAtivo:       true,
		DuracaoDeFundo: time.Hour,
		CampoDeEscrita: "_writtenAt",
		CampoDePrazo:   "_expiresIn",
		HashFieldNames: []string{"status"},
	})
	rel := &relogioDeTeste{t: time.Date(2026, 8, 25, 20, 0, 0, 0, time.UTC)}
	s.UsaRelogio(rel.agora)

	// O id do `status` é resolvido pelo DICIONÁRIO, e não pela constante
	// posicional do pacote: num dataset com expiração o Open registra os dois
	// campos de sistema antes de qualquer campo indexado, então o id fixo
	// apontaria para o `_expiresIn`.
	statusID := idDoCampo(t, s, "status")

	k := "CMP-V0001PPCLP-013"
	row, _ := mustPut(t, s, k, 1, payloadComEscrita(t, s, k, "Start", rel.t), nil)
	if got := postingsDeHashDeStatus(t, s, statusID, strVal("Start")); !slices.Equal(got, []types.RowID{row}) {
		t.Fatalf("posting antes da lápide: %v", got)
	}

	rel.avanca(2 * time.Hour)
	expirou, err := s.Expira(row)
	if err != nil {
		t.Fatal(err)
	}
	if !expirou {
		t.Fatal("Expira devolveu false para row vencida: o teste não chegou a exercitar a lápide")
	}
	if got := lookupRevalidado(t, s, statusID, strVal("Start")); len(got) != 0 {
		t.Fatalf("a row expirada continuou sendo devolvida por status=Start: %v", got)
	}
}

func postingsDeHashDeStatus(t *testing.T, s *Store, campo types.FieldID, v types.Value) []types.RowID {
	t.Helper()
	it, err := s.LookupHash(campo, v)
	if err != nil {
		t.Fatal(err)
	}
	return collectRows(t, it)
}
