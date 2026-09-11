package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/keyformat"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/storage/fielddict"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// avaliadorByID é o `where authenticated = true` sem binder: o Store só
// conhece a interface, e o teste de integração não precisa de um schema
// compilado para exercitar o caminho inteiro.
type avaliadorByID struct {
	field types.FieldID
	fr    encoding.FieldReader
}

func (a *avaliadorByID) Evaluate(payload []byte) (bool, error) {
	a.fr.Reset(payload)
	for {
		id, v, ok := a.fr.Next()
		if !ok {
			return false, a.fr.Err()
		}
		if id == a.field {
			return v.Type == types.TypeBool && v.I64 == 1, nil
		}
	}
}

// counterOfTest descreve um contador para o callback de abertura montar.
type counterOfTest struct {
	name      string
	groupBy   []string
	where     bool // "authenticated = true"
	ceiling   uint32
	retention time.Duration
}

// callbackOfCounters é o CompilaContadores dos testes: resolve os nomes pelo
// dicionário que o Open entrega — como o compilador de verdade faz — e devolve
// as definições.
func callbackOfCounters(cs ...counterOfTest) func(*fielddict.Dict) ([]counter.Definition, error) {
	return func(dict *fielddict.Dict) ([]counter.Definition, error) {
		out := make([]counter.Definition, 0, len(cs))
		for _, c := range cs {
			def := counter.Definition{Name: c.name, MaxGroups: c.ceiling, RetainEmptyGroups: c.retention}
			if def.MaxGroups == 0 {
				def.MaxGroups = 100
			}
			for _, col := range c.groupBy {
				id, err := dict.LookupOrCreate([]byte(col))
				if err != nil {
					return nil, err
				}
				def.Columns = append(def.Columns, id)
				def.Names = append(def.Names, col)
				tipo := types.TypeString
				if col == "slot" || col == "port" {
					tipo = types.TypeInt64
				}
				def.Types = append(def.Types, tipo)
			}
			if c.where {
				auth, err := dict.LookupOrCreate([]byte("authenticated"))
				if err != nil {
					return nil, err
				}
				def.Where = "authenticated = true"
				def.NewEvaluator = func() counter.Evaluator { return &avaliadorByID{field: auth} }
			}
			def.Format = keyformat.Default(keyformat.Columns(def.Names, def.Types))
			out = append(out, def)
		}
		return out, nil
	}
}

func byOLT(name string, where bool) counterOfTest {
	return counterOfTest{name: name, groupBy: []string{"olt"}, where: where}
}

// payloadServico é o estado de um serviço como o materializer o produziria:
// com o instante de escrita quando o dataset tem expiração.
func payloadServico(t *testing.T, s *Store, key, olt string, auth bool, write time.Time) []byte {
	t.Helper()
	fields := map[string]types.Value{
		"id":            {Type: types.TypeString, Bytes: []byte(key)},
		"authenticated": {Type: types.TypeBool},
	}
	if auth {
		fields["authenticated"] = types.Value{Type: types.TypeBool, I64: 1}
	}
	if olt != "" {
		fields["olt"] = types.Value{Type: types.TypeString, Bytes: []byte(olt)}
	}
	if !write.IsZero() {
		fields["_writtenAt"] = types.Value{Type: types.TypeTimestamp, I64: write.UnixMilli()}
	}
	return payloadOrdenado(t, s, fields)
}

func counterChamado(t *testing.T, s *Store, name string) *counter.Counter {
	t.Helper()
	for _, c := range s.Counters() {
		if c.Def().Name == name {
			return c
		}
	}
	t.Fatalf("contador %q não está aberto", name)
	return nil
}

// totalOfCounter devolve total e refs da OLT no contador nomeado.
func totalOfCounter(t *testing.T, s *Store, name, olt string) (total, refs int64) {
	t.Helper()
	c := counterChamado(t, s, name)
	id, ok := c.ByText([]byte(olt))
	if !ok {
		return 0, 0
	}
	_, _, total, refs, _ = c.Group(id)
	return total, refs
}

func exigeTotal(t *testing.T, s *Store, name, olt string, total, refs int64) {
	t.Helper()
	if gt, gr := totalOfCounter(t, s, name, olt); gt != total || gr != refs {
		t.Fatalf("%s[%s]: total %d refs %d, quer %d e %d", name, olt, gt, gr, total, refs)
	}
}

func optionsWithCounters(dir string, cs ...counterOfTest) Options {
	return Options{
		Dir:                             dir,
		SegmentSize:                     1 << 20,
		ActiveTTL:                       true,
		FallbackDuration:                time.Hour,
		WriteInstantFieldName:           "_writtenAt",
		TTLFieldName:                    "_expiresIn",
		CompileCountersCallbackFunction: callbackOfCounters(cs...),
	}
}

func storeWithCounters(t *testing.T, dir string, cs ...counterOfTest) (*Store, *relogioOfTest) {
	t.Helper()
	s := openStore(t, optionsWithCounters(dir, cs...))
	rel := &relogioOfTest{t: time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
	s.UsaRelogio(rel.agora)
	return s, rel
}

func TestContadorAcompanhaPutLapideECopyForward(t *testing.T) {
	s, rel := storeWithCounters(t, t.TempDir(), byOLT("aut", true), byOLT("serv", false))
	const key = "cliente-1"
	rowID, _ := mustPut(t, s, key, 1, payloadServico(t, s, key, "A", true, rel.agora()), nil)
	exigeTotal(t, s, "aut", "A", 1, 1)
	exigeTotal(t, s, "serv", "A", 1, 1)

	prev, _, _, err := s.GetByKey(s.presente(), []byte(key), nil)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, s, key, 2, payloadServico(t, s, key, "A", false, rel.agora()), &prev)
	exigeTotal(t, s, "aut", "A", 0, 1)
	exigeTotal(t, s, "serv", "A", 1, 1)

	prev, _, _, _ = s.GetByKey(s.presente(), []byte(key), nil)
	mustPut(t, s, key, 3, payloadServico(t, s, key, "A", true, rel.agora()), &prev)
	exigeTotal(t, s, "aut", "A", 1, 1)

	// A lápide da expiração passa pelo applyRecord e desconta; o grupo fica.
	rel.advance(2 * time.Hour)
	if expirou, err := s.Expira(rowID); err != nil || !expirou {
		t.Fatalf("Expira: %v, %v", expirou, err)
	}
	exigeTotal(t, s, "aut", "A", 0, 0)
	exigeTotal(t, s, "serv", "A", 0, 0)
	c := counterChamado(t, s, "aut")
	if id, ok := c.ByText([]byte("A")); !ok {
		t.Fatal("o grupo que esvaziou continua vivo")
	} else if _, _, _, _, vivo := c.Group(id); !vivo {
		t.Fatal("o grupo que esvaziou continua vivo")
	}

	// A ressurreição volta a contar. prev é nil: a linha lapidada não é
	// visível ao GetByKey, e o Put a reencontra sozinho pela primary.
	mustPut(t, s, key, 4, payloadServico(t, s, key, "A", true, rel.agora()), nil)
	exigeTotal(t, s, "aut", "A", 1, 1)
}

// TestCopyForwardNaoMexeNoContador: a compaction reapenda o record VIVO com o
// mesmo payload — transição G → G, delta zero.
func TestCopyForwardNaoMexeNoContador(t *testing.T) {
	opts := optionsWithCounters(t.TempDir(), byOLT("aut", true))
	opts.SegmentSize = 400
	s := openStore(t, opts)
	rel := &relogioOfTest{t: time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
	s.UsaRelogio(rel.agora)
	const key = "cf-alvo"
	rowID, _ := mustPut(t, s, key, 1, payloadServico(t, s, key, "A", true, rel.agora()), nil)
	origSec, origOff := localizacaoOfRow(t, s, rowID)
	for i := 0; s.Stats().CurrentSegmentID == origSec && i < 200; i++ {
		k := "enche-" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		mustPut(t, s, k, int64(i), payloadServico(t, s, k, "B", false, rel.agora()), nil)
	}
	exigeTotal(t, s, "aut", "A", 1, 1)
	rec := recordIn(t, s, origSec, origOff)
	copiado, _, err := s.reapendaSeVivo(origSec, origOff, rec)
	if err != nil || !copiado {
		t.Fatalf("reapendaSeVivo: %v, %v", copiado, err)
	}
	exigeTotal(t, s, "aut", "A", 1, 1)
	if got := counterChamado(t, s, "aut").Transitions(); got == 0 {
		t.Fatal("as escritas anteriores tinham de ter transicionado")
	}
}

// TestExpiracaoOportunistaDescontaDoContador: a compaction que encontra uma
// linha vencida grava a lápide DIRETO no destino — e ela passa pelo
// applyRecord como qualquer outra.
func TestExpiracaoOportunistaDescontaDoContador(t *testing.T) {
	opts := optionsWithCounters(t.TempDir(), byOLT("aut", true))
	opts.SegmentSize = 400
	s := openStore(t, opts)
	rel := &relogioOfTest{t: time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
	s.UsaRelogio(rel.agora)
	const key = "vencida"
	rowID, _ := mustPut(t, s, key, 1, payloadServico(t, s, key, "A", true, rel.agora()), nil)
	origSec, origOff := localizacaoOfRow(t, s, rowID)
	for i := 0; s.Stats().CurrentSegmentID == origSec && i < 200; i++ {
		k := "enche-" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		mustPut(t, s, k, int64(i), payloadServico(t, s, k, "B", false, rel.agora()), nil)
	}
	exigeTotal(t, s, "aut", "A", 1, 1)
	rel.advance(2 * time.Hour)
	rec := recordIn(t, s, origSec, origOff)
	copiado, _, err := s.reapendaSeVivo(origSec, origOff, rec)
	if err != nil || !copiado {
		t.Fatalf("reapendaSeVivo: %v, %v", copiado, err)
	}
	if flagsOfLocation(t, s, rowID)&types.FlagTombstone == 0 {
		t.Fatal("a montagem tinha de expirar a linha oportunisticamente")
	}
	exigeTotal(t, s, "aut", "A", 0, 0)
}

func TestContadorSobreviveAoCheckpointEReabertura(t *testing.T) {
	dir := t.TempDir()
	s, rel := storeWithCounters(t, dir, byOLT("aut", true))
	for i := 0; i < 100; i++ {
		k := "c-" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		olt := "A"
		if i%3 == 0 {
			olt = "B"
		}
		mustPut(t, s, k, 1, payloadServico(t, s, k, olt, i%2 == 0, rel.agora()), nil)
	}
	exigeTotal(t, s, "aut", "A", 33, 66)
	exigeTotal(t, s, "aut", "B", 17, 34)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := openStore(t, optionsWithCounters(dir, byOLT("aut", true)))
	if s2.CountersConstruidosForTest() != 0 {
		t.Fatal("reabertura com os mesmos parâmetros tem de ADOTAR, não reconstruir")
	}
	exigeTotal(t, s2, "aut", "A", 33, 66)
	exigeTotal(t, s2, "aut", "B", 17, 34)
}

// TestContadorRecuperaDoTailSemCheckpoint é a prova do argumento "dobra antes
// do replay": em qualquer janela de crash, a reabertura chega aos totais de
// uma instância que nunca caiu.
func TestContadorRecuperaDoTailSemCheckpoint(t *testing.T) {
	for _, variante := range []struct {
		name    string
		parcial bool
	}{
		{"sem checkpoint depois do tail", false},
		{"checkpoint interrompido antes do manifest", true},
	} {
		t.Run(variante.name, func(t *testing.T) {
			dir := t.TempDir()
			s, rel := storeWithCounters(t, dir, byOLT("aut", true))
			keys := make([]string, 100)
			for i := range keys {
				keys[i] = "c-" + string(rune('a'+i%26)) + string(rune('a'+i/26))
				mustPut(t, s, keys[i], 1, payloadServico(t, s, keys[i], "A", true, rel.agora()), nil)
			}
			if err := s.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			// A ponta: migrações de grupo, condições que caem, e lápides.
			for i := 0; i < 50; i++ {
				prev, _, _, _ := s.GetByKey(s.presente(), []byte(keys[i]), nil)
				switch i % 3 {
				case 0:
					mustPut(t, s, keys[i], 2, payloadServico(t, s, keys[i], "B", true, rel.agora()), &prev)
				case 1:
					mustPut(t, s, keys[i], 2, payloadServico(t, s, keys[i], "A", false, rel.agora()), &prev)
				case 2:
					if _, _, err := s.Put([]byte(keys[i]), 2, types.FlagTombstone, payloadServico(t, s, keys[i], "", false, rel.agora()), &prev); err != nil {
						t.Fatal(err)
					}
				}
			}
			// Referência: os totais da instância que nunca caiu.
			totA, refsA := totalOfCounter(t, s, "aut", "A")
			totB, refsB := totalOfCounter(t, s, "aut", "B")
			// 100 em A; 17 migram para B, 17 perdem a condição, 16 viram lápide.
			if totA != 50 || refsA != 67 || totB != 17 || refsB != 17 {
				t.Fatalf("referência inesperada: A=%d/%d B=%d/%d", totA, refsA, totB, refsB)
			}
			if variante.parcial {
				if err := s.PartialCheckpointWithCountersForTest(); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s2 := openStore(t, optionsWithCounters(dir, byOLT("aut", true)))
			if s2.CountersConstruidosForTest() != 0 {
				t.Fatal("a recovery tem de chegar lá por dobra + replay, sem reconstrução")
			}
			exigeTotal(t, s2, "aut", "A", totA, refsA)
			exigeTotal(t, s2, "aut", "B", totB, refsB)
		})
	}
}

func TestContadorNovoEhConstruidoNaAbertura(t *testing.T) {
	dir := t.TempDir()
	s, rel := storeWithCounters(t, dir)
	for i := 0; i < 100; i++ {
		k := "c-" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		mustPut(t, s, k, 1, payloadServico(t, s, k, "A", i%2 == 0, rel.agora()), nil)
	}
	if len(s.Counters()) != 0 {
		t.Fatal("dataset sem contador declarado não abre contador nenhum")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := openStore(t, optionsWithCounters(dir, byOLT("aut", true)))
	if s2.CountersConstruidosForTest() != 1 {
		t.Fatalf("contador novo num dataset com linhas tem de ser construído; construídos = %d", s2.CountersConstruidosForTest())
	}
	exigeTotal(t, s2, "aut", "A", 50, 100)
}

func TestContadorComWhereDiferenteEhReconstruido(t *testing.T) {
	dir := t.TempDir()
	s, rel := storeWithCounters(t, dir, byOLT("x", true))
	for i := 0; i < 10; i++ {
		k := "c-" + string(rune('a'+i))
		mustPut(t, s, k, 1, payloadServico(t, s, k, "A", i%2 == 0, rel.agora()), nil)
	}
	exigeTotal(t, s, "x", "A", 5, 10)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Mesmo nome, sem where: outro contador.
	s2 := openStore(t, optionsWithCounters(dir, byOLT("x", false)))
	if s2.CountersConstruidosForTest() != 1 {
		t.Fatal("where diferente tem de reconstruir")
	}
	exigeTotal(t, s2, "x", "A", 10, 10)
	if _, err := os.Stat(filepath.Join(dir, "indexes", "contadores", "x", "grupos.dict.old")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("os arquivos postos de lado tinham de ser apagados no fim do Open: %v", err)
	}
	m, err := checkpoint.Load(filepath.Join(dir, "checkpoint", "manifest.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Counters) != 1 || m.Counters[0].Where != "" {
		t.Fatalf("o manifest tinha de gravar a definição nova: %+v", m.Counters)
	}
}

func TestContadorRemovidoDoYamlEhApagado(t *testing.T) {
	dir := t.TempDir()
	s, rel := storeWithCounters(t, dir, byOLT("aut", true))
	mustPut(t, s, "k", 1, payloadServico(t, s, "k", "A", true, rel.agora()), nil)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := openStore(t, optionsWithCounters(dir))
	if len(s2.Counters()) != 0 {
		t.Fatal("contador removido do YAML não pode continuar aberto")
	}
	if _, err := os.Stat(filepath.Join(dir, "indexes", "contadores", "aut")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("o diretório do contador removido tinha de sumir: %v", err)
	}
	m, err := checkpoint.Load(filepath.Join(dir, "checkpoint", "manifest.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Counters) != 0 {
		t.Fatalf("o manifest não pode continuar declarando o contador: %+v", m.Counters)
	}
}

func TestAberturaSemCallbackRecusaDatasetComContadores(t *testing.T) {
	dir := t.TempDir()
	s, rel := storeWithCounters(t, dir, byOLT("aut", true))
	mustPut(t, s, "k", 1, payloadServico(t, s, "k", "A", true, rel.agora()), nil)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	opts := optionsWithCounters(dir)
	opts.CompileCountersCallbackFunction = nil
	_, err := Open(opts)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("abrir sem as definições um dataset com contadores tem de ser recusado com ErrInvalidArgument, veio %v", err)
	}
	if err == nil || !contem(err.Error(), "aut") {
		t.Fatalf("a recusa tem de nomear o contador: %v", err)
	}
}

func TestContadorEhRecusadoNoHistorico(t *testing.T) {
	base := openStore(t, Options{})
	opts := Options{
		Dir:                             t.TempDir(),
		Mode:                            HistoryMode,
		SharedDictionary:                base.FieldDict(),
		CompileCountersCallbackFunction: callbackOfCounters(byOLT("aut", true)),
	}
	if _, err := Open(opts); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("histórico com contador tem de ser recusado com ErrInvalidArgument, veio %v", err)
	}
}

func TestMudarMaxGroupsNaoReconstroi(t *testing.T) {
	dir := t.TempDir()
	c := byOLT("aut", true)
	c.ceiling = 10
	s, rel := storeWithCounters(t, dir, c)
	mustPut(t, s, "k", 1, payloadServico(t, s, "k", "A", true, rel.agora()), nil)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	c.ceiling = 20
	s2 := openStore(t, optionsWithCounters(dir, c))
	if s2.CountersConstruidosForTest() != 0 {
		t.Fatal("mudar o teto não muda o conteúdo: não reconstrói")
	}
	if counterChamado(t, s2, "aut").Stats().Ceiling != 20 {
		t.Fatal("o teto novo tem de valer")
	}
	exigeTotal(t, s2, "aut", "A", 1, 1)
}

func TestMaxGroupsMenorQueOsGruposEmUsoEhRecusado(t *testing.T) {
	dir := t.TempDir()
	c := byOLT("aut", true)
	c.ceiling = 10
	s, rel := storeWithCounters(t, dir, c)
	for _, olt := range []string{"A", "B", "C"} {
		mustPut(t, s, "k-"+olt, 1, payloadServico(t, s, "k-"+olt, olt, true, rel.agora()), nil)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	c.ceiling = 2
	_, err := Open(optionsWithCounters(dir, c))
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("teto abaixo dos grupos em uso tem de ser recusado, veio %v", err)
	}
	if err == nil || !contem(err.Error(), "3") {
		t.Fatalf("a recusa tem de dizer quantos grupos estão em uso: %v", err)
	}
}

func contem(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
