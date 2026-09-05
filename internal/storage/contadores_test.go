package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/contador"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/storage/fielddict"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// avaliadorPorID é o `where authenticated = true` sem binder: o Store só
// conhece a interface, e o teste de integração não precisa de um schema
// compilado para exercitar o caminho inteiro.
type avaliadorPorID struct {
	campo types.FieldID
	fr    encoding.FieldReader
}

func (a *avaliadorPorID) Avalia(payload []byte) (bool, error) {
	a.fr.Reset(payload)
	for {
		id, v, ok := a.fr.Next()
		if !ok {
			return false, a.fr.Err()
		}
		if id == a.campo {
			return v.Type == types.TypeBool && v.I64 == 1, nil
		}
	}
}

// contadorDeTeste descreve um contador para o callback de abertura montar.
type contadorDeTeste struct {
	nome     string
	groupBy  []string
	where    bool // "authenticated = true"
	teto     uint32
	retencao time.Duration
}

// callbackDeContadores é o CompilaContadores dos testes: resolve os nomes pelo
// dicionário que o Open entrega — como o compilador de verdade faz — e devolve
// as definições.
func callbackDeContadores(cs ...contadorDeTeste) func(*fielddict.Dict) ([]contador.Definicao, error) {
	return func(dict *fielddict.Dict) ([]contador.Definicao, error) {
		out := make([]contador.Definicao, 0, len(cs))
		for _, c := range cs {
			def := contador.Definicao{Nome: c.nome, MaxGroups: c.teto, RetainEmptyGroups: c.retencao}
			if def.MaxGroups == 0 {
				def.MaxGroups = 100
			}
			for _, col := range c.groupBy {
				id, err := dict.LookupOrCreate([]byte(col))
				if err != nil {
					return nil, err
				}
				def.Colunas = append(def.Colunas, id)
				def.Nomes = append(def.Nomes, col)
				tipo := types.TypeString
				if col == "slot" || col == "port" {
					tipo = types.TypeInt64
				}
				def.Tipos = append(def.Tipos, tipo)
			}
			if c.where {
				auth, err := dict.LookupOrCreate([]byte("authenticated"))
				if err != nil {
					return nil, err
				}
				def.Where = "authenticated = true"
				def.NovoAvaliador = func() contador.Avaliador { return &avaliadorPorID{campo: auth} }
			}
			out = append(out, def)
		}
		return out, nil
	}
}

func porOLT(nome string, where bool) contadorDeTeste {
	return contadorDeTeste{nome: nome, groupBy: []string{"olt"}, where: where}
}

// payloadServico é o estado de um serviço como o materializer o produziria:
// com o instante de escrita quando o dataset tem expiração.
func payloadServico(t *testing.T, s *Store, key, olt string, auth bool, escrita time.Time) []byte {
	t.Helper()
	campos := map[string]types.Value{
		"id":            {Type: types.TypeString, Bytes: []byte(key)},
		"authenticated": {Type: types.TypeBool},
	}
	if auth {
		campos["authenticated"] = types.Value{Type: types.TypeBool, I64: 1}
	}
	if olt != "" {
		campos["olt"] = types.Value{Type: types.TypeString, Bytes: []byte(olt)}
	}
	if !escrita.IsZero() {
		campos["_writtenAt"] = types.Value{Type: types.TypeTimestamp, I64: escrita.UnixMilli()}
	}
	return payloadOrdenado(t, s, campos)
}

func contadorChamado(t *testing.T, s *Store, nome string) *contador.Contador {
	t.Helper()
	for _, c := range s.Contadores() {
		if c.Def().Nome == nome {
			return c
		}
	}
	t.Fatalf("contador %q não está aberto", nome)
	return nil
}

// totalDoContador devolve total e refs da OLT no contador nomeado.
func totalDoContador(t *testing.T, s *Store, nome, olt string) (total, refs int64) {
	t.Helper()
	c := contadorChamado(t, s, nome)
	id, ok := c.PorTexto([]byte(olt))
	if !ok {
		return 0, 0
	}
	_, _, total, refs, _ = c.Grupo(id)
	return total, refs
}

func exigeTotal(t *testing.T, s *Store, nome, olt string, total, refs int64) {
	t.Helper()
	if gt, gr := totalDoContador(t, s, nome, olt); gt != total || gr != refs {
		t.Fatalf("%s[%s]: total %d refs %d, quer %d e %d", nome, olt, gt, gr, total, refs)
	}
}

func opcoesComContadores(dir string, cs ...contadorDeTeste) Options {
	return Options{
		Dir:               dir,
		SegmentSize:       1 << 20,
		TTLAtivo:          true,
		DuracaoDeFundo:    time.Hour,
		CampoDeEscrita:    "_writtenAt",
		CampoDePrazo:      "_expiresIn",
		CompilaContadores: callbackDeContadores(cs...),
	}
}

func storeComContadores(t *testing.T, dir string, cs ...contadorDeTeste) (*Store, *relogioDeTeste) {
	t.Helper()
	s := openStore(t, opcoesComContadores(dir, cs...))
	rel := &relogioDeTeste{t: time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
	s.UsaRelogio(rel.agora)
	return s, rel
}

func TestContadorAcompanhaPutLapideECopyForward(t *testing.T) {
	s, rel := storeComContadores(t, t.TempDir(), porOLT("aut", true), porOLT("serv", false))
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
	rel.avanca(2 * time.Hour)
	if expirou, err := s.Expira(rowID); err != nil || !expirou {
		t.Fatalf("Expira: %v, %v", expirou, err)
	}
	exigeTotal(t, s, "aut", "A", 0, 0)
	exigeTotal(t, s, "serv", "A", 0, 0)
	c := contadorChamado(t, s, "aut")
	if id, ok := c.PorTexto([]byte("A")); !ok {
		t.Fatal("o grupo que esvaziou continua vivo")
	} else if _, _, _, _, vivo := c.Grupo(id); !vivo {
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
	opts := opcoesComContadores(t.TempDir(), porOLT("aut", true))
	opts.SegmentSize = 400
	s := openStore(t, opts)
	rel := &relogioDeTeste{t: time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
	s.UsaRelogio(rel.agora)
	const key = "cf-alvo"
	rowID, _ := mustPut(t, s, key, 1, payloadServico(t, s, key, "A", true, rel.agora()), nil)
	origSeg, origOff := localizacaoDaRow(t, s, rowID)
	for i := 0; s.Stats().CurrentSegmentID == origSeg && i < 200; i++ {
		k := "enche-" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		mustPut(t, s, k, int64(i), payloadServico(t, s, k, "B", false, rel.agora()), nil)
	}
	exigeTotal(t, s, "aut", "A", 1, 1)
	rec := recordEm(t, s, origSeg, origOff)
	copiado, _, err := s.reapendaSeVivo(origSeg, origOff, rec)
	if err != nil || !copiado {
		t.Fatalf("reapendaSeVivo: %v, %v", copiado, err)
	}
	exigeTotal(t, s, "aut", "A", 1, 1)
	if got := contadorChamado(t, s, "aut").Transicoes(); got == 0 {
		t.Fatal("as escritas anteriores tinham de ter transicionado")
	}
}

// TestExpiracaoOportunistaDescontaDoContador: a compaction que encontra uma
// linha vencida grava a lápide DIRETO no destino — e ela passa pelo
// applyRecord como qualquer outra.
func TestExpiracaoOportunistaDescontaDoContador(t *testing.T) {
	opts := opcoesComContadores(t.TempDir(), porOLT("aut", true))
	opts.SegmentSize = 400
	s := openStore(t, opts)
	rel := &relogioDeTeste{t: time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
	s.UsaRelogio(rel.agora)
	const key = "vencida"
	rowID, _ := mustPut(t, s, key, 1, payloadServico(t, s, key, "A", true, rel.agora()), nil)
	origSeg, origOff := localizacaoDaRow(t, s, rowID)
	for i := 0; s.Stats().CurrentSegmentID == origSeg && i < 200; i++ {
		k := "enche-" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		mustPut(t, s, k, int64(i), payloadServico(t, s, k, "B", false, rel.agora()), nil)
	}
	exigeTotal(t, s, "aut", "A", 1, 1)
	rel.avanca(2 * time.Hour)
	rec := recordEm(t, s, origSeg, origOff)
	copiado, _, err := s.reapendaSeVivo(origSeg, origOff, rec)
	if err != nil || !copiado {
		t.Fatalf("reapendaSeVivo: %v, %v", copiado, err)
	}
	if flagsDaLocation(t, s, rowID)&types.FlagTombstone == 0 {
		t.Fatal("a montagem tinha de expirar a linha oportunisticamente")
	}
	exigeTotal(t, s, "aut", "A", 0, 0)
}

func TestContadorSobreviveAoCheckpointEReabertura(t *testing.T) {
	dir := t.TempDir()
	s, rel := storeComContadores(t, dir, porOLT("aut", true))
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
	s2 := openStore(t, opcoesComContadores(dir, porOLT("aut", true)))
	if s2.ContadoresConstruidosForTest() != 0 {
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
		nome    string
		parcial bool
	}{
		{"sem checkpoint depois do tail", false},
		{"checkpoint interrompido antes do manifest", true},
	} {
		t.Run(variante.nome, func(t *testing.T) {
			dir := t.TempDir()
			s, rel := storeComContadores(t, dir, porOLT("aut", true))
			chaves := make([]string, 100)
			for i := range chaves {
				chaves[i] = "c-" + string(rune('a'+i%26)) + string(rune('a'+i/26))
				mustPut(t, s, chaves[i], 1, payloadServico(t, s, chaves[i], "A", true, rel.agora()), nil)
			}
			if err := s.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			// A ponta: migrações de grupo, condições que caem, e lápides.
			for i := 0; i < 50; i++ {
				prev, _, _, _ := s.GetByKey(s.presente(), []byte(chaves[i]), nil)
				switch i % 3 {
				case 0:
					mustPut(t, s, chaves[i], 2, payloadServico(t, s, chaves[i], "B", true, rel.agora()), &prev)
				case 1:
					mustPut(t, s, chaves[i], 2, payloadServico(t, s, chaves[i], "A", false, rel.agora()), &prev)
				case 2:
					if _, _, err := s.Put([]byte(chaves[i]), 2, types.FlagTombstone, payloadServico(t, s, chaves[i], "", false, rel.agora()), &prev); err != nil {
						t.Fatal(err)
					}
				}
			}
			// Referência: os totais da instância que nunca caiu.
			totA, refsA := totalDoContador(t, s, "aut", "A")
			totB, refsB := totalDoContador(t, s, "aut", "B")
			// 100 em A; 17 migram para B, 17 perdem a condição, 16 viram lápide.
			if totA != 50 || refsA != 67 || totB != 17 || refsB != 17 {
				t.Fatalf("referência inesperada: A=%d/%d B=%d/%d", totA, refsA, totB, refsB)
			}
			if variante.parcial {
				if err := s.PartialCheckpointComContadoresForTest(); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s2 := openStore(t, opcoesComContadores(dir, porOLT("aut", true)))
			if s2.ContadoresConstruidosForTest() != 0 {
				t.Fatal("a recovery tem de chegar lá por dobra + replay, sem reconstrução")
			}
			exigeTotal(t, s2, "aut", "A", totA, refsA)
			exigeTotal(t, s2, "aut", "B", totB, refsB)
		})
	}
}

func TestContadorNovoEhConstruidoNaAbertura(t *testing.T) {
	dir := t.TempDir()
	s, rel := storeComContadores(t, dir)
	for i := 0; i < 100; i++ {
		k := "c-" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		mustPut(t, s, k, 1, payloadServico(t, s, k, "A", i%2 == 0, rel.agora()), nil)
	}
	if len(s.Contadores()) != 0 {
		t.Fatal("dataset sem contador declarado não abre contador nenhum")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := openStore(t, opcoesComContadores(dir, porOLT("aut", true)))
	if s2.ContadoresConstruidosForTest() != 1 {
		t.Fatalf("contador novo num dataset com linhas tem de ser construído; construídos = %d", s2.ContadoresConstruidosForTest())
	}
	exigeTotal(t, s2, "aut", "A", 50, 100)
}

func TestContadorComWhereDiferenteEhReconstruido(t *testing.T) {
	dir := t.TempDir()
	s, rel := storeComContadores(t, dir, porOLT("x", true))
	for i := 0; i < 10; i++ {
		k := "c-" + string(rune('a'+i))
		mustPut(t, s, k, 1, payloadServico(t, s, k, "A", i%2 == 0, rel.agora()), nil)
	}
	exigeTotal(t, s, "x", "A", 5, 10)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Mesmo nome, sem where: outro contador.
	s2 := openStore(t, opcoesComContadores(dir, porOLT("x", false)))
	if s2.ContadoresConstruidosForTest() != 1 {
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
	if len(m.Contadores) != 1 || m.Contadores[0].Where != "" {
		t.Fatalf("o manifest tinha de gravar a definição nova: %+v", m.Contadores)
	}
}

func TestContadorRemovidoDoYamlEhApagado(t *testing.T) {
	dir := t.TempDir()
	s, rel := storeComContadores(t, dir, porOLT("aut", true))
	mustPut(t, s, "k", 1, payloadServico(t, s, "k", "A", true, rel.agora()), nil)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := openStore(t, opcoesComContadores(dir))
	if len(s2.Contadores()) != 0 {
		t.Fatal("contador removido do YAML não pode continuar aberto")
	}
	if _, err := os.Stat(filepath.Join(dir, "indexes", "contadores", "aut")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("o diretório do contador removido tinha de sumir: %v", err)
	}
	m, err := checkpoint.Load(filepath.Join(dir, "checkpoint", "manifest.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Contadores) != 0 {
		t.Fatalf("o manifest não pode continuar declarando o contador: %+v", m.Contadores)
	}
}

func TestAberturaSemCallbackRecusaDatasetComContadores(t *testing.T) {
	dir := t.TempDir()
	s, rel := storeComContadores(t, dir, porOLT("aut", true))
	mustPut(t, s, "k", 1, payloadServico(t, s, "k", "A", true, rel.agora()), nil)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	opts := opcoesComContadores(dir)
	opts.CompilaContadores = nil
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
		Dir:               t.TempDir(),
		Modo:              ModoHistorico,
		DictCompartilhado: base.FieldDict(),
		CompilaContadores: callbackDeContadores(porOLT("aut", true)),
	}
	if _, err := Open(opts); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("histórico com contador tem de ser recusado com ErrInvalidArgument, veio %v", err)
	}
}

func TestMudarMaxGroupsNaoReconstroi(t *testing.T) {
	dir := t.TempDir()
	c := porOLT("aut", true)
	c.teto = 10
	s, rel := storeComContadores(t, dir, c)
	mustPut(t, s, "k", 1, payloadServico(t, s, "k", "A", true, rel.agora()), nil)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	c.teto = 20
	s2 := openStore(t, opcoesComContadores(dir, c))
	if s2.ContadoresConstruidosForTest() != 0 {
		t.Fatal("mudar o teto não muda o conteúdo: não reconstrói")
	}
	if contadorChamado(t, s2, "aut").Estatisticas().Teto != 20 {
		t.Fatal("o teto novo tem de valer")
	}
	exigeTotal(t, s2, "aut", "A", 1, 1)
}

func TestMaxGroupsMenorQueOsGruposEmUsoEhRecusado(t *testing.T) {
	dir := t.TempDir()
	c := porOLT("aut", true)
	c.teto = 10
	s, rel := storeComContadores(t, dir, c)
	for _, olt := range []string{"A", "B", "C"} {
		mustPut(t, s, "k-"+olt, 1, payloadServico(t, s, "k-"+olt, olt, true, rel.agora()), nil)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	c.teto = 2
	_, err := Open(opcoesComContadores(dir, c))
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
