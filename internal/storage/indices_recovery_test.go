package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/index/primary"
	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// hashIndexado reproduz a chave de busca do índice por segmento a partir de um
// valor. Ele existe para que o oráculo dos testes seja o MESMO par
// (valuekey.Append, primary.HashKey) que a ingestão usa: um teste que montasse
// a chave por conta própria passaria a medir a sua própria cópia da convenção
// no dia em que a canônica mudasse.
func hashIndexado(t testing.TB, v types.Value) uint64 {
	t.Helper()
	k, err := valuekey.Append(nil, v)
	if err != nil {
		t.Fatalf("valuekey.Append(%#x): %v", v.Type, err)
	}
	return primary.HashKey(k)
}

// TestOWIPNaoEhAdotadoAtravesDeUmRestart. O .widx é dado derivado e nunca é
// confiado através de um restart — reconstruí-lo do próprio segmento custa uma
// leitura sequencial de no máximo 512 MiB e elimina, de uma vez, o msync do
// índice no checkpoint, a invariante de ordem contra o durableOffset e a classe
// inteira de janelas de crash do índice secundário.
//
// O descarte sozinho não basta, e é essa metade que este teste cobra: sem a
// reconstrução, o WIP do segmento corrente nasce VAZIO depois de todo restart, e
// se esse segmento rotacionar a selagem grava um .sidx IMUTÁVEL sem os records
// anteriores ao restart — falso negativo petrificado, que é a única coisa que a
// revalidação do §44 não perdoa, porque ela só sabe descartar candidato a mais.
func TestOWIPNaoEhAdotadoAtravesDeUmRestart(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir, CamposIndexadosPorNome: []string{"node"}})
	poeLinhas(t, s, 100)
	campo := campoDoDicionario(t, s, "node")
	seg := s.curSegID.Load()
	h := hashIndexado(t, types.Value{Type: types.TypeString, Bytes: []byte("n1")})
	// O oráculo é o que a ingestão INCREMENTAL produziu, e não um número
	// escrito à mão: é exatamente a igualdade entre os dois caminhos que a
	// reconstrução promete.
	antes := s.wips[campo].Lookup(h)
	if len(antes) == 0 {
		t.Fatal("pré-condição: a ingestão não indexou nada, o teste não mede a reconstrução")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Corrompe o .widx de propósito: se ele fosse adotado, o Lookup mentiria.
	caminho := segindex.CaminhoWIP(filepath.Join(dir, "data"), seg, campo)
	if err := os.WriteFile(caminho, bytes.Repeat([]byte{0xAA}, 4096), 0o644); err != nil {
		t.Fatal(err)
	}

	s2 := openStore(t, Options{Dir: dir, CamposIndexadosPorNome: []string{"node"}})
	depois := s2.wips[campo].Lookup(h)
	if len(depois) == 0 {
		t.Fatal("o índice do segmento corrente não foi reconstruído na abertura")
	}
	if !slices.Equal(antes, depois) {
		t.Errorf("Lookup depois do restart = %v, quero %v: a reconstrução tem de dar o mesmo conjunto que a ingestão",
			depois, antes)
	}
}

// TestConstruirDoSegmentoDaOMesmoArquivoQueSelarOWIP é o golden que torna a
// reconstrução confiável. Os dois caminhos alimentam o MESMO Escritor, então a
// igualdade é por construção — o que este teste prova é que a ALIMENTAÇÃO é
// equivalente, que é o risco de verdade: um caminho que pulasse tombstones,
// lesse o payload anterior ou consultasse as locations para decidir liveness
// produziria um índice silenciosamente diferente.
func TestConstruirDoSegmentoDaOMesmoArquivoQueSelarOWIP(t *testing.T) {
	dir := t.TempDir()
	campos := []string{"node", "type"}
	s, rel := storeIndexadoComExpiracao(t, dir, campos)
	alvo := s.curSegID.Load()
	poeLinhasVariadasAteRotacionar(t, s, rel, alvo)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	caminho := segindex.CaminhoSelado(filepath.Join(dir, "data"), alvo)
	porSelagem, err := os.ReadFile(caminho)
	if err != nil {
		t.Fatalf("o segmento %d rotacionou sem selar o índice: %v", alvo, err)
	}
	// Sem esta guarda o golden passaria comparando dois arquivos triviais. O
	// secCount vem do offset 6 do header (format.go), lido cru de propósito:
	// derivá-lo do pacote faria o teste medir o lugar errado no dia em que o
	// layout mudasse, e continuar verde.
	if sec := binary.LittleEndian.Uint16(porSelagem[6:8]); sec != 2 {
		t.Fatalf("o .sidx selado declara %d seções em %d bytes; quero as duas (node e type)",
			sec, len(porSelagem))
	}
	if err := os.Remove(caminho); err != nil {
		t.Fatal(err)
	}

	s2, _ := storeIndexadoComExpiracao(t, dir, campos)
	if err := s2.ConstroiIndiceDoSegmento(t.Context(), alvo, s2.CamposIndexados()); err != nil {
		t.Fatalf("ConstroiIndiceDoSegmento(%d): %v", alvo, err)
	}
	porReconstrucao, err := os.ReadFile(caminho)
	if err != nil {
		t.Fatalf("a reconstrução não gravou o índice do segmento %d: %v", alvo, err)
	}
	if !bytes.Equal(porSelagem, porReconstrucao) {
		t.Errorf("selagem e reconstrução produziram arquivos diferentes: %d bytes contra %d",
			len(porSelagem), len(porReconstrucao))
	}
}

// TestAReconstrucaoNaoConsultaAsLocations trava o contrato do §44 no lugar em
// que ele é fácil de quebrar por bom senso: o índice é SUPERCONJUNTO, e uma
// reconstrução que filtrasse por liveness introduziria uma segunda definição de
// "vivo" — a dela — que divergiria da do executor. Uma row tombstonada continua
// tendo posting no segmento em que os records dela estão.
func TestAReconstrucaoNaoConsultaAsLocations(t *testing.T) {
	dir := t.TempDir()
	s, rel := storeIndexadoComExpiracao(t, dir, []string{"node"})
	seg := s.curSegID.Load()
	campo := campoDoDicionario(t, s, "node")
	h := hashIndexado(t, types.Value{Type: types.TypeString, Bytes: []byte("morta")})

	id, _ := mustPut(t, s, "A", 1, payloadIndexadoComPrazo(t, s, "A", "morta", 1, rel.t), nil)
	rel.avanca(2 * time.Hour)
	expirou, err := s.Expira(id)
	if err != nil || !expirou {
		t.Fatalf("Expira(%d) = %v, %v; a pré-condição do teste é uma lápide no segmento", id, expirou, err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, _ := storeIndexadoComExpiracao(t, dir, []string{"node"})
	if got := s2.wips[campo].Lookup(h); !slices.Equal(got, []types.RowID{id}) {
		t.Errorf("Lookup do valor da row tombstonada = %v, quero [%d]: a reconstrução decidiu liveness", got, id)
	}
	if seg != s2.curSegID.Load() {
		t.Fatalf("o segmento corrente mudou de %d para %d; o teste mediu outra coisa", seg, s2.curSegID.Load())
	}
}

// TestOsWIPsQueNaoSaoDoCorrenteSaemNaAberturaESoQuemTemDadoEntraNaFila cobre os
// dois casos da §2.5 que o segmento corrente não cobre, e a assimetria entre
// eles é o ponto: o descarte é o MESMO ato para os dois — remoção por nome, sem
// abrir o arquivo, porque um .widx rasgado por crash é justamente o que não se
// deixa abrir —, mas só o segmento que ainda tem .dat tem o que reconstruir.
// Enfileirar um segmento sem dado seria mandar a reconstrução varrer um arquivo
// que não existe, para sempre.
func TestOsWIPsQueNaoSaoDoCorrenteSaemNaAberturaESoQuemTemDadoEntraNaFila(t *testing.T) {
	campos := []string{"node"}
	s, dir := abreStoreIndexadoComSegmentoPequeno(t, campos, 64<<10)
	alvo := s.curSegID.Load()
	campo := campoDoDicionario(t, s, "node")
	poeLinhasAteRotacionar(t, s, alvo)
	corrente := s.curSegID.Load()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	dados := filepath.Join(dir, "data")
	lixo := bytes.Repeat([]byte{0xAA}, 4096)
	// Caso 2: segmento não corrente, com .widx e sem .sidx. É o estado que um
	// crash entre a rotação e a selagem deixa.
	if err := os.Remove(segindex.CaminhoSelado(dados, alvo)); err != nil {
		t.Fatal(err)
	}
	comDado := segindex.CaminhoWIP(dados, alvo, campo)
	if err := os.WriteFile(comDado, lixo, 0o644); err != nil {
		t.Fatal(err)
	}
	// Caso 3: .widx de um segmento que já não tem .dat. Sem esta regra ele
	// ficaria em disco para sempre, e o IndexBytes o contaria como disco que
	// nada explica.
	semDado := segindex.CaminhoWIP(dados, 9999, campo)
	if err := os.WriteFile(semDado, lixo, 0o644); err != nil {
		t.Fatal(err)
	}

	s2 := openStore(t, Options{
		Dir: dir, SegmentSize: 64 << 10,
		CamposIndexadosPorNome: campos,
	})
	for _, caminho := range []string{comDado, semDado} {
		if _, err := os.Stat(caminho); !os.IsNotExist(err) {
			t.Errorf("Stat(%s) = %v, quero IsNotExist: o .widx de um segmento que não é o corrente tem de sair na abertura",
				filepath.Base(caminho), err)
		}
	}
	pendentes, err := s2.SegmentosPendentesDeReconstrucao(JanelaDeIndice{})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(pendentes, []uint32{alvo}) {
		t.Errorf("SegmentosPendentesDeReconstrucao = %v, quero [%d]: o corrente (%d) tem WIP e o 9999 não tem dado",
			pendentes, alvo, corrente)
	}
}

// TestOWIPOrfaoDeUmSegmentoJaSeladoSaiSemMexerNoSelado cobre o QUARTO estado da
// §2.5, que é o meio-termo que o aposAppend admite explicitamente: crash entre o
// rename do .sidx e o unlink dos .widx deixa um segmento não corrente COM as duas
// formas do índice ao mesmo tempo.
//
// As duas metades importam. O .widx tem de sair, porque é lixo que ninguém mais
// lê. E o .sidx tem de sobreviver BYTE A BYTE: ele é imutável e já é a verdade
// daquele segmento, então reescrevê-lo seria trabalho por nada — e um Sela
// reinvocado nesse estado o ENCOLHERIA, porque o Escritor novo só enxergaria os
// campos cujo .widx sobreviveu. O segmento também não pode entrar na fila: ele
// não está pendente de coisa nenhuma.
func TestOWIPOrfaoDeUmSegmentoJaSeladoSaiSemMexerNoSelado(t *testing.T) {
	campos := []string{"node"}
	s, dir := abreStoreIndexadoComSegmentoPequeno(t, campos, 64<<10)
	alvo := s.curSegID.Load()
	campo := campoDoDicionario(t, s, "node")
	poeLinhasAteRotacionar(t, s, alvo)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	dados := filepath.Join(dir, "data")
	selado := segindex.CaminhoSelado(dados, alvo)
	antes, err := os.ReadFile(selado)
	if err != nil {
		t.Fatalf("pré-condição: o segmento %d deveria estar selado: %v", alvo, err)
	}
	orfao := segindex.CaminhoWIP(dados, alvo, campo)
	if err := os.WriteFile(orfao, bytes.Repeat([]byte{0xAA}, 4096), 0o644); err != nil {
		t.Fatal(err)
	}

	s2 := openStore(t, Options{
		Dir: dir, SegmentSize: 64 << 10,
		CamposIndexadosPorNome: campos,
	})
	if _, err := os.Stat(orfao); !os.IsNotExist(err) {
		t.Errorf("Stat(%s) = %v, quero IsNotExist: o .widx do segmento já selado tem de sair na abertura",
			filepath.Base(orfao), err)
	}
	depois, err := os.ReadFile(selado)
	if err != nil {
		t.Fatalf("a abertura removeu o .sidx de um segmento selado: %v", err)
	}
	if !bytes.Equal(antes, depois) {
		t.Errorf("o .sidx do segmento %d mudou de %d para %d bytes: a abertura o reescreveu",
			alvo, len(antes), len(depois))
	}
	pendentes, err := s2.SegmentosPendentesDeReconstrucao(JanelaDeIndice{})
	if err != nil {
		t.Fatal(err)
	}
	if len(pendentes) != 0 {
		t.Errorf("SegmentosPendentesDeReconstrucao = %v, quero vazio: o segmento %d já tem .sidx", pendentes, alvo)
	}
}

// TestConstruirOIndiceDoSegmentoCorrenteEhRecusado trava a guarda que fecha o
// falso negativo petrificado: o .sidx é IMUTÁVEL, e gravá-lo para um segmento que
// ainda recebe records congelaria um índice sem tudo que vier depois — e o §44 só
// sabe descartar candidato a mais.
func TestConstruirOIndiceDoSegmentoCorrenteEhRecusado(t *testing.T) {
	s, dir := abreStoreIndexadoComSegmentoPequeno(t, []string{"node"}, 1<<20)
	poeLinhas(t, s, 10)
	corrente := s.curSegID.Load()

	err := s.ConstroiIndiceDoSegmento(t.Context(), corrente, s.CamposIndexados())
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("ConstroiIndiceDoSegmento(corrente) = %v, quero ErrInvalidArgument", err)
	}
	caminho := segindex.CaminhoSelado(filepath.Join(dir, "data"), corrente)
	if _, err := os.Stat(caminho); !os.IsNotExist(err) {
		t.Errorf("Stat(%s) = %v, quero IsNotExist: a recusa não pode ter gravado nada",
			filepath.Base(caminho), err)
	}
}

// A recusa de SUBCONJUNTO de campos vivia aqui, e ela saiu com o agendamento por
// (segmento, campo) do débito 22: ela existia porque a pendência era decidida por
// "tem .sidx?", e um .sidx parcial
// faria o segmento contar como PRONTO com campos que ninguém indexou. Com o
// teste de pendência olhando seção por campo, pedir "reconstrua o campo que
// falta" passou a ser pergunta legítima — e o que impede o arquivo de ENCOLHER
// deixou de ser uma recusa e passou a ser a união da cobertura, provada por
// TestReconstruirSoOsCamposQueFaltamPreservaOsQueJaEstavam. A metade da recusa
// que continua verdadeira — campo fora do conjunto declarado — é
// TestConstruirComCampoNaoIndexadoEhRecusado. As duas moram em
// indices_backfill_test.go.

// TestNomeIlegivelNoEspacoDoWIPSaiComoDatasetIlegivel: a varredura da §2.5
// percorre o diretório INTEIRO, então recusar um nome ilegível deixou de custar
// "a selagem perde um campo" e passou a custar "o dataset não abre". A recusa é a
// doutrina; o que este teste trava é a CLASSIFICAÇÃO. Sem ela o erro cai no
// default do codigoDoErro e sai como exit 1, "erro de uso" — o único código que
// manda o operador olhar a linha de comando em vez do dataset, que aqui está
// certa.
func TestNomeIlegivelNoEspacoDoWIPSaiComoDatasetIlegivel(t *testing.T) {
	campos := []string{"node"}
	s, dir := abreStoreIndexadoComSegmentoPequeno(t, campos, 1<<20)
	poeLinhas(t, s, 5)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	intruso := filepath.Join(dir, "data", "segment-7-00003.widx")
	if err := os.WriteFile(intruso, []byte("nada"), 0o644); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(Options{
		Dir: dir, SegmentSize: 1 << 20,
		CamposIndexadosPorNome: campos,
	})
	if err == nil {
		_ = s2.Close()
		t.Fatal("a abertura aceitou um arquivo no espaço de nomes .widx que este binário não escreveu")
	}
	if !errors.Is(err, ErrDatasetCorrupt) {
		t.Errorf("Open = %v, quero ErrDatasetCorrupt (exit 3); sem a sentinela isto vira exit 1", err)
	}
	if !errors.Is(err, segindex.ErrFormato) {
		t.Errorf("Open = %v: a causa original tem de continuar alcançável por errors.Is", err)
	}
}

// storeIndexadoComExpiracao abre um dataset materializado com índice por
// segmento, segmento pequeno e expiração declarada — é a expiração que dá ao
// golden um TOMBSTONE de verdade, e sem ela o teste não distinguiria uma
// reconstrução que pula lápide de uma que não pula.
func storeIndexadoComExpiracao(t *testing.T, dir string, campos []string) (*Store, *relogioDeTeste) {
	t.Helper()
	s := openStore(t, Options{
		Dir:                    dir,
		SegmentSize:            64 << 10,
		CamposIndexadosPorNome: campos,
		TTLAtivo:               true,
		DuracaoDeFundo:         time.Hour,
		CampoDeEscrita:         "_writtenAt",
		CampoDePrazo:           "_expiresIn",
	})
	rel := &relogioDeTeste{t: time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)}
	s.UsaRelogio(rel.agora)
	return s, rel
}

// payloadIndexadoComPrazo monta uma linha com o campo de sistema da expiração.
// comNode=false produz o terceiro caso que o golden precisa: um record em que o
// campo indexado está AUSENTE, e que portanto não gera posting nenhuma para ele.
func payloadIndexadoComPrazo(t *testing.T, s *Store, key, node string, tipo int64, escrita time.Time) []byte {
	t.Helper()
	return payloadOrdenado(t, s, map[string]types.Value{
		"id":         {Type: types.TypeString, Bytes: []byte(key)},
		"node":       {Type: types.TypeString, Bytes: []byte(node)},
		"type":       {Type: types.TypeInt64, I64: tipo},
		"_writtenAt": {Type: types.TypeTimestamp, I64: escrita.UnixMilli()},
	})
}

func payloadSemNode(t *testing.T, s *Store, key string, tipo int64, escrita time.Time) []byte {
	t.Helper()
	return payloadOrdenado(t, s, map[string]types.Value{
		"id":         {Type: types.TypeString, Bytes: []byte(key)},
		"type":       {Type: types.TypeInt64, I64: tipo},
		"_writtenAt": {Type: types.TypeTimestamp, I64: escrita.UnixMilli()},
	})
}

// poeLinhasVariadasAteRotacionar enche o segmento com as três formas de record
// que separam uma reconstrução correta de uma plausível: o create, o UPDATE (que
// deixa no segmento tanto o payload novo quanto o anterior — uma reconstrução
// que lesse o estado corrente em vez do record perderia o antigo), o record com
// o campo indexado AUSENTE e a LÁPIDE.
func poeLinhasVariadasAteRotacionar(t *testing.T, s *Store, rel *relogioDeTeste, seg uint32) {
	t.Helper()
	// O sentinela é um bool, e não o rowID: o primeiro rowID do dataset é ZERO,
	// então "expirada == 0" significaria as duas coisas ao mesmo tempo.
	lapidou := false
	for i := range 100000 {
		key := "v" + strconv.Itoa(i)
		// Uma em cada sete linhas sai SEM o campo indexado: é o record que não
		// contribui posting nenhuma, e um caminho que tratasse "campo ausente"
		// como "valor vazio" o denunciaria aqui.
		var payload []byte
		if i%7 == 3 {
			payload = payloadSemNode(t, s, key, int64(i%3), rel.t)
		} else {
			payload = payloadIndexadoComPrazo(t, s, key, "n"+strconv.Itoa(i%4), int64(i%3), rel.t)
		}
		mustPut(t, s, key, 1, payload, nil)
		// Update sobre uma key já escrita: o record anterior continua no
		// segmento, e as postings dele também.
		if i%5 == 0 && i > 0 {
			anterior := "v" + strconv.Itoa(i-1)
			rec, _, pres, err := s.Estado([]byte(anterior), nil)
			if err != nil {
				t.Fatalf("Estado(%s): %v", anterior, err)
			}
			if pres == types.PresencaViva {
				if _, _, err := s.Put([]byte(anterior), 2, 0,
					payloadIndexadoComPrazo(t, s, anterior, "atualizado", int64(i%3), rel.t), &rec); err != nil {
					t.Fatalf("Put(update %s): %v", anterior, err)
				}
			}
		}
		// Uma lápide, e uma só: o que ela cobre é o RAMO, não o volume.
		if i == 40 && !lapidou {
			id, _ := mustPut(t, s, "lapidada", 1,
				payloadIndexadoComPrazo(t, s, "lapidada", "vaimorrer", 9, rel.t), nil)
			rel.avanca(2 * time.Hour)
			expirou, err := s.Expira(id)
			if err != nil || !expirou {
				t.Fatalf("Expira(%d) = %v, %v", id, expirou, err)
			}
			lapidou = true
		}
		if s.curSegID.Load() != seg {
			if !lapidou {
				t.Fatal("o segmento rotacionou antes da lápide; o golden não cobriria o tombstone")
			}
			return
		}
	}
	t.Fatalf("o segmento %d não rotacionou em 100000 linhas", seg)
}
