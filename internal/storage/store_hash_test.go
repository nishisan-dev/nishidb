package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

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

// TestIndexadoEhACapabilityQueOPlannerEsperava é a razão de esta frente
// existir. O `index: hash` era aceito no YAML e o planner tinha a regra do
// segundo motor, mas nenhum Store implementava a capability que ela consultava:
// o ponteiro ficava nil e toda igualdade sobre a coluna degradava para FullScan
// em silêncio.
func TestIndexadoEhACapabilityQueOPlannerEsperava(t *testing.T) {
	s := openStore(t, Options{CamposIndexadosPorNome: []string{"id", "status", "counter"}})
	for _, campo := range []types.FieldID{fldID, fldStatus, fldCounter} {
		if !s.Indexed(campo) {
			t.Fatalf("Indexed(%d) = false num campo declarado", campo)
		}
	}
	if s.Indexed(999) {
		t.Fatal("Indexado de campo não declarado devia ser false")
	}
	if got := s.CamposIndexados(); !slices.Equal(got, []types.FieldID{fldID, fldStatus, fldCounter}) {
		t.Fatalf("CamposIndexados = %v", got)
	}
}

// TestLookupEncontraAsRowsDoValor cobre o caminho feliz ponta a ponta no
// Store, incluindo a ORDEM crescente que o executor assume.
func TestLookupEncontraAsRowsDoValor(t *testing.T) {
	s := openStore(t, Options{CamposIndexados: []types.FieldID{fldStatus}})
	var esperadas []types.RowID
	for i, estado := range []string{"Start", "Stop", "Start", "Interim", "Start"} {
		k := chaveDe(i)
		row, _ := mustPut(t, s, k, int64(i), statusPayload(t, s, k, estado, int64(i)), nil)
		if estado == "Start" {
			esperadas = append(esperadas, row)
		}
	}
	it, err := s.Lookup(fldStatus, strVal("Start"))
	if err != nil {
		t.Fatal(err)
	}
	got := collectRows(t, it)
	slices.Sort(esperadas)
	if !slices.Equal(got, esperadas) {
		t.Fatalf("Lookup(Start) = %v, esperado %v", got, esperadas)
	}
	if !slices.IsSorted(got) {
		t.Fatalf("Lookup devolveu fora de ordem: %v", got)
	}
}

// TestLookupDeCampoNaoIndexadoEhErro: o planner degrada quando a capability
// falta, então chegar aqui é bug de plan-time — e ele tem de doer.
func TestLookupDeCampoNaoIndexadoEhErro(t *testing.T) {
	s := openStore(t, Options{})
	if _, err := s.Lookup(fldStatus, strVal("Start")); !errors.Is(err, ErrInvalidArgument) {
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
	s := openStore(t, Options{CamposIndexados: []types.FieldID{fldStatus}})
	k := "CMP-V0001PPCLP-013"
	row, _ := mustPut(t, s, k, 1, statusPayload(t, s, k, "Start", 1), nil)

	prev, _, ok, err := s.GetByKey(s.presente(), []byte(k), nil)
	if err != nil || !ok {
		t.Fatalf("GetByKey: (%v,%v)", ok, err)
	}
	mustPut(t, s, k, 2, statusPayload(t, s, k, "Stop", 2), &prev)

	itNovo, err := s.Lookup(fldStatus, strVal("Stop"))
	if err != nil {
		t.Fatal(err)
	}
	if got := collectRows(t, itNovo); !slices.Equal(got, []types.RowID{row}) {
		t.Fatalf("Lookup(Stop) = %v, esperado [%d]", got, row)
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
	opts := Options{Dir: dir, CamposIndexados: []types.FieldID{fldStatus}}
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
	it, err := s2.Lookup(fldStatus, strVal("Start"))
	if err != nil {
		t.Fatal(err)
	}
	if got := collectRows(t, it); !slices.Equal(got, []types.RowID{row}) {
		t.Fatalf("posting não sobreviveu ao restart: %v", got)
	}
}

// TestReplayDoTailReconstroiAsPostings é o caso de CRASH, e é diferente do
// restart limpo acima: o que veio depois do último checkpoint só existe no tail
// dos segmentos, e a abertura seguinte tem de reconstruir as postings a partir
// dos records. Sem isso, a query depois do crash omitiria as rows escritas no
// intervalo, sem erro em camada nenhuma.
//
// O teste PAROU de regredir o índice a um backup do ponto S, e a mudança não é
// de conveniência: com o índice por segmento não existe backup a regredir — o
// .widx nunca é confiado através de um restart (spec §2.5), então TODA abertura
// já parte de um índice descartado e o reconstrói. A montagem antiga simulava um
// estado que o produto não produz mais.
func TestReplayDoTailReconstroiAsPostings(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, CamposIndexados: []types.FieldID{fldStatus}}
	s := openStore(t, opts)

	k0 := chaveDe(0)
	row0, _ := mustPut(t, s, k0, 1, statusPayload(t, s, k0, "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// Escritas depois do checkpoint: só o tail as conhece. O Close não
	// checkpointa, então a abertura seguinte replaya de verdade.
	var depois []types.RowID
	for i := 1; i <= 3; i++ {
		k := chaveDe(i)
		row, _ := mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
		depois = append(depois, row)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := openStore(t, opts)
	it, err := s2.Lookup(fldStatus, strVal("Start"))
	if err != nil {
		t.Fatal(err)
	}
	got := collectRows(t, it)
	quer := append([]types.RowID{row0}, depois...)
	slices.Sort(quer)
	exigeCandidatos(t, "postings pós-replay do tail", got, quer)
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
	opts := Options{Dir: dir, CamposIndexados: []types.FieldID{fldStatus}}
	s := openStore(t, opts)

	k := "CMP-V0001PPCLP-013"
	row, _ := mustPut(t, s, k, 1, statusPayload(t, s, k, "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	prev, _, ok, err := s.GetByKey(s.presente(), []byte(k), nil)
	if err != nil || !ok {
		t.Fatalf("GetByKey: (%v,%v)", ok, err)
	}
	mustPut(t, s, k, 2, statusPayload(t, s, k, "Stop", 2), &prev)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := openStore(t, opts)
	it, err := s2.Lookup(fldStatus, strVal("Stop"))
	if err != nil {
		t.Fatal(err)
	}
	// Asserção CRUA sobre o índice: o replay religou a posting do valor NOVO.
	// Sem ela, a asserção pós-revalidação abaixo passaria com um Lookup que
	// devolvesse zero candidatos, porque quem decide lá é o payload corrente.
	exigeCandidatos(t, "posting do valor novo pós-replay", collectRows(t, it), []types.RowID{row})
	if got := lookupRevalidado(t, s2, fldStatus, strVal("Start")); len(got) != 0 {
		t.Fatalf("pós-replay e pós-revalidação, o valor ANTIGO não pode devolver a row: %v", got)
	}
}

// rebaixaManifestParaLegado reescreve o manifest.bin no formato que um binário
// ANTERIOR gravaria: versão 1, sem cauda TLV, com hashKind preenchido e com uma
// entrada no array de bitmaps do corpo.
//
// A cirurgia é feita nos BYTES, e não chamando o encoder deste repositório, pela
// mesma razão do manifestV1Golden do pacote checkpoint: um "teste de migração"
// que gerasse o arquivo antigo pelo encoder novo provaria só que o encoder
// concorda consigo mesmo. Aqui o ponto é mais forte ainda — o encoder novo NÃO
// SABE MAIS escrever a v1, então gerar o legado por ele seria impossível sem
// ressuscitar o código que este commit apagou.
//
// O que ela monta é exatamente o que um dataset em produção tem hoje em disco, e
// é sobre esse arquivo que a promessa "não é preciso recriar dataset para subir"
// tem de valer.
func rebaixaManifestParaLegado(t *testing.T, caminho string) {
	t.Helper()
	raw, err := os.ReadFile(caminho)
	if err != nil {
		t.Fatal(err)
	}
	corpo := raw[:len(raw)-4] // fora o CRC

	// Offsets do layout do corpo (checkpoint/manifest.go): segmentCount em 52,
	// 36 bytes por segmento a partir de 56, e então primarySlotCount (8) +
	// primaryUsed (8) antes do hashKind.
	segCount := int(binary.LittleEndian.Uint32(corpo[52:]))
	offHashKind := 56 + segCount*36 + 8 + 8
	offBitmapCount := offHashKind + 1 + 8
	offKafka := offBitmapCount + 2

	novo := make([]byte, 0, len(corpo)+16)
	novo = append(novo, corpo[:offHashKind]...)
	novo = append(novo, 1) // hashKind: o valor que o binário anterior gravava
	novo = append(novo, corpo[offHashKind+1:offBitmapCount]...)
	novo = binary.LittleEndian.AppendUint16(novo, 1) // bitmapCount = 1…
	novo = binary.LittleEndian.AppendUint16(novo, uint16(fldStatus))
	novo = binary.LittleEndian.AppendUint64(novo, 96) // …e a entrada dela
	// kafkaCount, e SÓ ele: a cauda TLV do v3 fica de fora, que é o que faz o
	// arquivo ser um v1 de verdade em vez de um v3 com a versão adulterada.
	novo = append(novo, corpo[offKafka:offKafka+4]...)
	novo[4] = 1

	novo = binary.LittleEndian.AppendUint32(novo, crc32.Checksum(novo, crc32.MakeTable(crc32.Castagnoli)))
	if err := os.WriteFile(caminho, novo, 0o644); err != nil {
		t.Fatal(err)
	}
	// Pré-condição do teste, e não asserção dele: se a cirurgia não produziu um
	// manifest legado LEGÍVEL, o que vem depois não estaria medindo migração.
	if novo[4] != 1 {
		t.Fatalf("a cirurgia não rebaixou a versão: %d", novo[4])
	}
	if _, err := checkpoint.Load(caminho); err != nil {
		t.Fatalf("o manifest legado montado à mão não é legível: %v", err)
	}
}

// TestDatasetLegadoAbreEOsIndicesGlobaisSaemDoDisco é a promessa de migração no
// nível do STORE, que é onde ela importa para o operador: um dataset gravado
// pelo binário anterior — manifest v1, com a seção de bitmaps no corpo e os
// arquivos dos dois motores globais em disco — abre sem ser recriado, e o disco
// daqueles motores volta.
//
// A prova de migração existia só dentro do pacote checkpoint, sobre bytes. Aqui
// ela atravessa o Store inteiro: recovery, abertura dos índices e o primeiro
// checkpoint, que é quem reescreve o manifest em v3.
//
// O oráculo do disco é a QUEDA, e não a existência. É a mesma lição do
// TestOIndiceDeSegmentoEntraNoIndexBytes: o indexes/ já tinha bytes antes desta
// tarefa, então um "o diretório sumiu" passaria sem que byte nenhum tivesse
// voltado ao dispositivo, e um "IndexBytes > 0" passaria com a limpeza inteira
// inerte. A única mudança entre as duas medidas é a abertura.
func TestDatasetLegadoAbreEOsIndicesGlobaisSaemDoDisco(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, CamposIndexados: []types.FieldID{fldStatus}}
	s := openStore(t, opts)
	for i := 0; i < 5; i++ {
		k := chaveDe(i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// O dataset legado: os arquivos dos dois motores que este binário não sabe
	// mais criar, e o manifest na versão que ele não sabe mais gravar.
	const porMotor = 2 << 20
	idxDir := filepath.Join(dir, "indexes")
	for _, motor := range []string{"bitmap", "hash"} {
		alvo := filepath.Join(idxDir, motor, "00003")
		if err := os.MkdirAll(alvo, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(alvo, "legado.bin"), make([]byte, porMotor), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	rebaixaManifestParaLegado(t, audManifestPath(dir))

	// A MESMA função que o IndexBytes usa (retention.go), e não uma segunda
	// contagem: uma cópia da convenção divergiria da série que o operador vê.
	antes := blocosDoDiretorio(idxDir)

	s2, err := Open(opts)
	if err != nil {
		t.Fatalf("dataset gravado pelo binário anterior não abriu: %v", err)
	}
	defer s2.Close()

	// 1. A migração: o dado continua lá, e o índice responde sobre ele.
	if got := lookupRevalidado(t, s2, fldStatus, strVal("Start")); len(got) != 5 {
		t.Errorf("pós-migração a consulta indexada devolveu %v, quero as 5 linhas", got)
	}

	// 2. O disco voltou, e a medida é a diferença.
	depois := blocosDoDiretorio(idxDir)
	if queda := antes - depois; queda < 2*porMotor {
		t.Errorf("o indexes/ caiu %d bytes com %d plantados nos dois motores: a limpeza não devolveu o disco",
			queda, 2*porMotor)
	}
	// 3. E os bytes que voltaram não são mais contados por ninguém: o IndexBytes
	// de agora tem de ser menor que o que o indexes/ sozinho ocupava antes.
	if idx := s2.EstatisticasDeRetencao().IndexBytes; idx >= antes {
		t.Errorf("IndexBytes = %d contra %d que o indexes/ ocupava com o lixo: os arquivos legados continuam contados",
			idx, antes)
	}
	for _, motor := range []string{"bitmap", "hash"} {
		if _, err := os.Stat(filepath.Join(idxDir, motor)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("indexes/%s sobreviveu à abertura: %v", motor, err)
		}
	}

	// 4. E o manifest subiu para v3 — é ele que fecha a volta, e é por isso que
	// a limpeza pôde acontecer.
	raw, err := os.ReadFile(audManifestPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if raw[4] != 3 {
		t.Errorf("o manifest continuou na v%d depois da migração", raw[4])
	}
}

// TestValorAcimaDoTetoEmCampoComHashEhRecusadoAntesDoAppend: descobrir a
// violação depois do append é fatal — o record fica durável, o replay do tail
// tenta reindexá-lo a cada abertura, falha sempre, e o dataset inteiro passa a
// ser INABRÍVEL.
func TestValorAcimaDoTetoEmCampoComHashEhRecusadoAntesDoAppend(t *testing.T) {
	s := openStore(t, Options{CamposIndexados: []types.FieldID{fldStatus}})
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

// TestPostingOrfaEhSupersetEOExecutorFiltra prova o contrato do §44, e ele é a
// afirmação em que o índice por segmento inteiro se apoia: o índice devolve
// candidato a MAIS, nunca a menos, e quem decide é a revalidação contra o
// payload corrente.
//
// A posting órfã DEIXOU DE PRECISAR DE ENCENAÇÃO, e é essa a diferença entre
// este teste e o que ele substitui. Antes ela era forçada por uma falha injetada
// na fase de Clear — o caminho tolerado do §10.1 —, porque no motor global a
// posting antiga normalmente seria apagada. No índice por segmento não existe
// Clear (spec §5.2): basta um update para a posting do valor anterior ficar
// onde está. O teste passou a exercitar o caminho real, e o hook saiu junto com
// a fase que ele injetava.
func TestPostingOrfaEhSupersetEOExecutorFiltra(t *testing.T) {
	s := openStore(t, Options{CamposIndexados: []types.FieldID{fldStatus}})

	k := "CMP-V0001PPCLP-013"
	row, _ := mustPut(t, s, k, 1, statusPayload(t, s, k, "Start", 1), nil)
	prev, _, ok, err := s.GetByKey(s.presente(), []byte(k), nil)
	if err != nil || !ok {
		t.Fatalf("GetByKey: (%v,%v)", ok, err)
	}
	mustPut(t, s, k, 2, statusPayload(t, s, k, "Stop", 2), &prev)

	it, err := s.Lookup(fldStatus, strVal("Start"))
	if err != nil {
		t.Fatal(err)
	}
	if got := collectRows(t, it); !slices.Equal(got, []types.RowID{row}) {
		t.Fatalf("o update devia deixar a posting do valor antigo como órfã (superset): %v", got)
	}
	// A revalidação é do executor, e ela parte do record CORRENTE: a row
	// existe, está viva, e o valor dela já é o novo. É por isso que o
	// candidato a mais nunca vira linha a mais.
	rec, _, vivo, err := s.GetByRowID(s.presente(), row, nil)
	if err != nil || !vivo {
		t.Fatalf("GetByRowID: (%v,%v)", vivo, err)
	}
	if v, achou := valorDoCampo(rec.Payload, fldStatus); !achou || string(v.Bytes) != "Stop" {
		t.Fatalf("o estado corrente da row devia ser Stop: %q (achou=%v)", v.Bytes, achou)
	}
	if got := lookupRevalidado(t, s, fldStatus, strVal("Start")); len(got) != 0 {
		t.Fatalf("pós-revalidação, o candidato órfão não pode virar linha: %v", got)
	}
}

// TestCloseNoMeioDaVarreduraDoHashNaoDaPanic: o iterador SOBREVIVE à chamada
// que o criou, e um Close concorrente não pode virar panic — a política de
// log-and-skip da ingestão não captura panic, ela derruba o processo.
func TestCloseNoMeioDaVarreduraDoHashNaoDaPanic(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir, SegmentSize: 1 << 20, CamposIndexados: []types.FieldID{fldStatus}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		k := chaveDe(i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	it, err := s.Lookup(fldStatus, strVal("Start"))
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
		Dir:                    dir,
		TTLAtivo:               true,
		DuracaoDeFundo:         time.Hour,
		CampoDeEscrita:         "_writtenAt",
		CampoDePrazo:           "_expiresIn",
		CamposIndexadosPorNome: []string{"status"},
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
	it, err := s.Lookup(campo, v)
	if err != nil {
		t.Fatal(err)
	}
	return collectRows(t, it)
}
