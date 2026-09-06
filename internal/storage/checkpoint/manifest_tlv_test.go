package checkpoint

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// manifestV1Golden monta um manifest.bin da versão 1 SEM passar pelo encode
// deste pacote — os bytes são escritos à mão, na ordem do layout documentado.
//
// Essa independência é o ponto. Um "teste de compatibilidade" que gerasse o
// arquivo antigo chamando o encoder novo provaria apenas que o encoder
// concorda consigo mesmo: bastaria alguém mudar as duas pontas na mesma
// direção para ele continuar verde enquanto todo dataset em produção ficava
// ilegível.
func manifestV1Golden() []byte {
	b := make([]byte, 0, 128)
	b = append(b, 'N', 'D', 'B', 'M')
	b = append(b, 1, 0, 0, 0)                    // version 1 + reserved
	b = binary.LittleEndian.AppendUint64(b, 7)   // epoch
	b = binary.LittleEndian.AppendUint64(b, 100) // createdAtUnixMs
	b = binary.LittleEndian.AppendUint64(b, 42)  // nextRowID
	b = binary.LittleEndian.AppendUint64(b, 40)  // rowCount
	b = binary.LittleEndian.AppendUint32(b, 3)   // currentSegment
	b = binary.LittleEndian.AppendUint64(b, 512) // durableOffset
	b = binary.LittleEndian.AppendUint32(b, 1)   // segmentCount
	b = binary.LittleEndian.AppendUint32(b, 3)   // segmento: id
	b = binary.LittleEndian.AppendUint64(b, 4096)
	b = binary.LittleEndian.AppendUint64(b, 9)
	b = binary.LittleEndian.AppendUint64(b, 8)
	b = binary.LittleEndian.AppendUint64(b, 3000)
	b = binary.LittleEndian.AppendUint64(b, 1<<20) // primarySlotCount
	b = binary.LittleEndian.AppendUint64(b, 40)    // primaryUsed
	b = append(b, 1)                               // hashKind
	b = binary.LittleEndian.AppendUint64(b, 256)   // fieldDictValidBytes
	b = binary.LittleEndian.AppendUint16(b, 1)     // bitmapCount
	b = binary.LittleEndian.AppendUint16(b, 5)     // bitmap: fieldID
	b = binary.LittleEndian.AppendUint64(b, 96)    // bitmap: valuesDictValidBytes
	b = binary.LittleEndian.AppendUint32(b, 0)     // kafkaCount
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

// TestManifestV1ContinuaSendoLido é a garantia de MIGRAÇÃO do formato: todo
// dataset já materializado tem no disco um manifest da versão 1 ou 2, e nem a
// cauda TLV nem a v3 podem torná-lo ilegível.
//
// A direção importa, e ela é de mão única desde a v3: o binário novo LÊ o
// manifest antigo, o binário antigo NÃO lê o novo. É a segunda metade que
// bloqueia o rollback de propósito; esta primeira é o que dispensa recriar
// dataset para subir.
//
// A consequência de falhar aqui não é um teste vermelho: é todo dataset em
// produção recusando a abertura com "manifest corrompido" no primeiro deploy,
// e o operador concluindo que perdeu os dados.
func TestManifestV1ContinuaSendoLido(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.bin")
	if err := os.WriteFile(path, manifestV1Golden(), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := Load(path)
	if err != nil {
		t.Fatalf("manifest v1 escrito à mão deixou de ser lido: %v", err)
	}
	if m.Epoch != 7 || m.NextRowID != 42 || m.RowCount != 40 || m.CurrentSegment != 3 || m.DurableOffset != 512 {
		t.Fatalf("escalares do v1 mudaram de posição: %+v", *m)
	}
	if len(m.Segments) != 1 || m.Segments[0].ID != 3 || m.Segments[0].LiveBytes != 3000 {
		t.Fatalf("segmentos do v1: %+v", m.Segments)
	}
	// As entradas de bitmap do corpo legado não têm mais destino no Manifest,
	// mas continuam tendo de ser PULADAS — e é o próprio sucesso do Load que
	// prova isso. O que vem depois delas é o kafkaCount, que o decode exige
	// zero: sem o avanço, ele leria os quatro primeiros bytes da entrada de
	// bitmap deste golden e recusaria o manifest com "seção kafka inesperada".
	// Um teste que só aferisse os escalares de cima não distinguiria os dois
	// casos, porque eles são lidos ANTES.
}

// TestTodoManifestSaiNaV3 trava a decisão que torna esta migração de mão única.
//
// Não há mais condição: o dataset sem histórico, sem agenda e sem campo
// indexado grava v3 igual a qualquer outro. Um binário anterior recusa os três
// com "version desconhecida", que é a resposta certa — ele manteria os índices
// globais que conhece, ignoraria os de segmento e os devolveria INCOMPLETOS.
//
// A alternativa que existia até aqui — v1 enquanto não houvesse cauda, para
// preservar o rollback — deixou de ser possível: o índice por segmento existe em
// TODO dataset que este binário abre, inclusive no que hoje não indexa campo
// nenhum, porque é a abertura seguinte que pode declarar um.
func TestTodoManifestSaiNaV3(t *testing.T) {
	cases := map[string]func(*Manifest){
		"sem seção nenhuma": func(*Manifest) {},
		"com histórico":     func(m *Manifest) { m.Mode, m.RowIDBase = 1, 500 },
		"com agenda": func(m *Manifest) {
			m.Schedule = &ScheduleInfo{FallbackDurationMs: 60_000, BlockShift: 6, Levels: 4}
		},
	}
	for name, adjust := range cases {
		t.Run(name, func(t *testing.T) {
			m := sampleManifest()
			adjust(&m)
			if v := m.encode()[4]; v != manifestV3 {
				t.Fatalf("manifest saiu na versão %d, esperado %d", v, manifestV3)
			}
		})
	}
}

// TestASecaoDoIndicePorSegmentoEhSempreEmitida cobre a seção 0x8004 nos dois
// lados: ela sai em todo manifest e o decode recusa um formato que não conhece.
//
// A recusa é a razão de a seção existir. Ela não é o que bloqueia o binário
// ANTERIOR — a versão já faz isso —, é o que vai bloquear o binário anterior à
// PRÓXIMA mudança de formato do índice, sem custar mais um bump de versão do
// manifest.
func TestASecaoDoIndicePorSegmentoEhSempreEmitida(t *testing.T) {
	m := sampleManifest()
	raw := m.encode()
	var marker [2]byte
	binary.LittleEndian.PutUint16(marker[:], tagPerSegmentIndex)
	i := bytes.Index(raw, marker[:])
	if i < 0 {
		t.Fatal("a seção do índice por segmento não foi emitida")
	}
	if n := binary.LittleEndian.Uint32(raw[i+2:]); n != indexInfoSize {
		t.Fatalf("a seção declara %d bytes, esperado %d", n, indexInfoSize)
	}
	if f := raw[i+tlvHeaderSize]; f != currentIndexFormat {
		t.Fatalf("formato gravado %d, esperado %d", f, currentIndexFormat)
	}
	if tagPerSegmentIndex&tagCritical == 0 {
		t.Fatalf("a tag do índice é %#04x, sem o bit crítico", tagPerSegmentIndex)
	}

	// Formato desconhecido: é o que um binário mais novo gravaria, e a leitura
	// tem de RECUSAR em vez de abrir o dataset sem saber manter o índice dele.
	raw[i+tlvHeaderSize] = currentIndexFormat + 1
	recrc(raw)
	if _, err := decode(raw); !errors.Is(err, ErrCorruptManifest) {
		t.Fatalf("formato de índice desconhecido foi aceito: %v", err)
	}
}

// TestSecaoDeHashesLegadaEhDescartadaSemRecusarODataset é o outro lado da
// remoção do motor global, e o modo de falha que ele fecha é de DIAGNÓSTICO.
//
// Um dataset que declarou `index: hash` tem a seção 0x8001 no manifest. Sem um
// ramo que a reconheça, ela cairia no ramo de seção crítica desconhecida e o
// dataset seria recusado com "escrito por um binário mais novo" — a afirmação
// exatamente invertida sobre um dataset escrito por um binário mais VELHO, que
// mandaria o operador procurar o problema no lugar errado.
func TestSecaoDeHashesLegadaEhDescartadaSemRecusarODataset(t *testing.T) {
	raw := manifestV1Golden()
	body := raw[:len(raw)-4]
	body[4] = manifestV2
	payload := binary.LittleEndian.AppendUint16(nil, 1)
	payload = binary.LittleEndian.AppendUint16(payload, 4)
	payload = binary.LittleEndian.AppendUint64(payload, 1<<10)
	body = binary.LittleEndian.AppendUint16(body, tagHashes)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(payload)))
	body = append(body, payload...)
	body = binary.LittleEndian.AppendUint32(body, crc32.Checksum(body, castagnoli))

	path := filepath.Join(t.TempDir(), "manifest.bin")
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := Load(path)
	if err != nil {
		t.Fatalf("manifest legado com seção de hashes foi recusado: %v", err)
	}
	if m.Epoch != 7 {
		t.Fatalf("o corpo não sobreviveu à seção descartada: %+v", *m)
	}
}

// TestCaudaIgnoravelEhPuladaECriticaRecusa é o teste da POLÍTICA de tags, e
// prova as duas direções com o mesmo manifest.
//
// A tag ignorável existe para que informação descritiva nova não condene um
// binário antigo; a crítica existe para que informação que muda o significado
// do dataset NUNCA seja lida pela metade. Sem a segunda, um binário sem motor
// de hash abriria o dataset, o índice pararia de receber sets e clears, e a
// primeira query indexada depois disso responderia menos linhas do que existem
// — sem erro, sem log, sem métrica.
func TestCaudaIgnoravelEhPuladaECriticaRecusa(t *testing.T) {
	bodyWith := func(tag uint16) []byte {
		raw := manifestV1Golden()
		body := raw[:len(raw)-4] // fora o CRC
		body[4] = 2              // tem cauda, logo é v2
		body = binary.LittleEndian.AppendUint16(body, tag)
		body = binary.LittleEndian.AppendUint32(body, 3)
		body = append(body, 0xAA, 0xBB, 0xCC)
		return binary.LittleEndian.AppendUint32(body, crc32.Checksum(body, castagnoli))
	}
	dir := t.TempDir()

	ign := filepath.Join(dir, "ignoravel.bin")
	if err := os.WriteFile(ign, bodyWith(0x0042), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := Load(ign)
	if err != nil {
		t.Fatalf("tag ignorável desconhecida devia ser pulada, veio %v", err)
	}
	if m.Epoch != 7 {
		t.Fatalf("o corpo não sobreviveu à cauda ignorada: %+v", *m)
	}

	crit := filepath.Join(dir, "critica.bin")
	if err := os.WriteFile(crit, bodyWith(0x8042), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(crit); !errors.Is(err, ErrCorruptManifest) {
		t.Fatalf("tag crítica desconhecida tem de RECUSAR o manifest, veio %v", err)
	}
}

// TestCaudaComComprimentoAdulteradoEhRecusada fecha o caminho em que o `len`
// do TLV aponta além do arquivo. O CRC já não fecharia num arquivo corrompido
// por acidente, mas o decodificador não pode depender disso para não ler fora
// dos limites: o CRC é conferido ANTES, e um adulterador recalcula o CRC.
func TestCaudaComComprimentoAdulteradoEhRecusada(t *testing.T) {
	raw := manifestV1Golden()
	body := raw[:len(raw)-4]
	body[4] = 2
	body = binary.LittleEndian.AppendUint16(body, tagHashes)
	body = binary.LittleEndian.AppendUint32(body, 0xFFFFFF00) // muito além do fim
	body = binary.LittleEndian.AppendUint32(body, crc32.Checksum(body, castagnoli))

	path := filepath.Join(t.TempDir(), "manifest.bin")
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); !errors.Is(err, ErrCorruptManifest) {
		t.Fatalf("len TLV fora dos limites tem de ser recusado, veio %v", err)
	}
}

// TestSecaoDeHashesInconsistenteEhRecusada cobre a contagem interna da seção
// legada, que continua conferida mesmo depois de o CONTEÚDO dela ter deixado de
// ter leitor.
//
// A checagem não é zelo vazio: uma seção cujo `count` não bate com o tamanho do
// payload é sinal de manifest corrompido, e um manifest corrompido não pode
// passar por causa de uma seção que por acaso ficou sem uso. Descartar o
// conteúdo é decisão de produto; deixar de olhar para os bytes seria trocar uma
// recusa por uma abertura sobre arquivo adulterado.
func TestSecaoDeHashesInconsistenteEhRecusada(t *testing.T) {
	raw := manifestV1Golden()
	body := raw[:len(raw)-4]
	body[4] = 2
	payload := binary.LittleEndian.AppendUint16(nil, 5) // diz 5 entradas...
	payload = binary.LittleEndian.AppendUint16(payload, 4)
	payload = binary.LittleEndian.AppendUint64(payload, 1024) // ...e traz 1
	body = binary.LittleEndian.AppendUint16(body, tagHashes)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(payload)))
	body = append(body, payload...)
	body = binary.LittleEndian.AppendUint32(body, crc32.Checksum(body, castagnoli))

	path := filepath.Join(t.TempDir(), "manifest.bin")
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); !errors.Is(err, ErrCorruptManifest) {
		t.Fatalf("seção de hashes inconsistente tem de ser recusada, veio %v", err)
	}
}

// A seção da agenda de vencimentos faz o round-trip ao lado das outras.
func TestManifestComAgendaRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.bin")
	want := sampleManifest()
	want.Schedule = &ScheduleInfo{FallbackDurationMs: 2_592_000_000, BlockShift: 6, Levels: 4}
	if err := Write(path, &want); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Schedule == nil {
		t.Fatal("a seção da agenda sumiu no round-trip")
	}
	if *got.Schedule != *want.Schedule {
		t.Fatalf("agenda voltou %+v, quer %+v", *got.Schedule, *want.Schedule)
	}
	// O corpo tem de sobreviver intacto ao lado da cauda.
	if len(got.Segments) != 3 || got.Segments[2] != want.Segments[2] {
		t.Fatalf("a cauda deslocou os segmentos: %+v", got.Segments)
	}
}

// A cauda NUNCA é vazia, e a reserva do encode tem de contar cada seção que
// entra: foi a conta duplicada que deixou a capacidade para trás quando o
// layout mudou.
func TestACaudaNuncaEhVaziaEAReservaAcompanha(t *testing.T) {
	m := sampleManifest()
	if n := TLVTailSize(&m); n != tlvHeaderSize+indexInfoSize {
		t.Fatalf("cauda de %d bytes num manifest sem histórico e sem agenda; a seção do índice é incondicional", n)
	}
	withSchedule := sampleManifest()
	withSchedule.Schedule = &ScheduleInfo{FallbackDurationMs: 1, BlockShift: 6, Levels: 4}
	growth := len(withSchedule.encode()) - len(m.encode())
	if declared := TLVTailSize(&withSchedule) - TLVTailSize(&m); growth != declared {
		t.Fatalf("a seção ocupou %d bytes; TLVTailSize declara %d", growth, declared)
	}
}

// A ordem dos registros é CRESCENTE por tag, e não a ordem em que as seções
// foram inventadas: o manifest é comparado byte a byte pelos testes de
// formato, e uma ordem que dependesse do histórico do produto mudaria o
// arquivo sem que o conteúdo mudasse.
func TestCaudaSaiEmOrdemCrescenteDeTagComAgenda(t *testing.T) {
	m := sampleManifest()
	m.Mode, m.RowIDBase = 1, 500
	m.Schedule = &ScheduleInfo{FallbackDurationMs: 60_000, BlockShift: 6, Levels: 4}
	raw := m.encode()
	tlvTail := raw[len(raw)-4-TLVTailSize(&m) : len(raw)-4]

	var tags []uint16
	for pos := 0; pos+tlvHeaderSize <= len(tlvTail); {
		tag := binary.LittleEndian.Uint16(tlvTail[pos:])
		n := binary.LittleEndian.Uint32(tlvTail[pos+2:])
		tags = append(tags, tag)
		pos += tlvHeaderSize + int(n)
	}
	if len(tags) != 3 {
		t.Fatalf("a cauda tem %d registros, quer 3: %#04x", len(tags), tags)
	}
	if !slices.IsSorted(tags) {
		t.Fatalf("as tags saíram fora de ordem crescente: %#04x", tags)
	}
}

// A seção é CRÍTICA, e o modo de falha que isso impede não é índice
// incompleto — é silêncio.
//
// A agenda é derivada da duração de fundo. Um binário que a adotasse sem
// saber que ela existe manteria uma árvore construída a partir de uma duração
// que ele já não honra, e as linhas simplesmente nunca venceriam: com o CRC
// batendo, sem erro, sem log e sem métrica. Por isso a tag carrega o bit
// crítico, que obriga a recusa.
func TestSecaoDaAgendaEhCritica(t *testing.T) {
	if tagSchedule&tagCritical == 0 {
		t.Fatalf("a tag da agenda é %#04x, sem o bit crítico: um binário antigo a ignoraria em silêncio", tagSchedule)
	}
	m := sampleManifest()
	m.Schedule = &ScheduleInfo{FallbackDurationMs: 60_000, BlockShift: 6, Levels: 4}
	raw := m.encode()
	// Troca a tag por outra crítica que ninguém conhece: é o que um binário
	// mais novo produziria, e a decodificação tem de RECUSAR.
	var marker [2]byte
	binary.LittleEndian.PutUint16(marker[:], tagSchedule)
	i := bytes.Index(raw, marker[:])
	if i < 0 {
		t.Fatal("registro da agenda não encontrado")
	}
	binary.LittleEndian.PutUint16(raw[i:], 0x8FFF)
	recrc(raw)
	if _, err := decode(raw); !errors.Is(err, ErrCorruptManifest) {
		t.Fatalf("seção crítica desconhecida foi aceita: %v", err)
	}
}

// Seção da agenda com tamanho errado é corrupção, não "campo ausente": ler
// pela metade daria uma agenda com blockShift zero, que endereça tudo no
// mesmo bloco.
func TestSecaoDaAgendaTruncadaEhRecusada(t *testing.T) {
	m := sampleManifest()
	m.Schedule = &ScheduleInfo{FallbackDurationMs: 60_000, BlockShift: 6, Levels: 4}
	raw := m.encode()
	var marker [2]byte
	binary.LittleEndian.PutUint16(marker[:], tagSchedule)
	i := bytes.Index(raw, marker[:])
	if i < 0 {
		t.Fatal("registro da agenda não encontrado")
	}
	binary.LittleEndian.PutUint32(raw[i+2:], scheduleInfoSize-1)
	recrc(raw)
	if _, err := decode(raw); err == nil {
		t.Fatal("manifest com seção de agenda truncada foi aceito")
	}
}

// recrc reescreve o checksum depois de uma adulteração, para que a recusa
// venha da checagem que o teste quer exercitar e não do CRC.
func recrc(raw []byte) {
	body := raw[:len(raw)-4]
	binary.LittleEndian.PutUint32(raw[len(raw)-4:], crc32.Checksum(body, castagnoli))
}
