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

	"github.com/nishisan-dev/nishidb/internal/types"
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

// TestManifestV1ContinuaSendoLido é a garantia de rollback do formato: todo
// dataset já materializado tem no disco um manifest da versão 1, e a cauda TLV
// não pode torná-lo ilegível.
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
	if len(m.Bitmaps) != 1 || m.Bitmaps[0].FieldID != 5 || m.Bitmaps[0].ValuesDictValidBytes != 96 {
		t.Fatalf("bitmaps do v1: %+v", m.Bitmaps)
	}
	if len(m.Hashes) != 0 {
		t.Fatalf("manifest v1 não declara hash nenhum, veio %+v", m.Hashes)
	}
}

// TestManifestSemCaudaContinuaSaindoV1 é a outra metade do rollback: enquanto
// nenhum dataset declarar índice hash, o binário novo grava EXATAMENTE o que o
// antigo gravava. Sem isto, subir a versão e voltar atrás deixaria de ser uma
// operação reversível.
func TestManifestSemCaudaContinuaSaindoV1(t *testing.T) {
	m := sampleManifest()
	b := m.encode()
	if b[4] != 1 {
		t.Fatalf("manifest sem cauda saiu na versão %d; o rollback exige v1", b[4])
	}
	// E o corpo é byte a byte o mesmo do golden montado à mão, para o caso em
	// que os dois manifests coincidem.
	esperado := manifestV1Golden()
	equivalente := Manifest{
		Epoch: 7, CreatedAtUnixMs: 100, NextRowID: 42, RowCount: 40,
		CurrentSegment: 3, DurableOffset: 512,
		Segments: []SegmentInfo{
			{ID: 3, SizeBytes: 4096, TotalRecords: 9, LiveRecords: 8, LiveBytes: 3000},
		},
		PrimarySlotCount: 1 << 20, PrimaryUsed: 40, HashKind: 1,
		FieldDictValidBytes: 256,
		Bitmaps:             []BitmapInfo{{FieldID: 5, ValuesDictValidBytes: 96}},
	}
	got := equivalente.encode()
	if !bytes.Equal(got, esperado) {
		t.Fatalf("o encode divergiu do layout v1 escrito à mão:\nobtido   %x\nesperado %x", got, esperado)
	}
}

// TestManifestComHashSobeParaV2 trava a única condição que muda o byte de
// versão. Emitir v2 sem cauda mataria o rollback sem entregar informação; ficar
// em v1 COM cauda faria um binário antigo ler o corpo, ignorar a seção crítica
// e abrir o dataset sem manter o índice hash — que é o falso negativo
// silencioso que a política de tags existe para impedir.
func TestManifestComHashSobeParaV2(t *testing.T) {
	m := sampleManifest()
	m.Hashes = []HashInfo{{FieldID: 4, SlotCount: 1 << 10}}
	b := m.encode()
	if b[4] != 2 {
		t.Fatalf("manifest com cauda saiu na versão %d, esperado 2", b[4])
	}
	semCaudaM := sampleManifest()
	semCauda := semCaudaM.encode()
	if len(b)-len(semCauda) != TamanhoDaCaudaTLV(&m) {
		t.Fatalf("a cauda ocupou %d bytes; TamanhoDaCaudaTLV declara %d",
			len(b)-len(semCauda), TamanhoDaCaudaTLV(&m))
	}
}

// TestManifestComHashRoundTrip cobre o caminho completo em disco.
func TestManifestComHashRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.bin")
	want := sampleManifest()
	want.Hashes = []HashInfo{
		{FieldID: 4, SlotCount: 1 << 10},
		{FieldID: 9, SlotCount: 1 << 14},
	}
	if err := Write(path, &want); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Hashes) != 2 || got.Hashes[0] != want.Hashes[0] || got.Hashes[1] != want.Hashes[1] {
		t.Fatalf("hashes: %+v", got.Hashes)
	}
	// O corpo v1 tem de sobreviver intacto ao lado da cauda.
	if len(got.Bitmaps) != 2 || got.Bitmaps[0] != want.Bitmaps[0] {
		t.Fatalf("a cauda deslocou os bitmaps: %+v", got.Bitmaps)
	}
	if len(got.Segments) != 3 || got.Segments[2] != want.Segments[2] {
		t.Fatalf("a cauda deslocou os segmentos: %+v", got.Segments)
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
	corpo := func(tag uint16) []byte {
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
	if err := os.WriteFile(ign, corpo(0x0042), 0o644); err != nil {
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
	if err := os.WriteFile(crit, corpo(0x8042), 0o644); err != nil {
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

// TestSecaoDeHashesInconsistenteEhRecusada cobre a contagem interna da seção:
// um `count` que não bate com o tamanho do payload não pode virar uma lista
// truncada em silêncio, porque essa lista é o CONJUNTO contra o qual a
// abertura decide se um índice foi abandonado.
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

// TestHashInfoUsaFieldIDDeVerdade evita a regressão preguiçosa de gravar o
// FieldID como u8 ou de truncá-lo: o dicionário do dataset chega a IDs bem
// acima de 255, e um truncamento faria a seção nomear o campo ERRADO.
func TestHashInfoUsaFieldIDDeVerdade(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.bin")
	m := sampleManifest()
	m.Hashes = []HashInfo{{FieldID: types.FieldID(4242), SlotCount: 1 << 20}}
	if err := Write(path, &m); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Hashes) != 1 || got.Hashes[0].FieldID != 4242 {
		t.Fatalf("FieldID não sobreviveu: %+v", got.Hashes)
	}
}

// A seção da agenda de vencimentos faz o round-trip e leva o manifest à v2.
func TestManifestComAgendaRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.bin")
	want := sampleManifest()
	want.Agenda = &AgendaInfo{DuracaoDeFundoMs: 2_592_000_000, BlockShift: 6, Niveis: 4}
	if want.encode()[4] != manifestV2 {
		t.Fatal("manifest com agenda não subiu para a v2")
	}
	if err := Write(path, &want); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Agenda == nil {
		t.Fatal("a seção da agenda sumiu no round-trip")
	}
	if *got.Agenda != *want.Agenda {
		t.Fatalf("agenda voltou %+v, quer %+v", *got.Agenda, *want.Agenda)
	}
	// O corpo v1 tem de sobreviver intacto ao lado da cauda.
	if len(got.Bitmaps) != 2 || got.Bitmaps[0] != want.Bitmaps[0] {
		t.Fatalf("a cauda deslocou os bitmaps: %+v", got.Bitmaps)
	}
	if len(got.Segments) != 3 || got.Segments[2] != want.Segments[2] {
		t.Fatalf("a cauda deslocou os segmentos: %+v", got.Segments)
	}
}

// Sem agenda declarada o manifest continua na v1, byte a byte igual ao que o
// binário anterior gravava: o rollback segue possível para quem nunca ligou a
// expiração.
func TestManifestSemAgendaContinuaV1(t *testing.T) {
	m := sampleManifest()
	if m.encode()[4] != manifestV1 {
		t.Fatal("manifest sem agenda não saiu na v1")
	}
	if TamanhoDaCaudaTLV(&m) != 0 {
		t.Fatalf("cauda de %d bytes num manifest sem seção nenhuma", TamanhoDaCaudaTLV(&m))
	}
	// E a reserva do encode tem de contar a seção quando ela existe: foi a
	// conta duplicada que deixou a capacidade para trás quando o layout mudou.
	comAgenda := sampleManifest()
	comAgenda.Agenda = &AgendaInfo{DuracaoDeFundoMs: 1, BlockShift: 6, Niveis: 4}
	if crescimento := len(comAgenda.encode()) - len(m.encode()); crescimento != TamanhoDaCaudaTLV(&comAgenda) {
		t.Fatalf("a seção ocupou %d bytes; TamanhoDaCaudaTLV declara %d",
			crescimento, TamanhoDaCaudaTLV(&comAgenda))
	}
}

// A ordem dos registros é CRESCENTE por tag, e não a ordem em que as seções
// foram inventadas: o manifest é comparado byte a byte pelos testes de
// formato, e uma ordem que dependesse do histórico do produto mudaria o
// arquivo sem que o conteúdo mudasse.
func TestCaudaSaiEmOrdemCrescenteDeTagComAgenda(t *testing.T) {
	m := sampleManifest()
	m.Hashes = []HashInfo{{FieldID: 7, SlotCount: 1024}}
	m.Modo, m.RowIDBase = 1, 500
	m.Agenda = &AgendaInfo{DuracaoDeFundoMs: 60_000, BlockShift: 6, Niveis: 4}
	raw := m.encode()
	cauda := raw[len(raw)-4-TamanhoDaCaudaTLV(&m) : len(raw)-4]

	var tags []uint16
	for pos := 0; pos+tlvHeaderSize <= len(cauda); {
		tag := binary.LittleEndian.Uint16(cauda[pos:])
		n := binary.LittleEndian.Uint32(cauda[pos+2:])
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
	if tagAgenda&tagCritica == 0 {
		t.Fatalf("a tag da agenda é %#04x, sem o bit crítico: um binário antigo a ignoraria em silêncio", tagAgenda)
	}
	m := sampleManifest()
	m.Agenda = &AgendaInfo{DuracaoDeFundoMs: 60_000, BlockShift: 6, Niveis: 4}
	raw := m.encode()
	// Troca a tag por outra crítica que ninguém conhece: é o que um binário
	// mais novo produziria, e a decodificação tem de RECUSAR.
	var marca [2]byte
	binary.LittleEndian.PutUint16(marca[:], tagAgenda)
	i := bytes.Index(raw, marca[:])
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
	m.Agenda = &AgendaInfo{DuracaoDeFundoMs: 60_000, BlockShift: 6, Niveis: 4}
	raw := m.encode()
	var marca [2]byte
	binary.LittleEndian.PutUint16(marca[:], tagAgenda)
	i := bytes.Index(raw, marca[:])
	if i < 0 {
		t.Fatal("registro da agenda não encontrado")
	}
	binary.LittleEndian.PutUint32(raw[i+2:], agendaInfoSize-1)
	recrc(raw)
	if _, err := decode(raw); err == nil {
		t.Fatal("manifest com seção de agenda truncada foi aceito")
	}
}

// recrc reescreve o checksum depois de uma adulteração, para que a recusa
// venha da checagem que o teste quer exercitar e não do CRC.
func recrc(raw []byte) {
	corpo := raw[:len(raw)-4]
	binary.LittleEndian.PutUint32(raw[len(raw)-4:], crc32.Checksum(corpo, castagnoli))
}
