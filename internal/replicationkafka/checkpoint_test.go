package replicationkafka

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/atomicfile"
)

// goldenCheckpointHex é o .ckpt de sampleCheckpoint gerado por um oráculo
// INDEPENDENTE do código (script Python com CRC32C bit a bit). Leitura:
//
//	4e44424b                  magic "NDBK"
//	01 01 0000                versão 1, flags = snapshotting, reservado
//	8813000000000000          Cursor = 5000
//	0000100000000000          CursorPos = 1048576
//	0200000000000000          ResyncConsumed = 2
//	6810000000000000          TargetCursor = 4200
//	03000000                  Partitions = 3
//	c0eeb843865bd718          LastSnapshotEnd = 1790000009123000000 ns
//	0102...20                 RenderHash (32 bytes)
//	0410000000000000          RulerSequence = 4100
//	1000 "aaaa0000bbbb1111"   Epoch
//	1000 "cccc2222dddd3333"   TargetEpoch
//	0a00 "events.cdc"         Topic
//	1000 "eeee4444ffff5555"   RulerEpoch
//	0200                      dois brokers, canonizados (ordenados, sem vazio nem repetição)
//	0c00 "kafka-a:9092"
//	0c00 "kafka-b:9092"
//	0f62d11d                  crc32c de tudo acima
//
// A versão com a régua saiu do mesmo tipo de oráculo: struct.pack e CRC32C
// bit a bit, com o CRC conferido contra o vetor "123456789" → e3069283.
const goldenCheckpointHex = "4e44424b01010000881300000000000000001000000000000200000000000000681000000000000003000000c0eeb843865bd7180102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f2004100000000000001000616161613030303062626262313131311000636363633232323264646464333333330a006576656e74732e63646310006565656534343434666666663535353502000c006b61666b612d613a393039320c006b61666b612d623a393039320f62d11d"

// goldenResyncHex é o .resync com contador 3, do mesmo oráculo:
//
//	4e444252    magic "NDBR"
//	01 000000   versão 1, reservado
//	0300000000000000   contador = 3
//	fec846d9    crc32c de tudo acima
const goldenResyncHex = "4e444252010000000300000000000000fec846d9"

func sampleCheckpoint() checkpointFile {
	var hash [32]byte
	for i := range hash {
		hash[i] = byte(i + 1)
	}
	return checkpointFile{
		Epoch:        "aaaa0000bbbb1111",
		Cursor:       5000,
		CursorPos:    1 << 20,
		TargetEpoch:  "cccc2222dddd3333",
		TargetCursor: 4200,
		Partitions:   3,
		Topic:        "events.cdc",
		// Fora da forma canônica de propósito: com espaço, repetido, vazio e
		// fora de ordem. O golden prova que o arquivo guarda a forma canônica.
		Bootstrap:       []string{" kafka-b:9092", "kafka-a:9092", "kafka-b:9092", ""},
		RenderHash:      hash,
		Snapshotting:    true,
		ResyncConsumed:  2,
		LastSnapshotEnd: time.Unix(1790000009, 123000000).UTC(),
		RulerEpoch:      "eeee4444ffff5555",
		RulerSequence:   4100,
	}
}

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("golden inválido: %v", err)
	}
	return b
}

// checkpointWithCRC refaz o CRC de um corpo mutado: fabrica a incoerência
// estrutural que o CRC, sozinho, não enxerga.
func checkpointWithCRC(body []byte) []byte {
	return binary.LittleEndian.AppendUint32(append([]byte(nil), body...), crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli)))
}

func TestCheckpointGoldenBytes(t *testing.T) {
	want := mustDecodeHex(t, goldenCheckpointHex)
	got, err := encodeCheckpoint(sampleCheckpoint())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("checkpoint divergiu do golden:\n got %x\nwant %x", got, want)
	}

	// O arquivo em disco é exatamente o golden, e o caminho de leitura devolve
	// o conteúdo canônico. O diretório replication/kafka não existe ainda:
	// storeCheckpoint o cria.
	path := filepath.Join(t.TempDir(), "replication", "kafka", "dest.ckpt")
	if err := storeCheckpoint(path, sampleCheckpoint()); err != nil {
		t.Fatal(err)
	}
	onDisk, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(onDisk, want) {
		t.Fatalf("arquivo .ckpt divergiu do golden:\n got %x\nwant %x", onDisk, want)
	}
	back, ok, err := loadCheckpoint(path)
	if err != nil || !ok {
		t.Fatalf("loadCheckpoint = ok %v, err %v", ok, err)
	}
	wantBack := sampleCheckpoint()
	wantBack.Bootstrap = []string{"kafka-a:9092", "kafka-b:9092"}
	if back.Epoch != wantBack.Epoch || back.TargetEpoch != wantBack.TargetEpoch || back.Cursor != 5000 || back.TargetCursor != 4200 ||
		back.CursorPos != 1<<20 || back.Partitions != 3 || back.Topic != "events.cdc" ||
		!slices.Equal(back.Bootstrap, wantBack.Bootstrap) || back.RenderHash != wantBack.RenderHash ||
		!back.Snapshotting || back.ResyncConsumed != 2 || !back.LastSnapshotEnd.Equal(wantBack.LastSnapshotEnd) ||
		back.RulerEpoch != "eeee4444ffff5555" || back.RulerSequence != 4100 {
		t.Fatalf("round trip = %+v", back)
	}
}

func TestCheckpointRoundTripsZeroValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dest.ckpt")
	if err := storeCheckpoint(path, checkpointFile{}); err != nil {
		t.Fatal(err)
	}
	back, ok, err := loadCheckpoint(path)
	if err != nil || !ok {
		t.Fatalf("loadCheckpoint = ok %v, err %v", ok, err)
	}
	if back.Snapshotting || back.Epoch != "" || back.Topic != "" || len(back.Bootstrap) != 0 ||
		!back.LastSnapshotEnd.IsZero() || back.RenderHash != [32]byte{} {
		t.Fatalf("round trip do zero = %+v", back)
	}
}

func TestCheckpointMissingIsNotAnError(t *testing.T) {
	c, ok, err := loadCheckpoint(filepath.Join(t.TempDir(), "absent.ckpt"))
	if err != nil || ok {
		t.Fatalf("arquivo ausente: ok = %v, err = %v; esperado false e nil", ok, err)
	}
	if c.Cursor != 0 || c.Snapshotting || c.Epoch != "" {
		t.Fatalf("checkpoint de arquivo ausente deveria ser zero, veio %+v", c)
	}
	if _, err := readResync(filepath.Join(t.TempDir(), "absent.resync")); err != nil {
		t.Fatalf("contador ausente deveria valer zero sem erro: %v", err)
	}
}

func TestCheckpointRejectsBadCRC(t *testing.T) {
	good := mustDecodeHex(t, goldenCheckpointHex)
	// Um byte trocado em CADA posição do arquivo, o do próprio CRC inclusive.
	for i := range good {
		bad := append([]byte(nil), good...)
		bad[i] ^= 0x01
		path := filepath.Join(t.TempDir(), "dest.ckpt")
		if err := os.WriteFile(path, bad, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := loadCheckpoint(path); !errors.Is(err, errCorruptCheckpoint) || ok {
			t.Fatalf("byte %d trocado: ok = %v, err = %v; esperado errCorruptCheckpoint", i, ok, err)
		}
	}
	// Truncado em qualquer ponto também é recusado.
	for n := 0; n < len(good); n++ {
		if _, err := decodeCheckpoint(good[:n]); !errors.Is(err, errCorruptCheckpoint) {
			t.Fatalf("truncado em %d bytes: err = %v; esperado errCorruptCheckpoint", n, err)
		}
	}
}

// O CRC sozinho não enxerga o corpo incoerente com o CRC refeito: versão
// desconhecida, bit de flag que não existe, reservado sujo, contagem de
// brokers maior que o arquivo e sobra depois do último campo.
func TestCheckpointRejectsStructuralNonsenseWithValidCRC(t *testing.T) {
	good := mustDecodeHex(t, goldenCheckpointHex)
	body := good[:len(good)-4]
	// O deslocamento do bootstrapCount: 92 + (2+16) + (2+16) + (2+10) + (2+16).
	const countAt = 92 + 18 + 18 + 12 + 18
	// O comprimento da RulerEpoch: 92 + (2+16) + (2+16) + (2+10).
	const rulerEpochAt = 92 + 18 + 18 + 12
	if binary.LittleEndian.Uint16(body[countAt:]) != 2 {
		t.Fatalf("o deslocamento do bootstrapCount mudou; o teste está desalinhado")
	}
	cases := map[string]func(b []byte) []byte{
		"version 2":        func(b []byte) []byte { b[4] = 2; return b },
		"unknown flag bit": func(b []byte) []byte { b[5] |= 0x02; return b },
		"reserved byte 6":  func(b []byte) []byte { b[6] = 1; return b },
		"reserved byte 7":  func(b []byte) []byte { b[7] = 1; return b },
		"negative partitions": func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[40:], 0xffffffff)
			return b
		},
		"count past end": func(b []byte) []byte {
			binary.LittleEndian.PutUint16(b[countAt:], 0xffff)
			return b
		},
		"count short of the data": func(b []byte) []byte {
			binary.LittleEndian.PutUint16(b[countAt:], 1)
			return b
		},
		"trailing byte": func(b []byte) []byte { return append(b, 0) },
		"epoch length past end": func(b []byte) []byte {
			binary.LittleEndian.PutUint16(b[92:], 0xffff)
			return b
		},
		// A régua sem geração: tira os 16 bytes da RulerEpoch e zera o
		// comprimento dela, deixando RulerSequence = 4100.
		"ruler sequence without ruler epoch": func(b []byte) []byte {
			binary.LittleEndian.PutUint16(b[rulerEpochAt:], 0)
			return append(b[:rulerEpochAt+2], b[rulerEpochAt+2+16:]...)
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			bad := checkpointWithCRC(mutate(append([]byte(nil), body...)))
			if _, err := decodeCheckpoint(bad); !errors.Is(err, errCorruptCheckpoint) {
				t.Errorf("err = %v; esperado errCorruptCheckpoint", err)
			}
		})
	}
	// Partitions zero e o maior positivo continuam válidos: só o negativo é
	// recusado.
	for _, ok := range []uint32{0, 1, 0x7fffffff} {
		b := append([]byte(nil), body...)
		binary.LittleEndian.PutUint32(b[40:], ok)
		if _, err := decodeCheckpoint(checkpointWithCRC(b)); err != nil {
			t.Errorf("Partitions %d deveria ser aceito: %v", ok, err)
		}
	}
	if _, err := decodeCheckpoint(checkpointWithCRC(body)); err != nil {
		t.Fatalf("o corpo íntegro com CRC refeito deveria decodificar: %v", err)
	}
}

// A mudança de ordem ou de espaço nos bootstrapServers não pode parecer troca
// de cluster: o arquivo guarda a forma canônica, e a configuração vira a mesma
// forma antes de comparar.
func TestCheckpointStoresCanonicalBootstrap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dest.ckpt")
	if err := storeCheckpoint(path, checkpointFile{Bootstrap: []string{"b:9092", " a:9092 ", "b:9092"}}); err != nil {
		t.Fatal(err)
	}
	back, _, err := loadCheckpoint(path)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a:9092", "b:9092"}; !slices.Equal(back.Bootstrap, want) {
		t.Errorf("Bootstrap = %q, quer %q", back.Bootstrap, want)
	}
	fromConfig, err := canonicalBootstrapList([]string{"a:9092", "b:9092 "})
	if err != nil || !slices.Equal(back.Bootstrap, fromConfig) {
		t.Errorf("a configuração em outra ordem deveria canonizar igual ao arquivo: %q, %q (%v)", back.Bootstrap, fromConfig, err)
	}
	if got, err := canonicalBootstrapList([]string{" ", ""}); err != nil || got != nil {
		t.Errorf("sem broker nenhum, quer nil; veio %q (%v)", got, err)
	}
}

// Um broker com vírgula é recusado, e não dividido em dois: o arquivo precisa
// guardar o que a configuração disse, e a vírgula é o separador da forma
// canônica. Nada é gravado.
func TestCheckpointRefusesBootstrapWithComma(t *testing.T) {
	if _, err := canonicalBootstrapList([]string{"a:9092,b:9092"}); !errors.Is(err, errBootstrapComma) {
		t.Errorf("canonicalBootstrapList err = %v, quer errBootstrapComma", err)
	}
	path := filepath.Join(t.TempDir(), "dest.ckpt")
	err := storeCheckpoint(path, checkpointFile{Bootstrap: []string{"ok:9092", "a:9092,b:9092"}})
	if !errors.Is(err, errBootstrapComma) {
		t.Errorf("storeCheckpoint err = %v, quer errBootstrapComma", err)
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("nada devia ter sido gravado: %v", statErr)
	}
}

func TestCheckpointRefusesNegativePartitionsOnStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dest.ckpt")
	if err := storeCheckpoint(path, checkpointFile{Partitions: -1}); err == nil {
		t.Error("Partitions negativo deveria ser recusado na gravação")
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("nada devia ter sido gravado: %v", statErr)
	}
}

// A régua sem geração não é gravada: ela não diz o que mede.
func TestCheckpointRefusesRulerSequenceWithoutEpochOnStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dest.ckpt")
	if err := storeCheckpoint(path, checkpointFile{RulerSequence: 7}); err == nil {
		t.Fatal("a régua sem geração foi gravada")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("o .ckpt recusado existe no disco: %v", err)
	}
}

func TestCheckpointPathsLayout(t *testing.T) {
	ckpt, resync := checkpointPaths("/data/ds", "dest-a")
	if ckpt != "/data/ds/replication/kafka/dest-a.ckpt" || resync != "/data/ds/replication/kafka/dest-a.resync" {
		t.Errorf("caminhos = %q, %q", ckpt, resync)
	}
}

// A queda no meio da gravação deixa o checkpoint anterior inteiro: é o que
// impede o publicador de acordar com um ponto confirmado pela metade.
func TestCheckpointSurvivesCrashDuringStore(t *testing.T) {
	old := sampleCheckpoint()
	next := sampleCheckpoint()
	next.Cursor = 9999
	next.Snapshotting = false
	for _, crashAfter := range []string{atomicfile.StepWrite, atomicfile.StepFsync} {
		t.Run("crash after "+crashAfter, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "dest.ckpt")
			if err := storeCheckpoint(path, old); err != nil {
				t.Fatal(err)
			}
			crash := errors.New("simulated crash")
			err := storeCheckpointHook(path, next, func(step string) error {
				if step == crashAfter {
					return crash
				}
				return nil
			})
			if !errors.Is(err, crash) {
				t.Fatalf("err = %v, quer a queda simulada", err)
			}
			back, ok, err := loadCheckpoint(path)
			if err != nil || !ok || back.Cursor != 5000 || !back.Snapshotting {
				t.Fatalf("o checkpoint antigo deveria sobreviver inteiro: %+v ok=%v err=%v", back, ok, err)
			}
		})
	}
	t.Run("crash after rename", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "dest.ckpt")
		if err := storeCheckpoint(path, old); err != nil {
			t.Fatal(err)
		}
		crash := errors.New("simulated crash")
		err := storeCheckpointHook(path, next, func(step string) error {
			if step == atomicfile.StepRename {
				return crash
			}
			return nil
		})
		if !errors.Is(err, crash) {
			t.Fatalf("err = %v, quer a queda simulada", err)
		}
		back, ok, err := loadCheckpoint(path)
		if err != nil || !ok || back.Cursor != 9999 || back.Snapshotting {
			t.Fatalf("depois do rename o checkpoint novo deveria estar inteiro: %+v ok=%v err=%v", back, ok, err)
		}
	})
}

func TestResyncCounterGoldenBytes(t *testing.T) {
	want := mustDecodeHex(t, goldenResyncHex)
	if got := encodeResync(3); !bytes.Equal(got, want) {
		t.Fatalf(".resync divergiu do golden:\n got %x\nwant %x", got, want)
	}

	// Três pedidos seguidos deixam em disco exatamente o golden do contador 3.
	path := filepath.Join(t.TempDir(), "replication", "kafka", "dest.resync")
	for i := uint64(1); i <= 3; i++ {
		n, err := requestResync(path)
		if err != nil || n != i {
			t.Fatalf("pedido %d: valor %d, err %v", i, n, err)
		}
	}
	onDisk, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(onDisk, want) {
		t.Fatalf("arquivo .resync divergiu do golden:\n got %x\nwant %x", onDisk, want)
	}
}

func TestResyncRejectsBadCRC(t *testing.T) {
	good := mustDecodeHex(t, goldenResyncHex)
	for i := range good {
		bad := append([]byte(nil), good...)
		bad[i] ^= 0x01
		path := filepath.Join(t.TempDir(), "dest.resync")
		if err := os.WriteFile(path, bad, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, _, err := pendingResync(path, 0); !errors.Is(err, errCorruptCheckpoint) {
			t.Fatalf("byte %d trocado: pendingResync err = %v; esperado errCorruptCheckpoint", i, err)
		}
		// Pedir em cima de um contador ilegível também falha: sobrescrevê-lo
		// com 1 poderia apagar um pedido pendente.
		if _, err := requestResync(path); !errors.Is(err, errCorruptCheckpoint) {
			t.Fatalf("byte %d trocado: requestResync err = %v; esperado errCorruptCheckpoint", i, err)
		}
	}
	for n := 0; n < len(good); n++ {
		if _, err := decodeResync(good[:n]); !errors.Is(err, errCorruptCheckpoint) {
			t.Fatalf("truncado em %d bytes: err = %v", n, err)
		}
	}
	for name, mutate := range map[string]func(b []byte){
		"version 2":       func(b []byte) { b[4] = 2 },
		"reserved byte 5": func(b []byte) { b[5] = 1 },
		"reserved byte 6": func(b []byte) { b[6] = 1 },
		"reserved byte 7": func(b []byte) { b[7] = 1 },
	} {
		bad := append([]byte(nil), good[:len(good)-4]...)
		mutate(bad)
		if _, err := decodeResync(checkpointWithCRC(bad)); !errors.Is(err, errCorruptCheckpoint) {
			t.Errorf("%s com CRC refeito: err = %v", name, err)
		}
	}
}

// O pedido 1 é consumido no início do snapshot, o pedido 2 chega DURANTE ele,
// e ao final o pedido 2 continua pendente. É a razão de o marcador ser um
// contador que nunca é apagado.
func TestResyncRequestDuringSnapshotIsNotLost(t *testing.T) {
	dir := t.TempDir()
	ckptPath, resyncPath := checkpointPaths(dir, "dest")

	// Sem pedido nenhum, nada pendente.
	if n, pending, err := pendingResync(resyncPath, 0); err != nil || pending || n != 0 {
		t.Fatalf("sem pedido: n=%d pending=%v err=%v", n, pending, err)
	}

	first, err := requestResync(resyncPath)
	if err != nil || first != 1 {
		t.Fatalf("pedido 1: %d, %v", first, err)
	}
	n, pending, err := pendingResync(resyncPath, 0)
	if err != nil || !pending || n != 1 {
		t.Fatalf("pedido 1 deveria estar pendente: n=%d pending=%v err=%v", n, pending, err)
	}

	// O laço consome o pedido 1 no INÍCIO do snapshot: grava o valor que
	// atendeu junto com Snapshotting.
	c := checkpointFile{Epoch: "old", TargetEpoch: "new", Snapshotting: true, ResyncConsumed: n}
	if err := storeCheckpoint(ckptPath, c); err != nil {
		t.Fatal(err)
	}
	if _, pending, err := pendingResync(resyncPath, c.ResyncConsumed); err != nil || pending {
		t.Fatalf("o pedido 1 já foi atendido, não pode estar pendente: pending=%v err=%v", pending, err)
	}

	// O pedido 2 chega com o snapshot em andamento.
	second, err := requestResync(resyncPath)
	if err != nil || second != 2 {
		t.Fatalf("pedido 2: %d, %v", second, err)
	}

	// O snapshot termina: o checkpoint passa a confirmado, MAS o
	// ResyncConsumed continua o do início.
	c.Epoch, c.TargetEpoch, c.Snapshotting = "new", "", false
	c.LastSnapshotEnd = time.Unix(1790000000, 0).UTC()
	if err := storeCheckpoint(ckptPath, c); err != nil {
		t.Fatal(err)
	}

	// Uma reabertura (leitura do disco) ainda enxerga o pedido 2 pendente.
	back, ok, err := loadCheckpoint(ckptPath)
	if err != nil || !ok {
		t.Fatalf("loadCheckpoint: ok=%v err=%v", ok, err)
	}
	n, pending, err = pendingResync(resyncPath, back.ResyncConsumed)
	if err != nil || !pending || n != 2 {
		t.Fatalf("o pedido 2 foi perdido: n=%d pending=%v err=%v", n, pending, err)
	}

	// Atendido o 2, nada mais pendente, e o arquivo continua existindo.
	if _, pending, err := pendingResync(resyncPath, n); err != nil || pending {
		t.Fatalf("depois de atender o pedido 2: pending=%v err=%v", pending, err)
	}
	if _, err := os.Stat(resyncPath); err != nil {
		t.Fatalf("o .resync nunca é apagado: %v", err)
	}
}

// Dois pedidos concorrentes não leem o mesmo contador: cada um recebe um
// valor distinto e o final é o total de pedidos.
func TestResyncConcurrentRequestsGetDistinctValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dest.resync")
	const requests = 24
	values := make(chan uint64, requests)
	var wg sync.WaitGroup
	for range requests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := requestResync(path)
			if err != nil {
				t.Errorf("requestResync: %v", err)
				return
			}
			values <- n
		}()
	}
	wg.Wait()
	close(values)
	seen := map[uint64]bool{}
	for n := range values {
		if seen[n] {
			t.Errorf("o valor %d foi devolvido duas vezes", n)
		}
		seen[n] = true
	}
	if final, err := readResync(path); err != nil || final != requests {
		t.Errorf("contador final = %d (%v), quer %d", final, err, requests)
	}
}

// A queda no meio do pedido deixa o contador anterior inteiro.
func TestResyncSurvivesCrashDuringRequest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dest.resync")
	if _, err := requestResync(path); err != nil {
		t.Fatal(err)
	}
	crash := errors.New("simulated crash")
	if _, err := requestResyncHook(path, func(step string) error {
		if step == atomicfile.StepFsync {
			return crash
		}
		return nil
	}); !errors.Is(err, crash) {
		t.Fatalf("err = %v, quer a queda simulada", err)
	}
	if n, err := readResync(path); err != nil || n != 1 {
		t.Fatalf("o contador antigo deveria sobreviver: %d, %v", n, err)
	}
}

// observeSyncDir troca o seam de fsync de diretório e devolve os diretórios
// sincronizados, na ordem.
func observeSyncDir(t *testing.T, fail error) *[]string {
	t.Helper()
	var synced []string
	prev := syncDirFn
	syncDirFn = func(dir string) error {
		synced = append(synced, dir)
		return fail
	}
	t.Cleanup(func() { syncDirFn = prev })
	return &synced
}

// A criação de replication/kafka é durável: o fsync vai ao pai de CADA nível
// criado, do mais alto para o mais baixo. Sincronizar só o pai imediato deixa
// replication/ fora do fsync, e o primeiro checkpoint (já "confirmado")
// sumiria numa queda junto com o diretório.
func TestCheckpointDirectoryCreationSyncsEveryCreatedParent(t *testing.T) {
	for _, tc := range []struct {
		name string
		do   func(root string) error
	}{
		{"checkpoint", func(root string) error {
			ckpt, _ := checkpointPaths(root, "dest")
			return storeCheckpoint(ckpt, checkpointFile{})
		}},
		{"resync", func(root string) error {
			_, resync := checkpointPaths(root, "dest")
			_, err := requestResync(resync)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			synced := observeSyncDir(t, nil)
			if err := tc.do(root); err != nil {
				t.Fatal(err)
			}
			want := []string{root, filepath.Join(root, "replication")}
			if !slices.Equal(*synced, want) {
				t.Errorf("diretórios sincronizados = %q, quer %q (pai de cada nível criado, do mais alto ao mais baixo)", *synced, want)
			}

			// Diretório que já existe não gera fsync nenhum.
			*synced = nil
			if err := tc.do(root); err != nil {
				t.Fatal(err)
			}
			if len(*synced) != 0 {
				t.Errorf("diretório já existente sincronizou %q; quer nada", *synced)
			}
		})
	}
}

// Falha no fsync do pai interrompe a gravação em vez de seguir como se o
// diretório fosse durável.
func TestCheckpointDirectorySyncFailureAbortsStore(t *testing.T) {
	root := t.TempDir()
	boom := errors.New("fsync boom")
	observeSyncDir(t, boom)
	ckpt, resync := checkpointPaths(root, "dest")
	if err := storeCheckpoint(ckpt, checkpointFile{}); !errors.Is(err, boom) {
		t.Errorf("storeCheckpoint err = %v, quer o fsync que falhou", err)
	}
	if _, statErr := os.Stat(ckpt); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("o checkpoint não devia ter sido gravado: %v", statErr)
	}
	// O diretório já foi criado pela tentativa anterior, mas a criação ainda
	// não é durável: a nova tentativa, por outro caminho, refaz o fsync e
	// falha de novo, em vez de achar o diretório pronto e seguir.
	if _, err := requestResync(resync); !errors.Is(err, boom) {
		t.Errorf("requestResync err = %v, quer o fsync que falhou", err)
	}
}

// A retentativa depois de um fsync de diretório falho sincroniza os níveis que
// a primeira tentativa criou e não tornou duráveis, mesmo achando o diretório
// já existente. Sem a memória dos níveis pendentes, a segunda chamada não vê
// nada a criar, pula o fsync e volta sucesso com a criação ainda volátil.
func TestCheckpointDirectoryRetrySyncsLevelsLeftPendingByAFailedSync(t *testing.T) {
	root := t.TempDir()
	ckpt, _ := checkpointPaths(root, "dest")
	boom := errors.New("fsync boom")
	observeSyncDir(t, boom)
	if err := storeCheckpoint(ckpt, checkpointFile{}); !errors.Is(err, boom) {
		t.Fatalf("primeira gravação err = %v, quer o fsync que falhou", err)
	}
	synced := observeSyncDir(t, nil)
	if err := storeCheckpoint(ckpt, checkpointFile{}); err != nil {
		t.Fatal(err)
	}
	want := []string{root, filepath.Join(root, "replication")}
	if !slices.Equal(*synced, want) {
		t.Errorf("a retentativa sincronizou %q, quer %q (os pais dos níveis criados pela tentativa falha)", *synced, want)
	}
	// Depois do fsync bem-sucedido, nada fica pendente.
	*synced = nil
	if err := storeCheckpoint(ckpt, checkpointFile{}); err != nil {
		t.Fatal(err)
	}
	if len(*synced) != 0 {
		t.Errorf("terceira gravação sincronizou %q; quer nada", *synced)
	}
}
