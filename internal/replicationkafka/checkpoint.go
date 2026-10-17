package replicationkafka

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/atomicfile"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
)

// Os arquivos de estado do publicador moram em
// <DatasetRootDir>/replication/kafka/, um par por destino:
//
//	<destino>.ckpt    o checkpoint (checkpointFile)
//	<destino>.resync  o contador de pedidos de snapshot
//
// O feed em disco (internal/replication) tem o seu próprio diretório; estes
// dois são do PUBLICADOR e dizem até onde ELE já entregou ao Kafka.
const (
	checkpointSubdir    = "replication/kafka"
	checkpointExt       = ".ckpt"
	resyncExt           = ".resync"
	checkpointVersion   = 1
	resyncVersion       = 1
	checkpointFlagSnap  = 1
	checkpointFixedSize = 4 + 1 + 1 + 2 + 8 + 8 + 8 + 8 + 4 + 8 + 32 + 8
	resyncSize          = 4 + 1 + 3 + 8 + 4
)

var (
	checkpointMagic = [4]byte{'N', 'D', 'B', 'K'}
	resyncMagic     = [4]byte{'N', 'D', 'B', 'R'}
	castagnoli      = crc32.MakeTable(crc32.Castagnoli)
)

// errCorruptCheckpoint é o .ckpt ou o .resync ilegível: truncado, magic
// errado, versão desconhecida, CRC que não bate ou corpo incoerente. É erro, e
// não "arquivo ausente": o publicador que perdesse o checkpoint em silêncio
// recomeçaria como destino novo e republicaria o dataset inteiro sem dizer por
// quê. Quem lê decide (o laço trata como queda do destino).
var errCorruptCheckpoint = errors.New("replicationkafka: corrupt checkpoint file")

// checkpointFile é o que o publicador persiste de um destino: o último ponto
// CONFIRMADO pelo Kafka e o que ele precisa para reconhecer, na subida
// seguinte, que o tópico ainda é o mesmo.
//
// Epoch e Cursor são o ponto confirmado. Enquanto Snapshotting é true há um
// reset em curso, com epoch TargetEpoch: o ponto confirmado continua o antigo
// até o snapshot_end ser confirmado, e a subida que encontrar o arquivo assim
// sabe que o snapshot anterior morreu no meio (snapshot_failed).
type checkpointFile struct {
	// Epoch e Cursor são o ponto confirmado; CursorPos é a posição em bytes
	// dele no feed em disco.
	Epoch       string
	Cursor      uint64
	CursorPos   uint64
	TargetEpoch string
	// TargetCursor é o cursor do reset em curso, isto é, a sequence que o
	// snapshot publica na TargetEpoch. Só vale com Snapshotting=true. A regra
	// de rewind sem Kafka precisa dele: o feed que voltou atrás da posição do
	// reset em curso é detectável sem consultar o tópico.
	TargetCursor uint64
	// Partitions e Topic fixam o destino. Topic diferente é troca de destino
	// e dá startup; Partitions diferente dá partitions_changed.
	Partitions int32
	Topic      string
	// Bootstrap são os bootstrapServers na forma canônica
	// (checkpoint.CanonicalBootstrap: aparados, sem vazios, ordenados e sem
	// repetição). storeCheckpoint canoniza; quem compara usa
	// canonicalBootstrapList do lado da configuração.
	Bootstrap []string
	// RenderHash é a impressão da renderização declarada do envelope: cobre só
	// as tabelas publicadas, os aliases declarados e publishHistory, e NUNCA as
	// colunas descobertas em runtime (o FieldID→nome é imutável e a descoberta
	// de coluna não muda o rótulo de uma coluna existente; hashear o Renderer
	// dispararia config_changed em todo reinício). Se muda, o que o tópico já
	// tem deixa de ser comparável e dá config_changed.
	RenderHash [32]byte
	// Snapshotting marca o reset em curso, com a epoch TargetEpoch.
	Snapshotting bool
	// ResyncConsumed é o último valor do contador do .resync que um snapshot
	// já atendeu. É gravado no INÍCIO do snapshot: o pedido que chega durante
	// ele fica acima desse valor e continua pendente.
	ResyncConsumed uint64
	// LastSnapshotEnd é o instante do último snapshot_end confirmado (zero se
	// nunca houve), para o snapshot periódico.
	LastSnapshotEnd time.Time
	// RulerEpoch e RulerSequence são a RÉGUA pendente da conferência de
	// rewind: a geração do feed e a menor sequence em que ele reabriu limpo
	// desde a última decisão tomada neste destino. Vazia (RulerEpoch "")
	// quando não há conferência pendente. Existe porque a vida que reabre o
	// feed e não chega a decidir (o tópico ilegível a vida inteira) ingere e
	// fecha limpo: a vida seguinte reabre com uma sequence MAIOR, e comparar
	// o topo do tópico com ela esconderia o rewind. A régua gravada nunca
	// sobe; ela sai do arquivo quando uma decisão termina (a retomada ou a
	// marca de reset em curso).
	RulerEpoch    string
	RulerSequence uint64
}

// CheckpointDir devolve o diretório dos arquivos de estado dos publicadores
// de um dataset, a partir do DatasetRootDir dele. É a fonte ÚNICA do caminho:
// o servidor passa o resultado em Options.CheckpointDir, e checkpointPaths
// (que o CLI offline também usa) parte dele.
func CheckpointDir(datasetRoot string) string {
	return filepath.Join(datasetRoot, filepath.FromSlash(checkpointSubdir))
}

// checkpointPaths devolve os caminhos do .ckpt e do .resync do destino. root é
// o DatasetRootDir.
func checkpointPaths(root, destination string) (ckpt, resync string) {
	return checkpointFiles(CheckpointDir(root), destination)
}

// checkpointFiles devolve os caminhos do .ckpt e do .resync do destino dentro
// de dir, o CheckpointDir do dataset.
func checkpointFiles(dir, destination string) (ckpt, resync string) {
	return filepath.Join(dir, destination+checkpointExt), filepath.Join(dir, destination+resyncExt)
}

// errBootstrapComma é o broker com vírgula no nome. A forma canônica do
// checkpoint.CanonicalBootstrap junta os brokers por vírgula, então dividir de
// volta um broker que já contém uma partiria um endereço em dois e o arquivo
// não guardaria o que a configuração disse. Um endereço host:porta não tem
// vírgula; "a:9092,b:9092" num único item é a lista colada por engano no YAML.
var errBootstrapComma = errors.New("replicationkafka: bootstrap server contains a comma")

// canonicalBootstrapList é a forma canônica dos brokers como lista, a mesma
// de checkpoint.CanonicalBootstrap (que é quem decide o que é canônico). Sem
// broker nenhum devolve nil, e nunca [""] (o Split da string vazia). Recusa o
// broker que contém vírgula (errBootstrapComma) em vez de dividi-lo; a carga
// da configuração já recusa a vírgula (schema.ValidateKafkaBootstrap), e esta
// guarda é a segunda linha, para quem chegar aqui por outro caminho.
func canonicalBootstrapList(brokers []string) ([]string, error) {
	for _, b := range brokers {
		if strings.Contains(b, ",") {
			return nil, fmt.Errorf("%w: %q", errBootstrapComma, b)
		}
	}
	joined := checkpoint.CanonicalBootstrap(brokers)
	if joined == "" {
		return nil, nil
	}
	return strings.Split(joined, ","), nil
}

// Layout do .ckpt (little-endian):
//
//	0   4  magic "NDBK"
//	4   1  versão = 1
//	5   1  flags (bit 0 = snapshotting)
//	6   2  reservado (zero)
//	8   8  Cursor
//	16  8  CursorPos
//	24  8  ResyncConsumed
//	32  8  TargetCursor
//	40  4  Partitions i32 (não negativo)
//	44  8  LastSnapshotEnd, em nanossegundos Unix (0 = nunca)
//	52 32  RenderHash
//	84  8  RulerSequence
//	92     Epoch, TargetEpoch, Topic, RulerEpoch: cada um  len u16 | bytes
//	       bootstrapCount u16, e por broker  len u16 | bytes
//	    4  crc32c sobre tudo acima
//
// RulerSequence diferente de zero com RulerEpoch vazia é incoerente e é
// recusada como arquivo corrompido: a régua sem geração não diz o que mede.
//
// O formato é de disco e tem golden test (TestCheckpointGoldenBytes) com o
// literal gerado fora do código. O instante zero não tem representação em
// nanossegundos Unix, então 0 é o "nunca": 01/01/1970 00:00:00 UTC exato volta
// como instante zero, e esse instante não é um fim de snapshot possível.
func encodeCheckpoint(c checkpointFile) ([]byte, error) {
	if c.Partitions < 0 {
		return nil, errors.New("replicationkafka: checkpoint partitions is negative")
	}
	bootstrap, err := canonicalBootstrapList(c.Bootstrap)
	if err != nil {
		return nil, err
	}
	if len(bootstrap) > math.MaxUint16 {
		return nil, errors.New("replicationkafka: checkpoint bootstrap list longer than 65535 entries")
	}
	if c.RulerEpoch == "" && c.RulerSequence != 0 {
		return nil, errors.New("replicationkafka: checkpoint ruler sequence without a ruler epoch")
	}
	size := checkpointFixedSize + 4*2 + len(c.Epoch) + len(c.TargetEpoch) + len(c.Topic) + len(c.RulerEpoch) + 2 + 4
	for _, s := range []string{c.Epoch, c.TargetEpoch, c.Topic, c.RulerEpoch} {
		if len(s) > math.MaxUint16 {
			return nil, errors.New("replicationkafka: checkpoint string longer than 65535 bytes")
		}
	}
	for _, s := range bootstrap {
		if len(s) > math.MaxUint16 {
			return nil, errors.New("replicationkafka: checkpoint bootstrap server longer than 65535 bytes")
		}
		size += 2 + len(s)
	}
	b := make([]byte, 0, size)
	b = append(b, checkpointMagic[:]...)
	flags := byte(0)
	if c.Snapshotting {
		flags |= checkpointFlagSnap
	}
	b = append(b, checkpointVersion, flags, 0, 0)
	b = binary.LittleEndian.AppendUint64(b, c.Cursor)
	b = binary.LittleEndian.AppendUint64(b, c.CursorPos)
	b = binary.LittleEndian.AppendUint64(b, c.ResyncConsumed)
	b = binary.LittleEndian.AppendUint64(b, c.TargetCursor)
	b = binary.LittleEndian.AppendUint32(b, uint32(c.Partitions))
	var snapEnd int64
	if !c.LastSnapshotEnd.IsZero() {
		snapEnd = c.LastSnapshotEnd.UnixNano()
	}
	b = binary.LittleEndian.AppendUint64(b, uint64(snapEnd))
	b = append(b, c.RenderHash[:]...)
	b = binary.LittleEndian.AppendUint64(b, c.RulerSequence)
	b = appendCheckpointString(b, c.Epoch)
	b = appendCheckpointString(b, c.TargetEpoch)
	b = appendCheckpointString(b, c.Topic)
	b = appendCheckpointString(b, c.RulerEpoch)
	b = binary.LittleEndian.AppendUint16(b, uint16(len(bootstrap)))
	for _, s := range bootstrap {
		b = appendCheckpointString(b, s)
	}
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli)), nil
}

func appendCheckpointString(b []byte, s string) []byte {
	b = binary.LittleEndian.AppendUint16(b, uint16(len(s)))
	return append(b, s...)
}

func decodeCheckpoint(src []byte) (checkpointFile, error) {
	var c checkpointFile
	if len(src) < checkpointFixedSize+4*2+2+4 || [4]byte(src[0:4]) != checkpointMagic {
		return c, errCorruptCheckpoint
	}
	body, crc := src[:len(src)-4], binary.LittleEndian.Uint32(src[len(src)-4:])
	if crc32.Checksum(body, castagnoli) != crc {
		return c, errCorruptCheckpoint
	}
	flags := src[5]
	if src[4] != checkpointVersion || flags&^checkpointFlagSnap != 0 || src[6] != 0 || src[7] != 0 {
		return c, errCorruptCheckpoint
	}
	c.Snapshotting = flags&checkpointFlagSnap != 0
	c.Cursor = binary.LittleEndian.Uint64(src[8:16])
	c.CursorPos = binary.LittleEndian.Uint64(src[16:24])
	c.ResyncConsumed = binary.LittleEndian.Uint64(src[24:32])
	c.TargetCursor = binary.LittleEndian.Uint64(src[32:40])
	c.Partitions = int32(binary.LittleEndian.Uint32(src[40:44]))
	if c.Partitions < 0 {
		return checkpointFile{}, errCorruptCheckpoint
	}
	if snapEnd := int64(binary.LittleEndian.Uint64(src[44:52])); snapEnd != 0 {
		c.LastSnapshotEnd = time.Unix(0, snapEnd).UTC()
	}
	copy(c.RenderHash[:], src[52:84])
	c.RulerSequence = binary.LittleEndian.Uint64(src[84:92])
	rest := body[92:]
	var ok bool
	if c.Epoch, rest, ok = readCheckpointString(rest); !ok {
		return checkpointFile{}, errCorruptCheckpoint
	}
	if c.TargetEpoch, rest, ok = readCheckpointString(rest); !ok {
		return checkpointFile{}, errCorruptCheckpoint
	}
	if c.Topic, rest, ok = readCheckpointString(rest); !ok {
		return checkpointFile{}, errCorruptCheckpoint
	}
	if c.RulerEpoch, rest, ok = readCheckpointString(rest); !ok || (c.RulerEpoch == "" && c.RulerSequence != 0) {
		return checkpointFile{}, errCorruptCheckpoint
	}
	if len(rest) < 2 {
		return checkpointFile{}, errCorruptCheckpoint
	}
	count := int(binary.LittleEndian.Uint16(rest))
	rest = rest[2:]
	// O count é u16, então a pré-alocação é limitada a 65.535 strings mesmo
	// com um count adulterado (e o CRC refeito); um count acima do que o
	// arquivo comporta falha na leitura do broker que falta.
	if count > 0 {
		c.Bootstrap = make([]string, 0, count)
	}
	for range count {
		var s string
		if s, rest, ok = readCheckpointString(rest); !ok {
			return checkpointFile{}, errCorruptCheckpoint
		}
		c.Bootstrap = append(c.Bootstrap, s)
	}
	if len(rest) != 0 {
		return checkpointFile{}, errCorruptCheckpoint
	}
	return c, nil
}

func readCheckpointString(src []byte) (string, []byte, bool) {
	if len(src) < 2 {
		return "", nil, false
	}
	n := int(binary.LittleEndian.Uint16(src))
	src = src[2:]
	if len(src) < n {
		return "", nil, false
	}
	return string(src[:n]), src[n:], true
}

// syncDirFn é o fsync de diretório de ensureDir, em variável para os testes
// observarem (e falharem) a durabilidade da criação.
var syncDirFn = atomicfile.SyncDir

// dirSync guarda os níveis de diretório criados cujo pai ainda não teve um
// fsync bem-sucedido. Sem essa memória, a nova tentativa depois de um fsync
// falho acharia o diretório já criado, não veria nível nenhum a criar e
// voltaria sucesso sem nunca ter tornado a criação durável.
var dirSync struct {
	mu      sync.Mutex
	pending map[string]struct{}
}

// ensureDir cria dir e os ancestrais que faltam, com fsync do pai de CADA
// nível criado (do mais alto para o mais baixo). Uma entrada de diretório só é
// durável depois do fsync do diretório que a contém: sincronizar só o pai
// imediato deixaria os níveis de cima fora do fsync, e o primeiro checkpoint
// de um destino novo, gravado com sucesso, sumiria numa queda junto com o
// diretório.
//
// Um nível criado fica PENDENTE até o fsync do pai dele dar certo: a chamada
// seguinte, mesmo achando o diretório já criado, sincroniza de novo todo nível
// pendente da cadeia de dir, e só então volta sucesso. Roda inteira sob
// dirSync.mu: uma chamada concorrente que achasse o diretório pronto não pode
// voltar antes do fsync da que o criou. É o mesmo padrão do ensureDir de
// internal/replication/disklog.go, replicado porque aquele é privado do pacote
// replication.
func ensureDir(dir string) error {
	dirSync.mu.Lock()
	defer dirSync.mu.Unlock()
	if dirSync.pending == nil {
		dirSync.pending = make(map[string]struct{})
	}
	dir = filepath.Clean(dir)
	for d := dir; ; d = filepath.Dir(d) {
		_, err := os.Stat(d)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		dirSync.pending[d] = struct{}{}
		if filepath.Dir(d) == d {
			break
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	// A cadeia de dir, do mais baixo para o mais alto; o fsync vai na ordem
	// inversa.
	var chain []string
	for d := dir; ; d = filepath.Dir(d) {
		if _, ok := dirSync.pending[d]; ok {
			chain = append(chain, d)
		}
		if filepath.Dir(d) == d {
			break
		}
	}
	for i := len(chain) - 1; i >= 0; i-- {
		if err := syncDirFn(filepath.Dir(chain[i])); err != nil {
			return err
		}
		delete(dirSync.pending, chain[i])
	}
	return nil
}

// storeCheckpoint grava o checkpoint em path por atomicfile (tmp, fsync,
// rename, fsync do diretório): quem lê depois de uma queda vê o checkpoint
// antigo inteiro ou o novo inteiro. Cria o diretório se faltar, com fsync de
// cada pai criado (ensureDir).
func storeCheckpoint(path string, c checkpointFile) error {
	return storeCheckpointHook(path, c, nil)
}

// storeCheckpointHook é storeCheckpoint com o seam de falha do atomicfile
// (hook nil em produção): os testes de queda interrompem a gravação entre os
// passos e conferem que o checkpoint antigo sobrevive inteiro.
func storeCheckpointHook(path string, c checkpointFile, hook atomicfile.Hook) error {
	b, err := encodeCheckpoint(c)
	if err != nil {
		return err
	}
	if err := ensureDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("replicationkafka: create checkpoint directory: %w", err)
	}
	return atomicfile.WriteWithHook(path, b, 0o644, hook)
}

// loadCheckpoint lê o checkpoint de path. Arquivo ausente é a primeira subida
// do destino: devolve ok=false sem erro. Qualquer outra falha de leitura, ou
// um conteúdo que não passa no CRC, devolve erro (nunca um checkpoint
// parcial).
func loadCheckpoint(path string) (checkpointFile, bool, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return checkpointFile{}, false, nil
	}
	if err != nil {
		return checkpointFile{}, false, err
	}
	c, err := decodeCheckpoint(b)
	if err != nil {
		return checkpointFile{}, false, fmt.Errorf("%s: %w", path, err)
	}
	return c, true, nil
}

// CheckpointRow é o que o .ckpt de um destino diz a quem lê o disco sem o
// publicador no ar (o `nishidb query --config`, na `_meta.replication`): o
// último ponto confirmado gravado e o instante do último snapshot_end. O
// resto do arquivo é a régua da retomada, que só o publicador interpreta.
type CheckpointRow struct {
	// Epoch e Cursor são o ponto confirmado gravado: Cursor é o que o
	// publicador vivo chama de CheckpointCursor.
	Epoch  string
	Cursor uint64
	// LastSnapshotAt é o instante do último snapshot_end confirmado; zero se
	// nunca houve.
	LastSnapshotAt time.Time
}

// LoadCheckpointRow lê o .ckpt do destino dentro de dir, o CheckpointDir do
// dataset. Arquivo ausente (destino que nunca gravou checkpoint) devolve
// ok=false sem erro; arquivo ilegível devolve o erro, nunca uma linha parcial
// — o mesmo tri-estado do loadCheckpoint, que é quem lê.
func LoadCheckpointRow(dir, destination string) (row CheckpointRow, ok bool, err error) {
	path, _ := checkpointFiles(dir, destination)
	c, ok, err := loadCheckpoint(path)
	if err != nil || !ok {
		return CheckpointRow{}, false, err
	}
	return CheckpointRow{Epoch: c.Epoch, Cursor: c.Cursor, LastSnapshotAt: c.LastSnapshotEnd}, true, nil
}

// resyncMu serializa o ler-incrementar-gravar do contador: dois pedidos
// concorrentes (duas chamadas administrativas) não podem ler o mesmo valor e
// devolver o mesmo número, senão um deles seria indistinguível do outro.
var resyncMu sync.Mutex

// Layout do .resync (little-endian):
//
//	0   4  magic "NDBR"
//	4   1  versão = 1
//	5   3  reservado (zero)
//	8   8  contador de pedidos
//	16  4  crc32c sobre tudo acima
//
// Tem golden test (TestResyncCounterGoldenBytes), literal gerado fora do
// código.
func encodeResync(counter uint64) []byte {
	b := make([]byte, 0, resyncSize)
	b = append(b, resyncMagic[:]...)
	b = append(b, resyncVersion, 0, 0, 0)
	b = binary.LittleEndian.AppendUint64(b, counter)
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

func decodeResync(src []byte) (uint64, error) {
	if len(src) != resyncSize || [4]byte(src[0:4]) != resyncMagic {
		return 0, errCorruptCheckpoint
	}
	if crc32.Checksum(src[:resyncSize-4], castagnoli) != binary.LittleEndian.Uint32(src[resyncSize-4:]) {
		return 0, errCorruptCheckpoint
	}
	if src[4] != resyncVersion || src[5] != 0 || src[6] != 0 || src[7] != 0 {
		return 0, errCorruptCheckpoint
	}
	return binary.LittleEndian.Uint64(src[8:16]), nil
}

// readResync lê o contador; arquivo ausente vale zero (nenhum pedido).
func readResync(path string) (uint64, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n, err := decodeResync(b)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", path, err)
	}
	return n, nil
}

// requestResync registra um pedido de snapshot: incrementa o contador
// persistido em path (o <destino>.resync) e devolve o valor novo. O pedido é
// durável quando a função volta.
//
// O pedido é um CONTADOR, e não um marcador que o laço apaga, porque o laço
// consome o pedido no INÍCIO do snapshot (gravando em ResyncConsumed do
// checkpoint o valor que atendeu) e o operador pode pedir de novo durante ele.
// Com um marcador apagável, esse segundo pedido seria apagado junto com o
// primeiro ao fim do snapshot; com o contador, ele fica acima de
// ResyncConsumed e continua pendente. Por isso o arquivo nunca é apagado.
func requestResync(path string) (uint64, error) {
	return requestResyncHook(path, nil)
}

// requestResyncHook é requestResync com o seam de falha do atomicfile (hook
// nil em produção).
func requestResyncHook(path string, hook atomicfile.Hook) (uint64, error) {
	resyncMu.Lock()
	defer resyncMu.Unlock()
	current, err := readResync(path)
	if err != nil {
		return 0, err
	}
	if current == math.MaxUint64 {
		return 0, errors.New("replicationkafka: resync counter overflow")
	}
	if err := ensureDir(filepath.Dir(path)); err != nil {
		return 0, fmt.Errorf("replicationkafka: create resync directory: %w", err)
	}
	next := current + 1
	if err := atomicfile.WriteWithHook(path, encodeResync(next), 0o644, hook); err != nil {
		return 0, err
	}
	return next, nil
}

// pendingResync diz se há pedido de snapshot ainda não atendido: o contador
// gravado em path é maior que consumed (o ResyncConsumed do checkpoint).
// Devolve também o valor do contador, que o laço grava em ResyncConsumed ao
// iniciar o snapshot que atende o pedido. Arquivo ausente é contador zero,
// nunca pendente.
func pendingResync(path string, consumed uint64) (uint64, bool, error) {
	n, err := readResync(path)
	if err != nil {
		return 0, false, err
	}
	return n, n > consumed, nil
}
