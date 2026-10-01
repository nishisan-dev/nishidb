// Package counter implementa o contador mantido na escrita: por dataset e
// por contador declarado, um vetor denso rowId → (condição | grupo), um
// dicionário de grupos e os totais por grupo, alimentados pela TRANSIÇÃO de
// estado de cada linha dentro do applyRecord.
//
// O contador conta ESTADOS, não mensagens (planning/contadores-mantidos-na-
// escrita.md §3): reprocessar o mesmo evento é uma transição de X para X, que
// não mexe em nada, e mudar de grupo ajusta as duas pontas na mesma operação.
// É o que dispensa qualquer contabilidade de "quais mensagens já contei".
package counter

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"
)

// Layout de grupos.dict (v1), cópia deliberada do fielddict v2:
//
//	header (8 bytes):        magic "NDBG" | version u8 | reserved[3]
//	entrada de grupo:        len u16 LE (>=1) | chave composta | crc32c u32
//	                         (CRC sobre len..chave; id implícito, 1-based)
//	registro de aposentadoria: 0x0000 u16 LE | id u32 LE | crc32c u32
//	                         (10 bytes fixos; CRC sobre os 6 primeiros)
//
// Os dois registros se distinguem pelo len: zero é impossível numa entrada de
// grupo (chave vazia é recusada), então serve de discriminador sem gastar um
// byte de tag — o mesmo truque do registro de hint do fielddict.
//
// Append-only, e aposentadoria como REGISTRO em vez de bit reescrito na
// entrada, pela razão que o fielddict registra: a abertura trunca o arquivo no
// primeiro registro com CRC inválido, e uma reescrita in-place rasgada no meio
// apagaria todos os grupos posteriores — grupos cujos ids já estão no vetor.
// Com append-only o rasgo só pode estar na cauda.
const (
	headerSize           = 8
	entryOverhead        = 2 + 4 // len antes da chave, CRC depois
	retirementRecordSize = 2 + 4 + 4
	dictVersion          = 1
)

var (
	dictMagic  = [4]byte{'N', 'D', 'B', 'G'}
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
)

// ErrGroupCeilingReached marca a internação de um grupo NOVO quando o
// número de grupos vivos já é o teto declarado (maxGroups). Não é dado ruim
// nem falha de infra: a linha entra normalmente e o contador a trata como
// "sem grupo" — é quem chama que decide isso, e conta.
var ErrGroupCeilingReached = errors.New("contador: teto de grupos atingido")

// dictFile é a superfície de arquivo do dicionário. Interface para
// os testes conferirem a ORDEM das chamadas de durabilidade (WriteAt antes de
// Sync, Sync antes de o id sair).
type dictFile interface {
	io.WriterAt
	Sync() error
	Truncate(size int64) error
	Close() error
}

// openDictFile abre o arquivo do dicionário. É variável, e não chamada direta,
// para que os testes do pacote ponham o gravador de chamadas de durabilidade
// ANTES da abertura — o fsync da abertura sincronizada (OpenDict) acontece
// antes de qualquer outro ponto em que o handle pudesse ser trocado.
var openDictFile = func(path string) (dictFile, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
}

// Dict é o interning dos grupos de um contador: chave composta → id.
//
// Intern e Retire são exclusivos do writer; Key, ByKey e os contadores
// são seguros para leitores concorrentes. O mutex é FOLHA na ordem de locks do
// Store: nada tomado sob ele toma o wmu. O writer o toma sob o wmu, e só ao
// internar um grupo NOVO ou aposentar — o lookup de chave existente é um RLock.
//
// Ids nunca são reemitidos: aposentar um grupo o tira do mapa, e a mesma chave,
// se reaparecer, ganha o próximo id. Reciclar exigiria provar que nenhuma
// entrada do vetor ainda aponta para o id, e essa prova é do tipo que falha em
// produção e não em teste (planning §6.5).
type Dict struct {
	mu   sync.RWMutex
	f    dictFile
	size int64
	// syncedSize é até onde o arquivo foi fsyncado: o prefixo que sobrevive a
	// uma queda de energia. Menor que size quando há append pendente.
	syncedSize int64
	ceiling    uint32
	// keys e live são paralelos, índice 0 = id 1. A chave de um grupo
	// aposentado continua guardada: a tabela virtual e a auditoria precisam
	// nomear o grupo que uma entrada órfã do vetor ainda aponta.
	keys          [][]byte
	live          []bool
	byKey         map[string]uint32
	liveGroups    uint32
	retiredGroups uint32
}

// OpenDict cria ou reabre o dicionário, validando o prefixo por CRC e
// truncando qualquer cauda rasgada por crash. ceiling é o número máximo de
// grupos VIVOS.
//
// # A abertura sincronizada
//
// Um arquivo com mais que o header é fsyncado DEPOIS da varredura (e da
// truncagem), uma vez por abertura. O motivo é o page cache, que sobrevive à
// morte do PROCESSO: num kill -9, OOM ou panic, a cauda que o processo anterior
// escreveu sem fsync continua no cache, e esta abertura a lê como se estivesse
// em disco. O replay resolve as chaves aos MESMOS ids, o dicionário não tem
// append pendente, e o checkpoint não teria o que sincronizar — mas o vetor
// passaria a referenciar ids que só existem no cache, e uma queda de energia
// depois os apagaria: entradas órfãs, zeradas em silêncio pela dobra. Depois do
// fsync, o que esta abertura leu é exatamente o que está em disco.
func OpenDict(path string, ceiling uint32) (*Dict, error) {
	f, err := openDictFile(path)
	if err != nil {
		return nil, fmt.Errorf("contador: abrir %s: %w", path, err)
	}
	d := &Dict{f: f, ceiling: ceiling, byKey: make(map[string]uint32)}
	raw, err := os.ReadFile(path)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("contador: ler %s: %w", path, err)
	}
	if len(raw) == 0 {
		var h [headerSize]byte
		copy(h[0:4], dictMagic[:])
		h[4] = dictVersion
		if _, err := f.WriteAt(h[:], 0); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("contador: escrever header de %s: %w", path, err)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, err
		}
		d.size, d.syncedSize = headerSize, headerSize
		return d, nil
	}
	if len(raw) < headerSize || [4]byte(raw[0:4]) != dictMagic {
		_ = f.Close()
		return nil, fmt.Errorf("contador: header inválido em %s", path)
	}
	if raw[4] != dictVersion {
		_ = f.Close()
		return nil, fmt.Errorf("contador: %s está na version %d, esperada %d: recrie o contador (a abertura reconstrói)",
			path, raw[4], dictVersion)
	}
	pos := d.readRecords(raw)
	if int64(pos) != int64(len(raw)) {
		if err := f.Truncate(int64(pos)); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("contador: truncar cauda rasgada de %s em %d: %w", path, pos, err)
		}
	}
	d.size = int64(pos)
	if d.size > headerSize {
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("contador: fsync da abertura de %s: %w", path, err)
		}
	}
	d.syncedSize = d.size
	return d, nil
}

// readRecords percorre o prefixo válido e devolve onde ele termina. Para no
// primeiro registro incompleto, com CRC errado ou logicamente impossível (uma
// aposentadoria de id que não existe ou já saiu): a partir dali é cauda
// rasgada, e o que vier depois não pode ser confiado.
func (d *Dict) readRecords(raw []byte) int {
	pos := headerSize
	for pos < len(raw) {
		rest := raw[pos:]
		if len(rest) < 2 {
			break
		}
		if binary.LittleEndian.Uint16(rest[0:2]) == 0 {
			if len(rest) < retirementRecordSize {
				break
			}
			body := rest[:6]
			if crc32.Checksum(body, castagnoli) != binary.LittleEndian.Uint32(rest[6:10]) {
				break
			}
			id := binary.LittleEndian.Uint32(body[2:6])
			if id < 1 || int(id) > len(d.keys) || !d.live[id-1] {
				break
			}
			d.retireInMemory(id)
			pos += retirementRecordSize
			continue
		}
		n := int(binary.LittleEndian.Uint16(rest[0:2]))
		if len(rest) < entryOverhead+n {
			break
		}
		body := rest[:2+n]
		if crc32.Checksum(body, castagnoli) != binary.LittleEndian.Uint32(rest[2+n:2+n+4]) {
			break
		}
		key := make([]byte, n)
		copy(key, body[2:])
		d.internInMemory(key)
		pos += entryOverhead + n
	}
	return pos
}

func (d *Dict) internInMemory(key []byte) uint32 {
	d.keys = append(d.keys, key)
	d.live = append(d.live, true)
	id := uint32(len(d.keys))
	d.byKey[string(key)] = id
	d.liveGroups++
	return id
}

func (d *Dict) retireInMemory(id uint32) {
	d.live[id-1] = false
	delete(d.byKey, string(d.keys[id-1]))
	d.liveGroups--
	d.retiredGroups++
}

// Intern devolve o id do grupo, criando-o (com fsync) se novo. Exclusivo do
// writer. Chave já internada nunca toca o arquivo nem aloca: o lookup é
// m[string(key)], que o compilador não materializa.
//
// ErrGroupCeilingReached quando um grupo NOVO não cabe: o teto conta vivos, e
// aposentar um grupo libera a vaga.
func (d *Dict) Intern(key []byte) (uint32, error) {
	if id, ok := d.ByKey(key); ok {
		return id, nil
	}
	if len(key) == 0 {
		return 0, fmt.Errorf("contador: chave de grupo vazia")
	}
	if len(key) > math.MaxUint16 {
		return 0, fmt.Errorf("contador: chave de grupo com %d bytes excede u16", len(key))
	}
	if d.liveGroups >= d.ceiling {
		return 0, fmt.Errorf("%w (maxGroups=%d)", ErrGroupCeilingReached, d.ceiling)
	}
	if len(d.keys) >= math.MaxInt32 {
		return 0, fmt.Errorf("contador: limite de %d ids de grupo atingido", math.MaxInt32)
	}

	entry := make([]byte, 0, entryOverhead+len(key))
	entry = binary.LittleEndian.AppendUint16(entry, uint16(len(key)))
	entry = append(entry, key...)
	entry = binary.LittleEndian.AppendUint32(entry, crc32.Checksum(entry, castagnoli))

	// fsync ANTES de o id existir em memória: nenhuma entrada do vetor pode
	// apontar para um id que a reabertura não resolva (invariante do
	// fielddict, §32/§14.6, aplicada aos grupos).
	if _, err := d.f.WriteAt(entry, d.size); err != nil {
		return 0, fmt.Errorf("contador: append do grupo: %w", err)
	}
	if err := d.f.Sync(); err != nil {
		return 0, fmt.Errorf("contador: fsync do grupo: %w", err)
	}

	keyClone := make([]byte, len(key))
	copy(keyClone, key)
	d.mu.Lock()
	d.size += int64(len(entry))
	d.syncedSize = d.size
	id := d.internInMemory(keyClone)
	d.mu.Unlock()
	return id, nil
}

// Retire registra (com fsync) que o grupo saiu: ele deixa de resolver por
// chave e de ser vivo, e o id nunca é reemitido. Exclusivo do writer.
//
// Aposentar um id inexistente ou já aposentado é erro, não no-op: a decisão é
// do coletor sobre um grupo vivo, e a repetição denunciaria um coletor que
// perdeu o próprio estado.
func (d *Dict) Retire(id uint32) error {
	d.mu.RLock()
	valid := id >= 1 && int(id) <= len(d.keys) && d.live[id-1]
	d.mu.RUnlock()
	if !valid {
		return fmt.Errorf("contador: aposentar grupo %d: não é um grupo vivo", id)
	}
	var rec [retirementRecordSize]byte
	binary.LittleEndian.PutUint16(rec[0:2], 0)
	binary.LittleEndian.PutUint32(rec[2:6], id)
	binary.LittleEndian.PutUint32(rec[6:10], crc32.Checksum(rec[:6], castagnoli))
	if _, err := d.f.WriteAt(rec[:], d.size); err != nil {
		return fmt.Errorf("contador: append da aposentadoria de %d: %w", id, err)
	}
	if err := d.f.Sync(); err != nil {
		return fmt.Errorf("contador: fsync da aposentadoria de %d: %w", id, err)
	}
	d.mu.Lock()
	d.size += retirementRecordSize
	d.syncedSize = d.size
	d.retireInMemory(id)
	d.mu.Unlock()
	return nil
}

// Key devolve a chave composta do id e se o grupo está vivo. A chave de um
// grupo aposentado continua disponível (live=false); id 0 e id além do limite
// devolvem nil, false. Seguro para leitores concorrentes; a slice é estável
// (nunca mutada) e pode ser retida.
func (d *Dict) Key(id uint32) (key []byte, live bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if id < 1 || int(id) > len(d.keys) {
		return nil, false
	}
	return d.keys[id-1], d.live[id-1]
}

// ByKey resolve a chave de um grupo VIVO para o id. Seguro para leitores
// concorrentes; não aloca.
func (d *Dict) ByKey(key []byte) (uint32, bool) {
	d.mu.RLock()
	id, ok := d.byKey[string(key)]
	d.mu.RUnlock()
	return id, ok
}

// Limit é o maior id emitido mais um: o fim de uma varredura por id.
func (d *Dict) Limit() uint32 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return uint32(len(d.keys)) + 1
}

// LiveGroups é o número de grupos que resolvem por chave — o que o teto limita.
func (d *Dict) LiveGroups() uint32 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.liveGroups
}

// RetiredGroups é o número de grupos que saíram desde a criação do dicionário.
func (d *Dict) RetiredGroups() uint32 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.retiredGroups
}

// Ceiling é o número máximo de grupos vivos declarado.
func (d *Dict) Ceiling() uint32 { return d.ceiling }

// SizeBytes devolve o tamanho válido do arquivo.
func (d *Dict) SizeBytes() int64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.size
}

// Close fecha o arquivo do dicionário, sincronizando antes o append pendente.
//
// O fsync aqui não é o que dá durabilidade ao contador — quem a dá é o
// checkpoint, e um Close sem checkpoint deixa o vetor atrás e o replay o
// repõe. Ele existe pelo ROLLBACK com parada limpa: o binário anterior
// confia no prefixo do arquivo como durável (ele sincronizava cada entrada) e
// nunca sincroniza uma cauda herdada. Sem este fsync, a cauda escrita por este
// binário e não sincronizada seria adotada como durável por ele.
func (d *Dict) Close() error {
	var syncErr error
	if d.size > d.syncedSize {
		if err := d.f.Sync(); err != nil {
			syncErr = fmt.Errorf("contador: fsync do dicionário no fechamento: %w", err)
		} else {
			d.syncedSize = d.size
		}
	}
	return errors.Join(syncErr, d.f.Close())
}
