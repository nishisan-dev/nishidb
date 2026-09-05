// Package contador implementa o contador mantido na escrita: por dataset e
// por contador declarado, um vetor denso rowId → (condição | grupo), um
// dicionário de grupos e os totais por grupo, alimentados pela TRANSIÇÃO de
// estado de cada linha dentro do applyRecord.
//
// O contador conta ESTADOS, não mensagens (planning/contadores-mantidos-na-
// escrita.md §3): reprocessar o mesmo evento é uma transição de X para X, que
// não mexe em nada, e mudar de grupo ajusta as duas pontas na mesma operação.
// É o que dispensa qualquer contabilidade de "quais mensagens já contei".
package contador

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
	cabecalhoDoDicionario   = 8
	overheadDaEntrada       = 2 + 4 // len antes da chave, CRC depois
	registroDeAposentadoria = 2 + 4 + 4
	versaoDoDicionario      = 1
)

var (
	magicDoDicionario = [4]byte{'N', 'D', 'B', 'G'}
	castagnoli        = crc32.MakeTable(crc32.Castagnoli)
)

// ErrTetoDeGrupos marca a internação de um grupo NOVO quando o número de grupos
// vivos já é o teto declarado (maxGroups). Não é dado ruim nem falha de infra:
// a linha entra normalmente e o contador a trata como "sem grupo" — é quem
// chama que decide isso, e conta.
var ErrTetoDeGrupos = errors.New("contador: teto de grupos atingido")

// arquivoDoDicionario é a superfície de arquivo do dicionário. Interface para
// os testes conferirem a ORDEM das chamadas de durabilidade (WriteAt antes de
// Sync, Sync antes de o id sair).
type arquivoDoDicionario interface {
	io.WriterAt
	Sync() error
	Truncate(size int64) error
	Close() error
}

// Dicionario é o interning dos grupos de um contador: chave composta → id.
//
// Interna e Aposenta são exclusivos do writer; Chave, PorChave e os contadores
// são seguros para leitores concorrentes. O mutex é FOLHA na ordem de locks do
// Store: nada tomado sob ele toma o wmu. O writer o toma sob o wmu, e só ao
// internar um grupo NOVO ou aposentar — o lookup de chave existente é um RLock.
//
// Ids nunca são reemitidos: aposentar um grupo o tira do mapa, e a mesma chave,
// se reaparecer, ganha o próximo id. Reciclar exigiria provar que nenhuma
// entrada do vetor ainda aponta para o id, e essa prova é do tipo que falha em
// produção e não em teste (planning §6.5).
type Dicionario struct {
	mu      sync.RWMutex
	f       arquivoDoDicionario
	tamanho int64
	teto    uint32
	// chaves e vivo são paralelos, índice 0 = id 1. A chave de um grupo
	// aposentado continua guardada: a tabela virtual e a auditoria precisam
	// nomear o grupo que uma entrada órfã do vetor ainda aponta.
	chaves   [][]byte
	vivo     []bool
	porChave map[string]uint32
	vivos    uint32
	aposent  uint32
}

// AbreDicionario cria ou reabre o dicionário, validando o prefixo por CRC e
// truncando qualquer cauda rasgada por crash. teto é o número máximo de grupos
// VIVOS.
func AbreDicionario(path string, teto uint32) (*Dicionario, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("contador: abrir %s: %w", path, err)
	}
	d := &Dicionario{f: f, teto: teto, porChave: make(map[string]uint32)}
	raw, err := os.ReadFile(path)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("contador: ler %s: %w", path, err)
	}
	if len(raw) == 0 {
		var h [cabecalhoDoDicionario]byte
		copy(h[0:4], magicDoDicionario[:])
		h[4] = versaoDoDicionario
		if _, err := f.WriteAt(h[:], 0); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("contador: escrever header de %s: %w", path, err)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, err
		}
		d.tamanho = cabecalhoDoDicionario
		return d, nil
	}
	if len(raw) < cabecalhoDoDicionario || [4]byte(raw[0:4]) != magicDoDicionario {
		_ = f.Close()
		return nil, fmt.Errorf("contador: header inválido em %s", path)
	}
	if raw[4] != versaoDoDicionario {
		_ = f.Close()
		return nil, fmt.Errorf("contador: %s está na version %d, esperada %d: recrie o contador (a abertura reconstrói)",
			path, raw[4], versaoDoDicionario)
	}
	pos := d.leRegistros(raw)
	if int64(pos) != int64(len(raw)) {
		if err := f.Truncate(int64(pos)); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("contador: truncar cauda rasgada de %s em %d: %w", path, pos, err)
		}
	}
	d.tamanho = int64(pos)
	return d, nil
}

// leRegistros percorre o prefixo válido e devolve onde ele termina. Para no
// primeiro registro incompleto, com CRC errado ou logicamente impossível (uma
// aposentadoria de id que não existe ou já saiu): a partir dali é cauda
// rasgada, e o que vier depois não pode ser confiado.
func (d *Dicionario) leRegistros(raw []byte) int {
	pos := cabecalhoDoDicionario
	for pos < len(raw) {
		rest := raw[pos:]
		if len(rest) < 2 {
			break
		}
		if binary.LittleEndian.Uint16(rest[0:2]) == 0 {
			if len(rest) < registroDeAposentadoria {
				break
			}
			corpo := rest[:6]
			if crc32.Checksum(corpo, castagnoli) != binary.LittleEndian.Uint32(rest[6:10]) {
				break
			}
			id := binary.LittleEndian.Uint32(corpo[2:6])
			if id < 1 || int(id) > len(d.chaves) || !d.vivo[id-1] {
				break
			}
			d.aposentaEmMemoria(id)
			pos += registroDeAposentadoria
			continue
		}
		n := int(binary.LittleEndian.Uint16(rest[0:2]))
		if len(rest) < overheadDaEntrada+n {
			break
		}
		corpo := rest[:2+n]
		if crc32.Checksum(corpo, castagnoli) != binary.LittleEndian.Uint32(rest[2+n:2+n+4]) {
			break
		}
		chave := make([]byte, n)
		copy(chave, corpo[2:])
		d.internaEmMemoria(chave)
		pos += overheadDaEntrada + n
	}
	return pos
}

func (d *Dicionario) internaEmMemoria(chave []byte) uint32 {
	d.chaves = append(d.chaves, chave)
	d.vivo = append(d.vivo, true)
	id := uint32(len(d.chaves))
	d.porChave[string(chave)] = id
	d.vivos++
	return id
}

func (d *Dicionario) aposentaEmMemoria(id uint32) {
	d.vivo[id-1] = false
	delete(d.porChave, string(d.chaves[id-1]))
	d.vivos--
	d.aposent++
}

// Interna devolve o id do grupo, criando-o (com fsync) se novo. Exclusivo do
// writer. Chave já internada nunca toca o arquivo nem aloca: o lookup é
// m[string(chave)], que o compilador não materializa.
//
// ErrTetoDeGrupos quando um grupo NOVO não cabe: o teto conta vivos, e
// aposentar um grupo libera a vaga.
func (d *Dicionario) Interna(chave []byte) (uint32, error) {
	if id, ok := d.PorChave(chave); ok {
		return id, nil
	}
	if len(chave) == 0 {
		return 0, fmt.Errorf("contador: chave de grupo vazia")
	}
	if len(chave) > math.MaxUint16 {
		return 0, fmt.Errorf("contador: chave de grupo com %d bytes excede u16", len(chave))
	}
	if d.vivos >= d.teto {
		return 0, fmt.Errorf("%w (maxGroups=%d)", ErrTetoDeGrupos, d.teto)
	}
	if len(d.chaves) >= math.MaxInt32 {
		return 0, fmt.Errorf("contador: limite de %d ids de grupo atingido", math.MaxInt32)
	}

	entrada := make([]byte, 0, overheadDaEntrada+len(chave))
	entrada = binary.LittleEndian.AppendUint16(entrada, uint16(len(chave)))
	entrada = append(entrada, chave...)
	entrada = binary.LittleEndian.AppendUint32(entrada, crc32.Checksum(entrada, castagnoli))

	// fsync ANTES de o id existir em memória: nenhuma entrada do vetor pode
	// apontar para um id que a reabertura não resolva (invariante do
	// fielddict, §32/§14.6, aplicada aos grupos).
	if _, err := d.f.WriteAt(entrada, d.tamanho); err != nil {
		return 0, fmt.Errorf("contador: append do grupo: %w", err)
	}
	if err := d.f.Sync(); err != nil {
		return 0, fmt.Errorf("contador: fsync do grupo: %w", err)
	}

	copia := make([]byte, len(chave))
	copy(copia, chave)
	d.mu.Lock()
	d.tamanho += int64(len(entrada))
	id := d.internaEmMemoria(copia)
	d.mu.Unlock()
	return id, nil
}

// Aposenta registra (com fsync) que o grupo saiu: ele deixa de resolver por
// chave e de ser vivo, e o id nunca é reemitido. Exclusivo do writer.
//
// Aposentar um id inexistente ou já aposentado é erro, não no-op: a decisão é
// do coletor sobre um grupo vivo, e a repetição denunciaria um coletor que
// perdeu o próprio estado.
func (d *Dicionario) Aposenta(id uint32) error {
	d.mu.RLock()
	valido := id >= 1 && int(id) <= len(d.chaves) && d.vivo[id-1]
	d.mu.RUnlock()
	if !valido {
		return fmt.Errorf("contador: aposentar grupo %d: não é um grupo vivo", id)
	}
	var reg [registroDeAposentadoria]byte
	binary.LittleEndian.PutUint16(reg[0:2], 0)
	binary.LittleEndian.PutUint32(reg[2:6], id)
	binary.LittleEndian.PutUint32(reg[6:10], crc32.Checksum(reg[:6], castagnoli))
	if _, err := d.f.WriteAt(reg[:], d.tamanho); err != nil {
		return fmt.Errorf("contador: append da aposentadoria de %d: %w", id, err)
	}
	if err := d.f.Sync(); err != nil {
		return fmt.Errorf("contador: fsync da aposentadoria de %d: %w", id, err)
	}
	d.mu.Lock()
	d.tamanho += registroDeAposentadoria
	d.aposentaEmMemoria(id)
	d.mu.Unlock()
	return nil
}

// Chave devolve a chave composta do id e se o grupo está vivo. A chave de um
// grupo aposentado continua disponível (vivo=false); id 0 e id além do limite
// devolvem nil, false. Seguro para leitores concorrentes; a slice é estável
// (nunca mutada) e pode ser retida.
func (d *Dicionario) Chave(id uint32) (chave []byte, vivo bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if id < 1 || int(id) > len(d.chaves) {
		return nil, false
	}
	return d.chaves[id-1], d.vivo[id-1]
}

// PorChave resolve a chave de um grupo VIVO para o id. Seguro para leitores
// concorrentes; não aloca.
func (d *Dicionario) PorChave(chave []byte) (uint32, bool) {
	d.mu.RLock()
	id, ok := d.porChave[string(chave)]
	d.mu.RUnlock()
	return id, ok
}

// Limite é o maior id emitido mais um: o fim de uma varredura por id.
func (d *Dicionario) Limite() uint32 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return uint32(len(d.chaves)) + 1
}

// Vivos é o número de grupos que resolvem por chave — o que o teto limita.
func (d *Dicionario) Vivos() uint32 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.vivos
}

// Aposentados é o número de grupos que saíram desde a criação do dicionário.
func (d *Dicionario) Aposentados() uint32 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.aposent
}

// Teto é o número máximo de grupos vivos declarado.
func (d *Dicionario) Teto() uint32 { return d.teto }

// SizeBytes devolve o tamanho válido do arquivo.
func (d *Dicionario) SizeBytes() int64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.tamanho
}

// Close fecha o arquivo do dicionário.
func (d *Dicionario) Close() error { return d.f.Close() }
