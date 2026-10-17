package replication

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// SnapshotSource é o que o exportador de snapshot precisa de um Store. O
// *storage.Store satisfaz a interface; ela existe porque este pacote não pode
// importar o storage (é o storage que importa este pacote para publicar no
// feed).
type SnapshotSource interface {
	OpenSnapshot() types.Snapshot
	CloseSnapshot(types.Snapshot)
	ScanLiveFrom(snap types.Snapshot, from, limit types.RowID) types.RowIDIterator
	GetByRowID(snap types.Snapshot, id types.RowID, buf []byte) (types.Record, []byte, bool, error)
}

// SnapshotTable é uma tabela a exportar: o dataset base ou a tabela histórica
// irmã, com o nome que as entradas levam em Table.
type SnapshotTable struct {
	Name    string
	History bool
	Source  SnapshotSource
}

// SnapshotHeader identifica a geração exportada. Cursor é o ponto do feed a
// partir do qual o tail completa o snapshot; At é UM instante só para a
// exportação inteira. EndPos é a posição em bytes do fim do último lote
// fechado, lida JUNTO com Epoch e Cursor (Stream.Position): é a base do atraso
// em bytes do publicador enquanto o tail ainda não confirmou lote nenhum.
type SnapshotHeader struct {
	Epoch  string
	Cursor uint64
	EndPos uint64
	At     time.Time
}

// ErrInvalidSnapshotExport recusa uma exportação sem feed, sem callbacks ou
// com bloco não positivo.
var ErrInvalidSnapshotExport = errors.New("replication: invalid snapshot export")

// snapshotChunkMaxBytes é o teto, em bytes de key+payload copiados, de um
// bloco do exportador. O bloco fecha na primeira linha que o leva a este teto
// ou além, e sempre varre pelo menos uma linha — uma linha maior que o teto
// sai sozinha num bloco (um bloco de linhas mortas, ou o último, pode não
// copiar nenhuma). Junto com o teto em linhas (chunkRows) é o que limita a
// memória da exportação: sem ele, um bloco de linhas grandes seguraria
// chunkRows × o tamanho da maior linha.
const snapshotChunkMaxBytes = 8 << 20

// snapshotArenaKeepBytes é a capacidade acima da qual os buffers de cópia são
// descartados entre blocos. Uma linha gigante faz o buffer crescer além do
// teto; reaproveitá-lo nos blocos seguintes prenderia essa memória pela
// exportação inteira.
const snapshotArenaKeepBytes = 2 * snapshotChunkMaxBytes

// ExportSnapshot exporta o estado vivo das tabelas em blocos curtos e o
// entrega a emit, uma entrada por linha.
//
// O cursor do feed é lido UMA vez, ANTES do primeiro OpenSnapshot, e vale para
// todas as tabelas. A publicação no feed acontece depois do commit de
// visibilidade, então toda mudança com sequence ≤ cursor já está visível para
// qualquer snapshot aberto depois; o que vier depois do cursor chega pelo
// tail. Ler o cursor depois da varredura deixaria fora do tail uma mudança que
// o bloco já copiado não viu.
//
// Cada bloco abre um snapshot próprio, copia linhas para buffers do exportador
// até chunkRows linhas varridas ou snapshotChunkMaxBytes bytes copiados (o que
// vier primeiro, e sempre pelo menos uma linha), FECHA o snapshot e só então
// chama emit. Um snapshot aberto retém o journal de versões, congela o relógio
// da expiração oportunista e o piso da retenção; emit pode bloquear (rede,
// Kafka) e nunca roda com um deles aberto. Os buffers são reaproveitados entre
// os blocos, e a memória fica limitada a um bloco, em linhas e em bytes.
// Linhas que o snapshot declara mortas contam para o teto em linhas, mas não
// encerram a tabela: ela só acaba quando a varredura se esgota.
//
// O primeiro snapshot de cada tabela fixa o piso (base₀) e o teto (limit₀).
// Os blocos seguintes varrem [max(próximo, piso corrente), limit₀): linhas com
// rowId ≥ limit₀ foram reservadas depois do cursor e chegam pelo tail. Linhas
// abaixo de limit₀ podem sair num estado mais novo que o cursor — o tail
// reaplica as mudanças seguintes por cima, e o estado converge.
//
// Na tabela histórica sai antes das linhas um Retain com Record.RowID = base₀,
// o piso do MESMO snapshot que fixou o teto, e não uma leitura avulsa da
// marca-d'água.
//
// Toda entrada leva Sequence = cursor, At = header.At e Table = nome da tabela;
// Operation é Insert (Retain para o piso do histórico). Key e Payload são
// EMPRESTADOS: valem só durante a chamada de emit, e o bloco seguinte os
// sobrescreve. Os dois saem com a capacidade limitada ao comprimento, então um
// append de emit sobre a Key realoca, e nunca sobrescreve o Payload vizinho.
//
// header.At é o Instant do primeiro snapshot aberto — o relógio do Store,
// e não o do processo. Sem tabela nenhuma não há snapshot, e At é time.Now().
//
// begin é chamado uma vez, antes de qualquer emit — depois do primeiro bloco
// copiado, para que um erro nele ainda possa ser respondido sem nada escrito.
// Erro de begin, de emit, da varredura (inclusive ErrExpiredSnapshot) ou do
// contexto interrompe a exportação e é devolvido; o snapshot aberto é fechado
// em todos os caminhos.
func ExportSnapshot(ctx context.Context, feed *Stream, tables []SnapshotTable, chunkRows int,
	begin func(SnapshotHeader) error, emit func(Entry) error) error {
	if feed == nil || begin == nil || emit == nil || chunkRows < 1 {
		return ErrInvalidSnapshotExport
	}
	epoch, cursor, endPos := feed.Position()
	x := &snapshotExporter{
		header:    SnapshotHeader{Epoch: epoch, Cursor: cursor, EndPos: endPos},
		chunkRows: chunkRows,
		begin:     begin,
		emit:      emit,
	}
	for _, table := range tables {
		if err := x.exportTable(ctx, table); err != nil {
			return err
		}
	}
	if !x.begun {
		x.header.At = time.Now()
		return x.start()
	}
	return nil
}

// snapshotRow é uma linha copiada do bloco corrente; Key e Payload moram no
// arena, por deslocamento, porque o arena pode ser realocado durante a cópia.
type snapshotRow struct {
	rowID      types.RowID
	sequence   int64
	flags      uint8
	keyStart   int
	keyEnd     int
	payloadEnd int
}

// snapshotExporter guarda o estado de UMA exportação. Não é thread-safe.
type snapshotExporter struct {
	header    SnapshotHeader
	chunkRows int
	begin     func(SnapshotHeader) error
	emit      func(Entry) error
	begun     bool

	rows  []snapshotRow
	arena []byte
	buf   []byte
	// next é o rowId em que o próximo bloco da tabela corrente retoma.
	next types.RowID
}

func (x *snapshotExporter) start() error {
	x.begun = true
	return x.begin(x.header)
}

func (x *snapshotExporter) exportTable(ctx context.Context, table SnapshotTable) error {
	var limit types.RowID
	first := true
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		snap := table.Source.OpenSnapshot()
		if first {
			limit = snap.LimitRowID
			x.next = snap.BaseRowID
			if !x.begun {
				x.header.At = snap.Instant
			}
		}
		done, err := x.copyChunk(table.Source, snap, limit)
		if err != nil {
			return fmt.Errorf("replication: snapshot of %s: %w", table.Name, err)
		}
		if !x.begun {
			if err := x.start(); err != nil {
				return err
			}
		}
		if first && table.History {
			if err := x.emit(Entry{
				Sequence: x.header.Cursor, Table: table.Name, Operation: Retain, At: x.header.At,
				Record: types.Record{RowID: snap.BaseRowID},
			}); err != nil {
				return err
			}
		}
		first = false
		for i := range x.rows {
			if err := ctx.Err(); err != nil {
				return err
			}
			row := &x.rows[i]
			if err := x.emit(Entry{
				Sequence: x.header.Cursor, Table: table.Name, Operation: Insert, At: x.header.At,
				Record: types.Record{
					RowID: row.rowID, Sequence: row.sequence, Flags: row.flags,
					Key:     x.arena[row.keyStart:row.keyEnd:row.keyEnd],
					Payload: x.arena[row.keyEnd:row.payloadEnd:row.payloadEnd],
				},
			}); err != nil {
				return err
			}
		}
		if done {
			return nil
		}
	}
}

// resetChunk esvazia os buffers para o próximo bloco, descartando os que uma
// linha gigante fez passar de snapshotArenaKeepBytes.
func (x *snapshotExporter) resetChunk() {
	x.rows = x.rows[:0]
	if cap(x.arena) > snapshotArenaKeepBytes {
		x.arena = nil
	}
	x.arena = x.arena[:0]
	if cap(x.buf) > snapshotArenaKeepBytes {
		x.buf = nil
	}
}

// copyChunk copia um bloco de [x.next, limit) e fecha o snapshot antes de
// voltar, em qualquer caminho. done diz que a varredura da tabela se esgotou.
func (x *snapshotExporter) copyChunk(src SnapshotSource, snap types.Snapshot, limit types.RowID) (done bool, err error) {
	defer src.CloseSnapshot(snap)
	it := src.ScanLiveFrom(snap, x.next, limit)
	// O Close do iterador de linhas vivas não libera nada que o
	// CloseSnapshot já não libere; o erro da varredura vem do Err().
	defer func() { _ = it.Close() }()
	x.resetChunk()
	scanned := 0
	exhausted := false
	for scanned < x.chunkRows && len(x.arena) < snapshotChunkMaxBytes {
		if !it.Next() {
			exhausted = true
			break
		}
		id := it.RowID()
		rec, buf, live, err := src.GetByRowID(snap, id, x.buf)
		x.buf = buf
		if err != nil {
			return false, err
		}
		scanned++
		// O próximo bloco retoma depois da última linha VARRIDA, viva ou não:
		// avançar só nas vivas faria um bloco inteiro de mortas se repetir
		// para sempre.
		x.next = id + 1
		if !live {
			continue
		}
		start := len(x.arena)
		x.arena = append(x.arena, rec.Key...)
		keyEnd := len(x.arena)
		x.arena = append(x.arena, rec.Payload...)
		x.rows = append(x.rows, snapshotRow{
			rowID: rec.RowID, sequence: rec.Sequence, flags: rec.Flags,
			keyStart: start, keyEnd: keyEnd, payloadEnd: len(x.arena),
		})
	}
	if err := it.Err(); err != nil {
		return false, err
	}
	return exhausted, nil
}
