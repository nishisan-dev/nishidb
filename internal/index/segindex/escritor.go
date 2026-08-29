package segindex

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// entrada é uma posting ainda não ordenada. 24 bytes, e a ordem dos campos é
// para o alinhamento — 224 mil linhas por segmento vezes o número de campos
// indexados vivem aqui durante a selagem.
type entrada struct {
	hash  uint64
	row   types.RowID
	campo types.FieldID
}

// Escritor acumula postings e emite o arquivo selado. É o ÚNICO lugar que
// decide o layout selado: a selagem do WIP e a reconstrução a partir dos
// records alimentam ESTE tipo, e é isso que torna "incremental idêntico a
// reconstruído" verdade por construção em vez de por vigilância.
//
// Não é seguro para uso concorrente. Cada selagem ou reconstrução usa o seu.
type Escritor struct {
	ents []entrada
}

// NovoEscritor devolve um escritor vazio.
func NovoEscritor() *Escritor { return &Escritor{} }

// Adiciona registra a posting. Duplicatas e desordem são aceitas: o Escreve
// ordena e deduplica.
func (e *Escritor) Adiciona(campo types.FieldID, h uint64, row types.RowID) {
	e.ents = append(e.ents, entrada{hash: h, row: row, campo: campo})
}

// Vazio reporta que nada foi adicionado.
func (e *Escritor) Vazio() bool { return len(e.ents) == 0 }

// Escreve emite o arquivo em caminho, por .tmp + fsync + rename + fsync do
// diretório. Escritor vazio NÃO cria arquivo: "sem índice" é um estado que o
// Lookup trata por enumeração, e um arquivo vazio faria a reconstrução
// considerar o segmento pronto.
func (e *Escritor) Escreve(caminho string, seg uint32) error {
	if e.Vazio() {
		return nil
	}
	slices.SortFunc(e.ents, func(a, b entrada) int {
		if c := cmp.Compare(a.campo, b.campo); c != 0 {
			return c
		}
		if c := cmp.Compare(a.hash, b.hash); c != 0 {
			return c
		}
		return cmp.Compare(a.row, b.row)
	})
	e.ents = slices.Compact(e.ents)

	base, maxRow := e.ents[0].row, e.ents[0].row
	for _, en := range e.ents {
		base = min(base, en.row)
		maxRow = max(maxRow, en.row)
	}
	largura := larguraPara(uint64(maxRow - base))

	corpo := e.montaCorpo(base, largura)

	buf := make([]byte, 0, HeaderSelado+len(corpo)+TamCRC)
	buf = append(buf, MagicSelado...)
	buf = append(buf, VersaoSelado, 0)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(e.contaCampos()))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(seg))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(base))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(maxRow))
	buf = append(buf, largura)
	buf = append(buf, make([]byte, HeaderSelado-len(buf))...)
	buf = append(buf, corpo...)
	buf = binary.LittleEndian.AppendUint32(buf, crc32.Checksum(buf, castagnoli))

	return escreveAtomico(caminho, buf)
}

// contaCampos conta os fieldIDs distintos em e.ents. Chamado só depois da
// ordenação em Escreve, então uma corrida do mesmo campo é sempre contígua.
func (e *Escritor) contaCampos() int {
	n := 0
	var atual types.FieldID
	for i, en := range e.ents {
		if i == 0 || en.campo != atual {
			n++
			atual = en.campo
		}
	}
	return n
}

// secaoInfo é o resultado da primeira passada de montaCorpo: quanto uma
// seção (um fieldID) vai ocupar, sem ainda saber ONDE — a segunda passada
// resolve os offsets absolutos a partir destes tamanhos.
type secaoInfo struct {
	campo     types.FieldID
	distintos uint32
	postings  uint32
}

// montaCorpo escreve a tabela de seções, os diretórios e as postings a partir
// de e.ents — já ordenado por (campo, hash, row) e deduplicado por Escreve —
// em DUAS passadas.
//
// A primeira passada só CONTA: quantas seções (fieldIDs distintos), quantos
// valores distintos (grupos de hash) e quantas postings cada seção tem. É o
// que permite alocar o slice do corpo com a capacidade exata de uma vez, e
// também é o que permite calcular, ANTES de escrever um byte, o offset
// absoluto de cada diretório e de cada array de postings — porque o tamanho
// de uma seção só é conhecido depois de somar as duas primeiras.
//
// A segunda passada escreve a tabela de seções e, para cada seção na mesma
// ordem, o diretório (ordenado por hash, porque e.ents já está) e as
// postings (deltas contíguos por valor, ascendentes dentro do grupo, porque
// e.ents já está ordenado por row dentro do grupo). dirOff e postOff são
// ABSOLUTOS a partir do início do arquivo selado — o leitor (Tarefa 2) fatia
// o mmap direto por eles, sem recompor uma base a cada Lookup.
func (e *Escritor) montaCorpo(base types.RowID, largura uint8) []byte {
	// --- primeira passada: dimensiona ---
	var secoes []secaoInfo
	for i := 0; i < len(e.ents); {
		campo := e.ents[i].campo
		inicioSecao := i
		var distintos uint32
		for i < len(e.ents) && e.ents[i].campo == campo {
			hashAtual := e.ents[i].hash
			distintos++
			for i < len(e.ents) && e.ents[i].campo == campo && e.ents[i].hash == hashAtual {
				i++
			}
		}
		secoes = append(secoes, secaoInfo{
			campo:     campo,
			distintos: distintos,
			postings:  uint32(i - inicioSecao),
		})
	}

	tamTabela := len(secoes) * TamSecao
	tamCorpo := tamTabela
	for _, s := range secoes {
		tamCorpo += int(s.distintos)*TamSlotDir + int(s.postings)*int(largura)
	}

	// Offsets absolutos: o corpo começa logo após o header fixo.
	dirOffs := make([]uint64, len(secoes))
	postOffs := make([]uint64, len(secoes))
	off := uint64(HeaderSelado) + uint64(tamTabela)
	for i, s := range secoes {
		dirOffs[i] = off
		off += uint64(s.distintos) * TamSlotDir
		postOffs[i] = off
		off += uint64(s.postings) * uint64(largura)
	}

	// --- segunda passada: escreve ---
	corpo := make([]byte, 0, tamCorpo)
	for i, s := range secoes {
		corpo = binary.LittleEndian.AppendUint16(corpo, uint16(s.campo))
		corpo = binary.LittleEndian.AppendUint16(corpo, 0) // pad
		corpo = binary.LittleEndian.AppendUint32(corpo, s.distintos)
		corpo = binary.LittleEndian.AppendUint64(corpo, dirOffs[i])
		corpo = binary.LittleEndian.AppendUint64(corpo, postOffs[i])
	}

	i := 0
	for _, s := range secoes {
		fimSecao := i + int(s.postings)

		// diretório: um slot por valor distinto, ordenado por hash porque
		// e.ents já está.
		var postIdx uint32
		j := i
		for j < fimSecao {
			hashAtual := e.ents[j].hash
			inicioValor := j
			for j < fimSecao && e.ents[j].hash == hashAtual {
				j++
			}
			count := uint32(j - inicioValor)
			corpo = binary.LittleEndian.AppendUint64(corpo, hashAtual)
			corpo = binary.LittleEndian.AppendUint32(corpo, postIdx)
			corpo = binary.LittleEndian.AppendUint32(corpo, count)
			postIdx += count
		}

		// postings: deltas de largura bytes, contíguos por valor e
		// ascendentes dentro do grupo (e.ents já está ordenado por row ali).
		for k := i; k < fimSecao; k++ {
			delta := uint64(e.ents[k].row - base)
			if largura == 4 {
				corpo = binary.LittleEndian.AppendUint32(corpo, uint32(delta))
			} else {
				corpo = binary.LittleEndian.AppendUint64(corpo, delta)
			}
		}

		i = fimSecao
	}

	return corpo
}

// escreveAtomico grava tmp -> fsync -> rename -> fsync do diretório: a mesma
// política de rename durável do design §9/M18 que checkpoint/manifest.go já
// usa, replicada aqui — e não importada de lá — porque segindex vive sob
// internal/index e não tem por que depender de internal/storage/checkpoint;
// os outros motores de índice (hash, primary) fazem o mesmo.
//
// Sem isto o arquivo pode sobreviver a um crash com o nome `.tmp`, e o Open
// do leitor (Tarefa 2) o trataria como órfão em vez de como o índice do
// segmento.
func escreveAtomico(caminho string, dados []byte) error {
	tmp := caminho + ".tmp"
	if err := os.WriteFile(tmp, dados, 0o644); err != nil {
		return fmt.Errorf("segindex: escrever %s: %w", tmp, err)
	}
	f, err := os.OpenFile(tmp, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("segindex: reabrir %s para fsync: %w", tmp, err)
	}
	err = f.Sync()
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("segindex: fsync de %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, caminho); err != nil {
		return fmt.Errorf("segindex: rename de %s: %w", filepath.Base(caminho), err)
	}
	return syncDir(filepath.Dir(caminho))
}

// syncDir força a durabilidade da entrada de diretório após um rename.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	// O erro do Close entra no resultado em vez de ir para um defer mudo:
	// num diretório recém-sincronizado ele é o mesmo writeback que o Sync
	// tenta forçar, e engoli-lo aqui deixaria a escrita se declarar durável
	// sobre uma entrada que não chegou ao disco.
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("segindex: fsync do diretório %s: %w", dir, err)
	}
	return nil
}
