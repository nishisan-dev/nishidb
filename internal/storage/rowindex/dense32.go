package rowindex

import "github.com/nishisan-dev/nishidb/internal/types"

// Denso32 é um vetor denso de 4 bytes por rowId sobre mmap, com a mesma base
// (header, doubling, sentinela de nascimento) dos vetores de 64 deste pacote.
//
// Existe para o vetor de grupos do contador mantido na escrita: uma entrada de
// 64 bits para guardar um id de grupo e um bit de condição dobraria o custo
// residente do índice sem guardar nada a mais — 8M linhas são 32 MB em vez de
// 64 por contador, e o contador é o único índice do produto que pode existir
// várias vezes sobre o mesmo dataset.
//
// A entrada é OPACA para este pacote: quem decide o que os 32 bits significam
// é o dono do vetor. O único contrato é que zero é o valor de uma entrada nunca
// gravada, porque é o que o fallocate deixa no arquivo.
type Denso32 struct {
	dense *denseFile
}

// OpenDenso32 cria/reabre o arquivo com capacidade para minRows. Um arquivo
// gravado com entradas de outro tamanho é recusado pelo byte 5 do header: um
// vetor de 64 lido como 32 endereçaria a entrada errada em toda linha, e o
// sintoma seria número silenciosamente errado, não erro.
func OpenDenso32(path string, magic [4]byte, minRows uint64) (*Denso32, error) {
	d, err := openDenseCom(path, magic, minRows, entrySize32)
	if err != nil {
		return nil, err
	}
	return &Denso32{dense: d}, nil
}

// Get devolve a entrada do rowId, e gravado=false quando o rowId está além do
// arquivo. Nunca cresce na leitura.
func (d *Denso32) Get(rowID types.RowID) (v uint32, gravado bool) {
	return d.dense.load32(rowID)
}

// Set grava a entrada, crescendo o arquivo (doubling) se necessário. Exclusivo
// do writer.
func (d *Denso32) Set(rowID types.RowID, v uint32) error {
	return d.dense.store32(rowID, v)
}

// Nasceu diz se ESTA abertura criou o arquivo — o sinal que separa "vetor
// ainda não existia" de "vetor existe e está zerado".
func (d *Denso32) Nasceu() bool { return d.dense.nasceu }

// Cobre diz se o arquivo tem entrada para toda linha abaixo de limite. É a
// pergunta que o manifest não responde: ele declara os parâmetros do vetor,
// não a existência dele.
func (d *Denso32) Cobre(limite types.RowID) bool {
	if limite == 0 {
		return true
	}
	return d.dense.off(limite-1)+entrySize32 <= d.dense.region.Size()
}

// Entradas devolve quantas entradas o arquivo comporta hoje — o limite de uma
// varredura pelo tamanho do arquivo, para quem precisa dobrar o vetor antes de
// saber quantas linhas o dataset tem.
func (d *Denso32) Entradas() uint64 {
	n := d.dense.region.Size() - headerSize
	if n <= 0 {
		return 0
	}
	return uint64(n / entrySize32)
}

// Zera apaga todas as entradas, mantendo header e tamanho. É a reconstrução
// começando do zero; roda em blocos sob o RLock da região, sem alocar o
// arquivo inteiro.
func (d *Denso32) Zera() error {
	const bloco = 1 << 20
	total := d.dense.region.Size()
	for off := int64(headerSize); off < total; off += bloco {
		n := min(int64(bloco), total-off)
		d.dense.region.WithBytes(int(off), int(n), func(b []byte) { clear(b) })
	}
	return nil
}

// Msync força as páginas sujas para disco.
func (d *Denso32) Msync() error { return d.dense.region.Msync() }

// SizeBytes devolve o tamanho do arquivo mapeado.
func (d *Denso32) SizeBytes() int64 { return d.dense.region.Size() }

// Close libera o mapeamento.
func (d *Denso32) Close() error { return d.dense.region.Close() }
