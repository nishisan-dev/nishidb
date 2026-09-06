package segindex

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"

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

	// corpo e secCount vêm da MESMA travessia de e.ents: secCount é o campo
	// que diz ao leitor quantas entradas de 24 bytes da tabela de seções
	// existem, e uma segunda contagem independente — por mais que hoje dê o
	// mesmo número — seria uma segunda fonte de verdade para exatamente o
	// dado que não pode divergir (a mesma razão de format.go não ter
	// minRowID: aqui a consequência de divergir é o leitor caminhar para
	// dentro dos bytes do diretório, não só uma inconsistência cosmética).
	corpo, secCount := e.montaCorpo(base, largura)

	buf := make([]byte, 0, HeaderSelado+len(corpo)+TamCRC)
	buf = append(buf, MagicSelado...)
	buf = append(buf, VersaoSelado, 0)
	// O truncamento do uint16 é inalcançável: secCount é o número de campos
	// com posting, e o fielddict tem teto de 65.535 campos — o formato herda o
	// teto da fonte dos FieldIDs.
	buf = binary.LittleEndian.AppendUint16(buf, uint16(secCount))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(seg))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(base))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(maxRow))
	buf = append(buf, largura)
	buf = append(buf, make([]byte, HeaderSelado-len(buf))...)
	buf = append(buf, corpo...)
	buf = binary.LittleEndian.AppendUint32(buf, crc32.Checksum(buf, castagnoli))

	return escreveAtomico(caminho, buf)
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
//
// O segundo retorno é len(secoes): o número de seções que Escreve grava como
// secCount no header. Ele sai desta MESMA travessia de e.ents em vez de uma
// contagem própria em Escreve porque secCount é o campo que diz ao leitor
// quantas entradas de 24 bytes da tabela de seções existem — um segundo
// código contando "fieldIDs distintos" por conta própria seria uma segunda
// fonte de verdade para o mesmo número, e se um dia as duas contagens
// discordassem o leitor caminharia para dentro dos bytes do diretório sem
// erro nenhum no meio. É a mesma razão de não existir minRowID.
func (e *Escritor) montaCorpo(base types.RowID, largura uint8) ([]byte, int) {
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

	return corpo, len(secoes)
}

// numeroDoTemporario numera as escritas de índice selado DESTE processo. Um
// atômico, e não um nome sorteado, porque o que se precisa aqui é apenas que
// dois escritores VIVOS ao mesmo tempo não colidam: o dataset tem flock
// exclusivo por processo, então não há um terceiro escritor para desempatar.
//
// Reiniciar em zero a cada subida é PROPRIEDADE, e não descuido: um temporário
// que sobreviva a um crash volta a ser um nome alcançável pela próxima execução,
// em vez de virar lixo de nome único que só cresce. A limpeza da abertura
// (ApagaTemporarios) é quem o remove de fato; isto é só o que impede o resíduo
// de ser ilimitado se aquela limpeza um dia falhar.
var numeroDoTemporario atomic.Uint64

// caminhoTemporario devolve o arquivo intermediário de UMA escrita, com nome
// único por escritor.
//
// O NOME FIXO ANTERIOR ERA UM DEFEITO, e ele é o segundo dos três fatos que a
// §2.4 emendada põe no lugar do wmu na publicação do índice. O aposAppend troca
// o segmento CORRENTE antes de selar o anterior (storage/indices.go), então
// existe uma janela em que o segmento recém-rotacionado aparece na fila de
// reconstrução: a selagem e a reconstrução escreviam no MESMO `<sidx>.tmp` e
// produziam um arquivo rasgado — inclusive o .sidx de ZERO BYTES que o
// mmap.ErrArquivoVazio existe para tolerar. Com nomes distintos as duas escritas
// não se tocam, e a corrida que sobra é no nome FINAL, que é neutra: o rename é
// atômico e os dois caminhos produzem arquivos byte a byte idênticos (golden da
// §6.3), então quem renomeia por último grava os mesmos bytes.
//
// O número vai no MEIO do nome, e não no fim, para que ele continue terminando
// em ".sidx.tmp": é assim que EhArquivoDeIndice segue contando estes bytes no
// IndexBytes e que a limpeza da abertura segue os encontrando.
func caminhoTemporario(caminho string) string {
	return fmt.Sprintf("%s.%d%s.tmp", strings.TrimSuffix(caminho, sufixoSelado), numeroDoTemporario.Add(1), sufixoSelado)
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
//
// O TEMPORÁRIO QUE NÃO CHEGOU A SER RENOMEADO É REMOVIDO AQUI MESMO, e isso
// passou a ser obrigatório junto com o nome único: com o nome fixo, a escrita
// seguinte truncava o resíduo por cima e o disco voltava sozinho; com um nome
// por escritor, cada falha entre o WriteFile e o rename deixaria um arquivo que
// nada mais alcança. O que a remoção não cobre é o processo MORTO no meio — esse
// é da ApagaTemporarios, na abertura.
func escreveAtomico(caminho string, dados []byte) error {
	tmp := caminhoTemporario(caminho)
	renomeou := false
	defer func() {
		if !renomeou {
			_ = os.Remove(tmp)
		}
	}()
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
	renomeou = true
	return syncDir(filepath.Dir(caminho))
}

// ApagaTemporarios remove os temporários de índice selado que sobraram de uma
// execução anterior. Roda na ABERTURA do dataset, junto do descarte dos .widx, e
// é a contrapartida do nome único: um processo morto entre o WriteFile e o
// rename deixa em disco um arquivo que nenhuma escrita futura vai reaproveitar.
//
// Nesse instante ela é sempre segura: o flock do dataset é exclusivo por
// processo e a abertura acontece antes de qualquer escritor deste processo
// existir, então todo `*.sidx.tmp` que estiver ali é, por construção, de um
// processo que já morreu.
//
// Arquivo que some entre a listagem e o unlink não é anomalia: é o mesmo estado
// que a remoção pretendia produzir.
func ApagaTemporarios(dir string) error {
	entradas, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("segindex: listar os temporários de %s: %w", dir, err)
	}
	for _, e := range entradas {
		if e.IsDir() || !EhTemporarioDeIndice(e.Name()) {
			continue
		}
		caminho := filepath.Join(dir, e.Name())
		if err := os.Remove(caminho); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("segindex: remover o temporário %s: %w", caminho, err)
		}
	}
	return nil
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
