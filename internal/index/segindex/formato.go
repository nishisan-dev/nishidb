// Package segindex implementa o índice secundário POR SEGMENTO do NishiDB:
// valor -> conjunto ordenado de rowIds dentro de um segmento.
//
// Ele substitui os dois motores globais (hash e bitmap) por um só, e a
// unificação não é economia de código: POR SEGMENTO a diferença entre os dois
// some. Os dois são "valor -> conjunto ordenado de rowIds", e densidade vira
// decisão de codificação, não de motor.
//
// # Duas formas
//
// A forma WIP (wip.go) é mutável e existe para o segmento CORRENTE apenas:
// diretório open-addressing sobre valores distintos, mais um array de postings
// encadeado para trás. Inserir é uma sondagem amortizada e duas escritas —
// O(1), que é o defeito que este pacote existe para corrigir.
//
// A forma selada (este arquivo, escritor.go e selado.go) é IMUTÁVEL e cobre
// todo segmento já rotacionado: diretório ordenado com busca binária e postings
// contíguas por valor. Imutável compra três coisas que a WIP não tem: não há
// load factor a respeitar (o diretório tem exatamente D entradas), o Lookup
// devolve uma fatia do mmap sem copiar nada, e o RWMutex do mmap.Region deixa
// de importar — ele existe para proteger contra o remap do Grow, e um arquivo
// selado nunca cresce.
package segindex

import (
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// Layout da forma SELADA, little-endian (arquivo data/segment-%08d.sidx):
//
//	0   4  magic "NDBX"
//	4   1  version = 1
//	5   1  flags (reservado, 0)
//	6   2  secCount u16
//	8   8  segmentID u64
//	16  8  baseRowID u64   — o MENOR rowId do segmento; base dos deltas
//	24  8  maxRowID u64
//	32  1  postingWidth u8 — 4 ou 8
//	33  31 reservado
//	64  .. tabela de seções: secCount x 24 bytes, ORDENADA por fieldID
//	          fieldID u16 | pad u16 | distintos u32 | dirOff u64 | postOff u64
//	    .. por seção, na ordem da tabela:
//	          diretório: distintos x 16 bytes, ORDENADO por hash
//	              hash u64 | postIdx u32 | count u32
//	          postings: array de deltas de postingWidth bytes, contíguo por
//	              valor e ascendente dentro do grupo
//	EOF-4  crc32c u32 sobre [0, EOF-4)
//
// postIdx é o ÍNDICE do elemento dentro do array de postings da SEÇÃO — não
// offset em bytes. O endereço do primeiro delta do valor é
// `postOff + postIdx * postingWidth`. A distinção importa porque as duas
// leituras nunca estouram limite nenhum: um leitor que tratasse postIdx como
// offset em bytes andaria para DENTRO do array de deltas (por um fator igual
// a postingWidth), leria lixo de outro valor como se fosse rowId, e nada no
// formato acusaria — sem erro, sem crash, só resposta errada.
//
// O crc fica no FIM e cobre tudo antes dele, que é o idioma que o manifest já
// usa (checkpoint/manifest.go). Cobrir o header inclusive é o ponto: um header
// corrompido que passasse pelo crc levaria a busca binária a offsets
// arbitrários dentro do mapeamento.
//
// NÃO existe campo minRowID: baseRowID É o menor rowId da seção. A mesma
// informação em dois lugares é uma chance de divergirem, e quem divergisse
// produziria delta negativo — que em u32 vira um rowId gigante e um falso
// NEGATIVO silencioso, o único desfecho que este produto não tolera.
const (
	// MagicSelado identifica o arquivo selado.
	MagicSelado = "NDBX"
	// VersaoSelado é a versão do formato selado.
	VersaoSelado = 1
	// HeaderSelado é o tamanho do header em bytes.
	HeaderSelado = 64
	// TamSecao é o tamanho de uma entrada da tabela de seções.
	TamSecao = 24
	// TamSlotDir é o tamanho de uma entrada de diretório.
	TamSlotDir = 16
	// TamCRC é o crc32c no fim do arquivo.
	TamCRC = 4
)

var (
	// ErrFormato é a família de erros de arquivo de índice ilegível.
	ErrFormato = errors.New("segindex: formato inválido")
	// ErrCRC marca divergência de checksum. É SEPARADO do ErrFormato porque o
	// chamador reage diferente: formato inválido é bug ou versão, e para;
	// crc divergente é corrupção de mídia, e o remédio é apagar e reconstruir.
	ErrCRC = errors.New("segindex: crc divergente")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// sufixoSelado é a extensão da forma selada, e ela é constante porque TRÊS
// lugares precisam concordar com ela: o caminho do arquivo (CaminhoSelado), o
// nome do temporário que o publica (caminhoTemporario) e o predicado que conta o
// disco do dataset (EhArquivoDeIndice).
//
// A terceira concordância é a que quebraria em SILÊNCIO. O temporário deixou de
// ter nome fixo (§2.4, Tarefa 14) e passou a carregar um número no meio do nome;
// se ele tivesse deixado de casar com EhArquivoDeIndice, sairia da conta do
// IndexBytes e viraria disco que nada explica — que é exatamente o modo de falha
// que aquela contagem existe para eliminar.
const sufixoSelado = ".sidx"

// CaminhoSelado devolve o caminho do índice selado do segmento dentro de dir.
// Ele mora ao lado do .dat de propósito: a aposentadoria remove os dois no
// mesmo passo, e é isso que faz "purgar o segmento leva o índice junto" ser
// uma linha de código em vez de uma promessa.
func CaminhoSelado(dir string, seg uint32) string {
	return filepath.Join(dir, fmt.Sprintf("segment-%08d%s", seg, sufixoSelado))
}

// SegmentosComSelado devolve, em ordem crescente, os segmentos que têm .sidx
// no diretório — a mesma pergunta do SegmentosComWIP, para a forma selada.
//
// Existe para a limpeza de órfãos da abertura (spec §2.3): um crash entre o
// unlink do .dat e a remoção do índice deixa .sidx de um segmento que não
// existe mais, e nenhuma enumeração que parta dos dados o alcança. Nome no
// espaço .sidx que não é o canônico é ErrFormato, como no espaço .widx: a
// limpeza do lixo não pode adivinhar o que um arquivo desconhecido significa.
func SegmentosComSelado(dir string) ([]uint32, error) {
	entradas, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("segindex: listar os %s de %s: %w", sufixoSelado, dir, err)
	}
	var segs []uint32
	for _, e := range entradas {
		if e.IsDir() || !strings.HasSuffix(e.Name(), sufixoSelado) {
			continue
		}
		miolo := strings.TrimSuffix(strings.TrimPrefix(e.Name(), "segment-"), sufixoSelado)
		seg, err := strconv.ParseUint(miolo, 10, 32)
		if err != nil || e.Name() != filepath.Base(CaminhoSelado("", uint32(seg))) {
			return nil, fmt.Errorf("%w: %s não é um nome canônico de índice selado", ErrFormato, e.Name())
		}
		segs = append(segs, uint32(seg))
	}
	slices.Sort(segs)
	return segs, nil
}

// EhArquivoDeIndice reporta se o nome pertence ao espaço de nomes DESTE pacote
// dentro do diretório de dados: a forma selada, o temporário dela e a forma WIP.
//
// Ela é exportada para quem CONTA o disco do dataset, e mora aqui pela mesma
// razão que ListaWIP mora aqui: o nome é deste pacote. Uma segunda cópia da
// convenção em internal/storage pararia de contar em SILÊNCIO no dia em que a
// extensão mudasse, e a série que ela alimenta existe justamente para que
// nenhum byte do dataset fique sem explicação.
//
// Ela não valida conteúdo, de propósito: um arquivo com o nome certo e bytes
// podres é recusado na abertura e continua ocupando disco até ser removido — que
// é exatamente o que quem conta precisa enxergar.
func EhArquivoDeIndice(nome string) bool {
	return strings.HasSuffix(nome, sufixoSelado) ||
		EhTemporarioDeIndice(nome) ||
		strings.HasSuffix(nome, ".widx")
}

// EhTemporarioDeIndice reporta se o nome é um temporário da forma selada — o
// arquivo intermediário do .tmp + fsync + rename, que desde a §2.4 tem nome
// ÚNICO POR ESCRITOR (ver caminhoTemporario).
//
// Ela é exportada porque quem LIMPA os temporários de uma execução anterior é a
// abertura do dataset, em internal/storage, e a convenção de nome é deste
// pacote: uma segunda cópia dela em cima pararia de casar em silêncio no dia em
// que o formato do nome mudasse, e o resíduo passaria a ficar em disco para
// sempre.
func EhTemporarioDeIndice(nome string) bool {
	return strings.HasSuffix(nome, sufixoSelado+".tmp")
}

// larguraPara devolve a menor largura de posting que cobre o intervalo.
func larguraPara(span uint64) uint8 {
	if span < 1<<32 {
		return 4
	}
	return 8
}
