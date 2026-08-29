package storage

import (
	"context"
	"fmt"
	"slices"

	"github.com/nishisan-dev/nishidb/internal/index/primary"
	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/storage/segment"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// ConstroiIndiceDoSegmento deriva o índice SELADO de um segmento a partir dos
// records dele, e o publica pelo mesmo .tmp + fsync + rename da selagem.
//
// Ela é o mecanismo da §2.4, e só ele: o laço de fundo, o pacing contra o
// orçamento de ciclo de trabalho e as métricas são de quem a chama. Aqui não há
// goroutine, e o ctx existe para que o chamador possa desistir no meio de uma
// varredura que pode custar 512 MiB de leitura.
//
// A construção NÃO consulta locations.idx, NÃO filtra tombstone e NÃO decide
// liveness. Isso não é economia: o contrato do índice é SUPERCONJUNTO de
// candidatos, com o executor revalidando o WHERE inteiro (§44), e filtrar aqui
// criaria uma SEGUNDA definição de "vivo" — a desta função — que divergiria da
// do executor no primeiro dia em que uma das duas mudasse. A divergência que ela
// produziria é a única que o produto não tolera: candidato a menos é falso
// negativo, e o §44 só sabe descartar candidato a mais.
//
// O arquivo sai igual ao da selagem por CONSTRUÇÃO, porque os dois caminhos
// alimentam o mesmo segindex.Escritor — é ele quem ordena, deduplica, re-baseia
// e escolhe a largura da posting. O que o golden
// (TestConstruirDoSegmentoDaOMesmoArquivoQueSelarOWIP) prova é a outra metade: que
// a ALIMENTAÇÃO é equivalente, que é onde o erro moraria.
//
// Segmento sem posting nenhuma não gera arquivo (contrato de Escritor.Escreve):
// "sem índice" é um estado que o Lookup resolve por enumeração, e um .sidx vazio
// faria o segmento passar por indexado.
//
// A PUBLICAÇÃO NÃO TOMA O wmu, e a §2.4 emendada é explícita quanto a isso: a
// exigência anterior — tomá-lo "só no rename" — era inexequível, porque
// Escritor.Escreve faz escrita-tmp, fsync, rename e fsync do diretório numa
// chamada só, e segurar o lock pela escrita inteira é o que a mesma seção
// proíbe. O que substitui o lock são três fatos: o rename já é atômico, o
// temporário passa a ter nome único por escritor (é da Tarefa 14, junto com o
// laço — hoje ele ainda é fixo), e a selagem e a reconstrução produzem arquivos
// BYTE A BYTE IDÊNTICOS, o que o golden prova. Com a igualdade provada, uma
// corrida no nome final é neutra em conteúdo: quem renomear por último grava os
// mesmos bytes.
//
// O QUE O CHAMADOR AINDA PRECISA GARANTIR, e é concreto: esta função lê pelo
// s.segR, e a aposentadoria fecha o descritor por segR.FechaSegmento, que exige
// que nenhuma leitura esteja em voo — barreira que no Store é o aposMu. O Close
// fecha o segR inteiro. Um laço de fundo que varra sem o mesmo par claim/Add que
// a compaction usa colhe os.ErrClosed no meio da varredura, e o resíduo aceito
// da §2.4 (publicar o .sidx de um segmento recém-aposentado) vira erro em vez de
// órfão recuperável.
func (s *Store) ConstroiIndiceDoSegmento(ctx context.Context, seg uint32, campos []types.FieldID) error {
	if len(campos) == 0 {
		return nil
	}
	// O conjunto tem de ser o do dataset INTEIRO, e a recusa fecha o falso
	// negativo petrificado por uma segunda porta. Um subconjunto gravaria um
	// .sidx IMUTÁVEL e PARCIAL — e como a pendência é decidida por "tem .sidx?"
	// (SegmentosPendentesDeReconstrucao), o segmento passaria a contar como
	// pronto com campos que ninguém indexou: falso negativo permanente, sem erro
	// e sem log.
	//
	// A TENSÃO COM A §2.4 FICA ESCRITA, porque ela é real: lá a unidade de
	// trabalho é (segmento, campo), para que a §2.6 possa acrescentar um campo
	// ao schema de um dataset povoado. Quando essa unidade existir de fato, é o
	// TESTE DE PENDÊNCIA que precisa mudar primeiro — de "tem .sidx?" para "o
	// .sidx tem seção para cada campo indexado?" —, e só então esta recusa pode
	// afrouxar. Afrouxá-la antes trocaria uma recusa alta por um índice parcial
	// que se declara completo.
	if !slices.Equal(campos, s.camposIndexados) {
		return fmt.Errorf("%w: reconstrução do segmento %d pedida para %v, mas o dataset indexa %v; "+
			"o .sidx é imutável e um parcial passaria por pronto",
			ErrInvalidArgument, seg, campos, s.camposIndexados)
	}
	// O segmento CORRENTE é recusado, e a guarda não é defensiva: o .sidx é
	// IMUTÁVEL, e gravá-lo para um segmento que ainda recebe records
	// petrificaria um índice sem tudo que vier depois. Falso negativo
	// permanente, que é exatamente o desfecho que esta tarefa existe para
	// eliminar. O índice do corrente é o WIP, e ele se reconstrói na abertura.
	if corrente := s.curSegID.Load(); seg == corrente {
		return fmt.Errorf("%w: o segmento %d é o corrente; o índice dele é o WIP, não um .sidx imutável",
			ErrInvalidArgument, seg)
	}
	e := segindex.NovoEscritor()
	if err := s.varreOsRecordsDoSegmento(ctx, seg, campos,
		func(campo types.FieldID, h uint64, row types.RowID) error {
			e.Adiciona(campo, h, row)
			return nil
		}); err != nil {
		return err
	}
	return e.Escreve(segindex.CaminhoSelado(s.dirDeDados(), seg), seg)
}

// SegmentosPendentesDeReconstrucao devolve, em ordem crescente, os segmentos que
// têm dado e não têm índice selado.
//
// A fila é DERIVADA do disco a cada chamada, e não uma lista persistida, pela
// mesma razão que o índice é derivado dos records: um metadado a menos é uma
// janela de crash a menos. Uma fila em arquivo teria de ser durabilizada em
// ordem contra a selagem e contra a aposentadoria, e uma fila em memória se
// perderia no restart — que é justamente quando ela é mais necessária.
//
// Quem entra é o segmento com .dat e sem .sidx, exceto o corrente: o índice do
// corrente é o WIP, que a abertura reconstrói antes do primeiro Put. Um .widx
// de segmento sem .dat nunca aparece aqui porque a enumeração parte dos DADOS —
// enfileirá-lo seria mandar a reconstrução varrer um arquivo que não existe,
// para sempre. A remoção dele é da abertura (§2.5, terceiro caso).
//
// Dataset sem campo indexado devolve fila vazia: não há o que reconstruir.
//
// DOS QUATRO GATILHOS DA §2.4, ESTA FUNÇÃO COBRE UM: o formato novo sobre
// dataset antigo (migração), que é "tem dado e não tem .sidx". Os outros três
// ainda não têm resposta aqui, e o nome da função promete mais do que ela
// entrega até que tenham — a janela alargada depende da janela existir; o crc32c
// divergente (§1.5) só é conhecido na primeira consulta da seção, e quem o
// detectar precisa apagar o arquivo para o segmento voltar a ser pendente por
// esta mesma regra; e o campo acrescentado ao schema (§2.6) exige o teste de
// pendência por SEÇÃO discutido no godoc de ConstroiIndiceDoSegmento.
//
// DUAS ARESTAS que quem montar o laço de fundo (§2.4) precisa tratar, e que
// ficam escritas aqui porque nenhuma delas é visível de dentro desta função:
//
//   - A resposta é um instantâneo, e o segmento recém-rotacionado aparece nela
//     por uma janela. O aposAppend troca o curSegID ANTES de selar o anterior
//     (indices.go), então entre as duas coisas o anterior tem dado e ainda não
//     tem .sidx. A corrida no nome FINAL é neutra — a igualdade byte a byte é
//     provada pelo golden —, mas hoje os dois caminhos escreveriam no MESMO
//     `<sidx>.tmp`, que ainda é nome FIXO (segindex/escritor.go), e o arquivo
//     sairia rasgado. É esta a correção que a §2.4 emendada põe na Tarefa 14:
//     nome de temporário único por escritor, e não exclusão mútua.
//   - Segmento cujos records não têm campo indexado nenhum NUNCA sai da fila:
//     o Escritor vazio não cria arquivo, de propósito, e a pergunta "tem .sidx?"
//     continua respondendo não. O laço não pode tratar a fila como trabalho que
//     encolhe a cada passada.
func (s *Store) SegmentosPendentesDeReconstrucao() ([]uint32, error) {
	if len(s.camposIndexados) == 0 {
		return nil, nil
	}
	dir := s.dirDeDados()
	refs, err := segment.ListSegments(dir)
	if err != nil {
		return nil, err
	}
	corrente := s.curSegID.Load()
	var pendentes []uint32
	for _, ref := range refs {
		if ref.ID == corrente {
			continue
		}
		presente, err := arquivoExiste(segindex.CaminhoSelado(dir, ref.ID))
		if err != nil {
			return nil, err
		}
		if !presente {
			pendentes = append(pendentes, ref.ID)
		}
	}
	return pendentes, nil // ListSegments já devolve ordenado por ID
}

// reconstroiOIndiceDoCorrente realimenta os WIPs recém-abertos lendo o próprio
// segmento corrente.
//
// É a segunda metade da §2.5, e sem ela a primeira é um defeito: depois de todo
// restart o WIP do corrente nasce VAZIO — o replay da recovery roda antes de os
// índices existirem, e o .widx anterior é descartado porque um SIGKILL deixa
// header atrasado sobre diretório adiantado. Se esse segmento rotacionar antes de
// alguém reconstruí-lo, a selagem grava um .sidx IMUTÁVEL sem os records
// anteriores ao restart: falso negativo petrificado, e a revalidação do §44 só
// sabe descartar candidato a mais.
//
// O CUSTO É LIMITADO POR CONSTRUÇÃO, e é o argumento inteiro do desenho: um
// segmento tem no máximo 512 MiB, então isto é uma leitura sequencial de ~1 s na
// subida, uma vez. O que ela compra é não confiar no WIP através de um restart —
// e com isso somem o msync do índice no checkpoint, a invariante de ordem contra
// o durableOffset e a classe inteira de janelas de crash do índice secundário.
// Índice é dado derivado: a recuperação mais barata que está correta é derivá-lo
// de novo.
//
// Roda dentro do Open, depois do abreWIPsDoCorrente e ANTES de o primeiro Put ser
// aceito. Não há ctx porque não há quem cancele: a abertura ou termina ou falha.
func (s *Store) reconstroiOIndiceDoCorrente() error {
	if len(s.wips) == 0 {
		return nil
	}
	return s.varreOsRecordsDoSegmento(context.Background(), s.curSegID.Load(), s.camposIndexados,
		func(campo types.FieldID, h uint64, row types.RowID) error {
			// O mapa não pode errar: ele foi construído a partir DESTE mesmo
			// s.camposIndexados, no abreWIPsDoCorrente, algumas linhas acima.
			return s.wips[campo].Set(h, row)
		})
}

// varreOsRecordsDoSegmento percorre os records do segmento e emite, para cada
// campo indexado presente no payload de cada record, a tripla que o índice
// guarda: (campo, hash da chave canônica, rowID).
//
// UM varredor só, para os dois destinos — o Escritor da forma selada e os WIPs do
// segmento corrente. Dois varredores do mesmo formato divergiriam, e a
// divergência seria invisível: os dois produziriam arquivos bem formados, com
// conjuntos de postings diferentes.
//
// O ScanFrom é o MESMO iterador que o replay do tail usa, pela mesma razão que o
// replay passa pelo applyRecord: um segundo leitor do formato de record é um
// segundo lugar para o formato mudar pela metade. Ele para no primeiro record
// inválido sem erro (§52), e isso está certo aqui: no segmento corrente o
// OpenWriter já truncou o lixo além do fim válido, e nenhum outro segmento pode
// ter tail rasgado — a recovery recusa o dataset em que isso aconteça, e a
// rotação sela o anterior com fdatasync antes de criar o próximo.
//
// A pertinência do campo sai de `campos`, e o chamador passa o MESMO conjunto que
// a ingestão usa (s.camposIndexados). É essa igualdade que faz a alimentação
// incremental e a reconstruída coincidirem; slices.Contains sobre um conjunto
// ordenado de punhado de campos é mais barato que o mapa que o substituiria.
//
// A chave canônica é montada com valuekey.Append sobre um buffer LOCAL, e não
// sobre o s.chaveIndiceBuf do caminho de escrita: aquele é reutilizado sob o wmu,
// e esta varredura roda fora dele.
func (s *Store) varreOsRecordsDoSegmento(
	ctx context.Context,
	seg uint32,
	campos []types.FieldID,
	emite func(campo types.FieldID, h uint64, row types.RowID) error,
) error {
	var (
		chave  []byte
		leitor encoding.FieldReader
	)
	_, err := s.segR.ScanFrom(seg, int64(segment.HeaderSize), func(_ uint32, rec types.Record) error {
		// O cancelamento é conferido por RECORD, e não por segmento: um
		// segmento de 512 MiB é a unidade que se quer poder abandonar no meio,
		// e conferir só na entrada tornaria o ctx decorativo.
		if err := ctx.Err(); err != nil {
			return err
		}
		leitor.Reset(rec.Payload)
		for {
			id, v, ok := leitor.Next()
			if !ok {
				return leitor.Err()
			}
			if !slices.Contains(campos, id) {
				continue
			}
			k, err := valuekey.Append(chave[:0], v)
			if err != nil {
				return err
			}
			chave = k
			// primary.HashKey é a MESMA função da ingestão, e a escolha não é
			// gosto: ela nunca devolve 0 nem 1, e 0 é o sentinela de slot vazio
			// do WIP. Trocá-la aqui daria um arquivo bem formado e um conjunto
			// de postings que não casa com nada.
			if err := emite(id, primary.HashKey(k), rec.RowID); err != nil {
				return err
			}
		}
	})
	return err
}
