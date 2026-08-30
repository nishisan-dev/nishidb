package storage

import (
	"errors"
	"fmt"
	"math"
	"os"
	"slices"

	"github.com/nishisan-dev/nishidb/internal/index/primary"
	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/storage/segment"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// lookupIndexado é a superfície ÚNICA de consulta ao índice por segmento, e é
// ela que o Store.Lookup chama (spec §3.1).
//
// Ela nasceu como o corpo comum de LookupBitmap e LookupHash: mantendo os dois
// nomes e as duas assinaturas enquanto o planner ainda distinguia os motores, a
// suíte inteira de storage e de query — que já testa o
// contrato de superconjunto com ordem estritamente crescente e reentrância
// contra o Close — passou a exercitar o motor novo sem uma linha de teste
// mudada. Foi o oráculo da migração; com o planner unificado, resta um chamador
// só.
//
// UMA CONSULTA NUNCA TOMA O wmu, e isso é o ciclo inteiro: serializar consulta
// contra ingestão é o defeito que este motor existe para eliminar. O que ela
// toma são o lock de vida (pelo entra), o selMu — folha — para ler os mapas de
// índice, e o aposMu para o par (listar segmento, ler o primeiro record) da
// enumeração do histórico. A ordem vida → aposMu e vida → selMu respeita a
// invariante do godoc do aposMu.
//
// A chave canônica sai de um buffer LOCAL, e não do s.chaveIndiceBuf: aquele é
// reutilizado sob o wmu pelo caminho de escrita. Uma alocação por CONSULTA não
// aparece em gate nenhum — os gates medem por LINHA varrida.
func (s *Store) lookupIndexado(field types.FieldID, v types.Value) (types.RowIDIterator, error) {
	if err := s.entra(); err != nil {
		return nil, err
	}
	defer s.sai()
	// O campo não indexado é ERRO, e não iterador vazio: o planner só chega
	// aqui depois de perguntar pelas capabilities, então um campo sem índice é
	// plano inválido — e devolver vazio faria um bug de planner virar resultado
	// silenciosamente menor.
	if !slices.Contains(s.camposIndexados, field) {
		return nil, fmt.Errorf("%w: campo %d não tem índice por segmento", ErrInvalidArgument, field)
	}
	chave, err := valuekey.Append(nil, v)
	if err != nil {
		return nil, err
	}
	// primary.HashKey é a MESMA função da ingestão e da reconstrução. Trocá-la
	// aqui daria um hash bem formado que não casa com posting nenhuma.
	fontes, err := s.fontesDoCampo(field, primary.HashKey(chave))
	if err != nil {
		return nil, err
	}
	return &hashIterator{s: s, it: segindex.Une(fontes)}, nil
}

// fontesDoCampo monta as fontes da consulta, em ordem crescente de segmento
// (spec §3.2):
//
//	selado COM seção do campo → a fatia contígua das postings, zero alocação;
//	corrente (WIP)            → a caminhada da cadeia;
//	sem índice, histórico     → a faixa [primeiro, primeiroDoSeguinte);
//	sem índice, materializado → COLAPSA a consulta inteira numa caminhada
//	                            única do locations.idx (ver o comentário no
//	                            corpo — a forma por segmento tinha uma corrida
//	                            de falso negativo).
//
// É AQUI que a §3.4 é contada. Cada segmento cai num dos dois baldes de
// nishidb_index_segmentos_consultados_total, e essa é a ÚNICA coisa que a
// degradação torna visível: para o planner, o executor e o driver, os dois
// caminhos são o mesmo iterador ascendente.
//
// "Sem índice" NÃO é só "arquivo ausente": inclui o selado cuja tabela de
// seções não tem o campo, o selado que o crc32c reprovou e o segmento corrente
// na janela de rotação em que os WIPs foram fechados e ainda não reabertos. Os
// três chegam aqui como ok=false, e é POR ISSO que a decisão de indexar ou
// enumerar sai do `ok` do Lookup e nunca do TemCampo — o godoc do TemCampo
// escreve a ressalva por extenso. Tratar ok=false como "zero linhas" seria
// falso negativo, a única coisa que a revalidação do §44 não sabe consertar:
// ela só descarta candidato a mais.
func (s *Store) fontesDoCampo(campo types.FieldID, h uint64) ([]segindex.Fonte, error) {
	refs, err := s.listaSegmentos()
	if err != nil {
		return nil, err
	}
	corrente := s.curSegID.Load()
	fontes := make([]segindex.Fonte, 0, len(refs))
	// desiste fecha o que já foi montado. Sem isto, uma falha no meio da
	// montagem vazaria as fontes anteriores — e vazar iterador de índice só
	// aparece como descritor esgotado em produção.
	desiste := func(err error) ([]segindex.Fonte, error) {
		for _, f := range fontes {
			_ = f.Close()
		}
		return nil, err
	}
	// As duas contagens são LOCAIS e viram dois Add no fim, e não um Add por
	// segmento: um dataset com 500 segmentos pagaria 500 escritas atômicas na
	// mesma linha de cache por Lookup, e é justamente contenção de atômico que a
	// medição pré-ciclo do piloto encontrou consumindo 64% do CPU. Duas somas por
	// consulta não aparecem em gate nenhum — os gates medem por LINHA varrida.
	var porIndice, porEnumeracao int64
	for i, ref := range refs {
		if f, ok := s.fonteIndexada(ref.ID, corrente, campo, h); ok {
			fontes = append(fontes, f)
			porIndice++
			continue
		}
		// NO MATERIALIZADO, um único segmento sem índice COLAPSA a consulta
		// inteira numa caminhada só do locations.idx — todas as linhas vivas
		// viram candidatas, como num full scan. Não é pessimismo, é a única
		// resposta correta: a enumeração por dono da location decide a
		// pertinência no instante da ITERAÇÃO, enquanto o WIP do corrente é um
		// snapshot da CONSTRUÇÃO — um update concorrente que movesse a linha do
		// segmento enumerado para o corrente a faria sumir das duas fontes
		// (falso negativo, que o §44 não conserta). Uma caminhada única não tem
		// o cérebro dividido: cada location é lida UMA vez, e a linha sai por
		// onde quer que more. De quebra, ela custa UMA passada onde o desenho
		// anterior custava uma POR SEGMENTO sem índice.
		//
		// O histórico não precisa disso: lá não existe update, as faixas de
		// rowId são estáticas e a enumeração por faixa não consulta estado
		// concorrente nenhum.
		if !s.EhHistorico() {
			for _, f := range fontes {
				_ = f.Close()
			}
			// O colapso É a degradação, e a métrica diz isso: todos os
			// segmentos responderam pela enumeração única. Contar os que
			// "teriam" respondido por índice esconderia exatamente o que a
			// série existe para mostrar.
			s.segPorEnumeracao.Add(int64(len(refs)))
			return []segindex.Fonte{s.fonteDeTodasAsLocations()}, nil
		}
		f, err := s.fonteDaFaixa(refs, i)
		if err != nil {
			return desiste(err)
		}
		fontes = append(fontes, f)
		porEnumeracao++
	}
	// Publicadas mesmo com a montagem tendo falhado? Não: o desiste() acima
	// retorna antes, e o que ele desfaz são as fontes, não o trabalho de decidir
	// por qual caminho cada segmento responderia. Contar uma montagem abortada
	// diria que houve consulta onde não houve.
	s.segPorIndice.Add(porIndice)
	s.segPorEnumeracao.Add(porEnumeracao)
	return fontes, nil
}

// fonteIndexada devolve a fonte do índice do segmento, ou ok=false quando este
// segmento não responde por este campo e o chamador precisa enumerá-lo.
//
// Erro de ABERTURA do selado não sobe: um .sidx ausente, com formato que este
// binário não lê, ou corrompido é dado DERIVADO faltando, e a resposta correta
// para dado derivado faltando é derivá-lo de novo — ou, aqui, responder sem
// ele. Subir o erro transformaria um índice podre em consulta indisponível
// sobre dado íntegro. O dataset responde certo e devagar, que é exatamente o
// estado de qualquer segmento fora da janela de indexação.
//
// O QUE CONTINUA SEM DONO é o arquivo REPROVADO NO CRC. O laço de reconstrução
// da §2.4 já existe e drena a fila, mas a fila é derivada da tabela de seções,
// que não confere crc (segindex.CamposSelados, e ela não pode: são até 2,25 GB
// por arquivo a cada passada) — então um .sidx corrompido continua parecendo
// completo para ela. Curá-lo exige que quem detecta a divergência AQUI apague o
// arquivo, para o segmento voltar a ser pendente por AUSÊNCIA; é o gatilho 3 da
// §2.4, e ele tem número próprio — débito 103 —, com a decisão que falta escrita
// lá: se uma CONSULTA pode apagar arquivo do dataset.
func (s *Store) fonteIndexada(seg, corrente uint32, campo types.FieldID, h uint64) (segindex.Fonte, bool) {
	if seg == corrente {
		if f, ok := s.fonteDoWIP(seg, campo, h); ok {
			return f, true
		}
		// ok=false aqui tem DUAS causas, e as duas caem certas no caminho
		// selado: o mapa vazio da janela de rotação (o .sidx deste segmento
		// ainda não existe → enumera), e o snapshot de `corrente` OBSOLETO —
		// a rotação completou no meio da consulta e os WIPs publicados já são
		// do segmento novo (wipsSeg difere). Neste segundo caso o segmento
		// acabou de ganhar .sidx, e é ele que responde; devolver ok=false
		// direto o faria enumerar sem necessidade, e devolver o WIP do
		// segmento errado — o que este código fazia — suprimia as linhas dele
		// dos candidatos: falso negativo, que o §44 não conserta.
	}
	s.garanteSelado(seg)
	// O Lookup roda SOB o selMu, e não só a busca no mapa, pela mesma razão que
	// o fonteDoWIP segura o lock durante a caminhada da cadeia: entre pegar o
	// ponteiro e usá-lo, a aposentadoria pode fechar o índice. É lá dentro que a
	// Fonte registra a vida que impede o munmap (segindex.Selado.Fecha), e
	// registrar fora do lock deixaria justamente a janela que a vida existe para
	// fechar.
	//
	// O QUE NÃO PODE ACONTECER DEBAIXO DESTE LOCK é trabalho longo, e o crc32c
	// era isso: o selMu é o mesmo lock que o aposAppend toma a cada rotação, sob
	// o wmu. Por isso o garanteSelado já o conferiu, fora de lock nenhum, e o
	// Lookup daqui encontra o sync.Once cumprido. O que sobra sob o lock é busca
	// binária em tabela de dezenas de entradas mais o incremento da vida.
	s.selMu.RLock()
	defer s.selMu.RUnlock()
	sel := s.selados[seg]
	if sel == nil {
		return nil, false
	}
	return sel.Lookup(campo, h)
}

// garanteSelado abre o índice selado do segmento sob demanda e o publica no
// cache. Não devolve nada de propósito: quem for usá-lo tem de relê-lo do mapa
// sob o selMu, porque entre publicar e usar a aposentadoria pode ter passado.
//
// O mapeamento fica CACHEADO no s.selados até a aposentadoria do segmento ou o
// Close do dataset, e não há LRU: o mapeamento somente-leitura não retém
// descritor, e 500 VMAs num teto padrão de 65.530 não é pressão.
//
// A ausência NÃO é memorizada, e a escolha é deliberada: um dataset migrado
// paga um open(2) falho por segmento por consulta, que é ruído ao lado da
// enumeração que vem logo depois — e memorizar a ausência exigiria um segundo
// estado no mapa que o Close teria de saber distinguir de um Selado vivo.
//
// A APOSENTADORIA PODE FECHAR O ÍNDICE COM UMA VARREDURA EM VOO, e quem resolve
// isso é a contagem de vidas do segindex.Selado: o Fecha impede leitores novos e
// adia o munmap até a última Fonte sair. Sem ela o desfecho não seria resultado
// degradado, seria SIGSEGV — o Close do dataset está coberto pelo quiesce (o
// hashIterator reentra pelo entra() a cada avanço), mas a aposentadoria roda na
// goroutine de retenção e não passa por lock de vida nenhum.
//
// O que continua fora daqui é o PIN de segmento (subsistema 3), que é outra
// pergunta: se a consulta longa deve continuar ENXERGANDO as linhas que a
// retenção tirou. Aquilo é visibilidade de dado; isto é segurança de memória.
//
// A GERAÇÃO LIDA ANTES DA ABERTURA é a terceira aresta desta função, e ela
// deixou de ser teórica quando o laço da §2.4 subiu: entre abrir o arquivo aqui
// e publicá-lo lá embaixo cabe uma reescrita inteira da reconstrução, e o
// mapeamento aberto ANTES dela descreve o arquivo anterior — o mmap segura o
// inode, então ele sobrevive ao rename e continuaria servindo o conteúdo velho.
// Publicá-lo depois da evição faria o segmento responder sem a seção do campo
// novo (ou seja, ENUMERANDO) até a próxima abertura do dataset, e ainda
// prenderia o inode substituído. Ver o godoc do selGeracao.
func (s *Store) garanteSelado(seg uint32) {
	s.selMu.RLock()
	_, ok := s.selados[seg]
	geracao := s.selGeracao
	s.selMu.RUnlock()
	if ok {
		return
	}
	novo, err := segindex.AbreSelado(segindex.CaminhoSelado(s.dirDeDados(), seg))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			// Ausente é o estado NORMAL — segmento fora da janela, ou gravado
			// por um binário anterior — e sai calado. O resto é anomalia e ganha
			// rastro, porque silêncio sobre índice degradado é o terceiro
			// defeito que esta spec veio corrigir.
			//
			// O CUSTO ESTÁ ESCRITO: como a falha não é memorizada, um .sidx
			// podre emite este WARN uma vez por CONSULTA, não uma por processo.
			// Deduplicá-lo exigiria memorizar a falha, e memorizar impediria a
			// adoção do arquivo que a reconstrução (§2.4) vier a publicar no
			// lugar. Quem fecharia isso é a remoção do arquivo reprovado por
			// quem o detecta — o gatilho 3 da §2.4, ainda sem dono. O observável
			// já existe: nishidb_index_segmentos_consultados_total{modo}, que
			// conta quantos segmentos responderam por cada caminho, e
			// nishidb_index_crc_falhas_total, que separa o arquivo podre do
			// simplesmente ausente.
			s.opts.Log.Warn("indice.selado_ilegivel",
				"o índice do segmento não pôde ser aberto; ele responde por enumeração",
				"segmento", seg, "erro", err.Error())
		}
		return
	}
	// O crc32c é conferido AQUI, fora de qualquer lock, e não deixado para a
	// primeira consulta.
	//
	// A verificação continua PREGUIÇOSA no sentido da §1.5 — o que ela proíbe é
	// conferir na subida, porque um segmento com coluna quase-única chega a
	// 2,25 GB e a conta seria paga por 500 arquivos que a consulta talvez nem
	// toque. Aqui ela só acontece quando uma consulta de fato precisou deste
	// segmento, que é a mesma economia. O que muda é ONDE a conta é paga.
	//
	// DEIXÁ-LA PARA O Lookup A PORIA DEBAIXO DO selMu, e o selMu é o lock da
	// ROTAÇÃO: o aposAppend o toma a cada segmento novo, sob o wmu, no caminho
	// do Put e do Apenda. Com escritor pendente o RWMutex barra os RLock
	// seguintes, então uma varredura de centenas de MB debaixo dele faria a
	// primeira consulta a um .sidx grande e frio serializar consulta contra
	// ingestão — exatamente o defeito que este ciclo existe para eliminar.
	//
	// Crc divergente NÃO impede a publicação: o Lookup passa a responder
	// ok=false e o segmento enumera, que é a degradação correta. Publicar
	// mantém o Once cumprido e faz esse ok=false custar nada; recusar obrigaria
	// a próxima consulta a reabrir e reconferir o arquivo podre inteiro.
	if err := novo.ErroDeIntegridade(); err != nil {
		// A série sobe junto do WARN, e no mesmo ponto, porque as duas descrevem
		// o mesmo evento: um arquivo de índice reprovado.
		//
		// Ela conta por ABERTURA, e a linha logo abaixo é o que torna isso quase
		// "uma vez por processo": o publicaSelado instala o mapeamento REPROVADO
		// no cache, então a consulta seguinte o encontra lá e volta sem reconferir
		// o crc. O incremento só se repete se a evição tirar o mapeamento do
		// caminho. Por isso a série é DEGRAU e não taxa — e por isso a Ajuda dela
		// manda o operador olhar a enumeração persistente para saber se o defeito
		// continua de pé. O WARN acompanha o degrau pela mesma razão.
		s.crcFalhas.Add(1)
		s.opts.Log.Warn("indice.selado_corrompido",
			"o índice do segmento não passou no crc32c; ele responde por enumeração até ser reconstruído",
			"segmento", seg, "erro", err.Error())
	}
	s.publicaSelado(seg, novo, geracao)
}

// publicaSelado instala no cache o mapeamento aberto pelo garanteSelado, ou o
// descarta quando ele já não descreve o arquivo que está em disco.
//
// Função própria, e não o rabo do garanteSelado, porque a janela que ela fecha é
// entre DUAS chamadas — abrir e publicar — e um teste que não consiga se meter no
// meio delas não prova nada sobre a geração.
//
// Descartar um mapeamento que ainda estava bom é possível (a evição pode ter sido
// de OUTRO segmento) e é o lado barato do erro: a consulta seguinte reabre o
// arquivo. O lado caro é o oposto — publicar o velho, que degrada até a próxima
// abertura do dataset.
func (s *Store) publicaSelado(seg uint32, novo *segindex.Selado, geracao uint64) {
	s.selMu.Lock()
	defer s.selMu.Unlock()
	if s.selados == nil {
		// Só acontece em dataset sem campo indexado, que o lookupIndexado já
		// recusou antes de chegar aqui. A guarda existe porque a alternativa é
		// escrever num mapa nil — pânico —, e o preço dela é fechar o
		// mapeamento que ninguém mais teria como alcançar.
		_ = novo.Fecha()
		return
	}
	if s.selGeracao != geracao {
		_ = novo.Fecha()
		return
	}
	// Duas consultas podem ter aberto o mesmo arquivo em paralelo: quem chega
	// depois descarta o próprio mapeamento e fica com o já publicado, senão o
	// segundo vazaria — o Close só alcança o que está no mapa.
	if _, ok := s.selados[seg]; ok {
		_ = novo.Fecha()
		return
	}
	s.selados[seg] = novo
}

// fonteDoWIP caminha a cadeia do valor no índice MUTÁVEL do segmento corrente.
//
// ok=false em dois casos. O primeiro é não haver WIP para o campo — a janela de
// rotação, em que o aposAppend fecha os WIPs do segmento que deixou de ser
// corrente, sela e só então abre os do novo. O segundo é a IDENTIDADE: `seg` é
// o que o CHAMADOR acredita ser o corrente (um snapshot do curSegID tirado no
// início da consulta), e os WIPs publicados podem já pertencer ao segmento
// seguinte quando a rotação completou no meio. Responder pelo WIP do segmento
// errado devolveria ok=true com as postings de OUTRO segmento e suprimiria as
// deste dos candidatos — falso negativo. Nos dois casos o chamador cai para o
// caminho selado, que responde pelo .sidx recém-selado ou manda enumerar.
//
// O selMu é segurado DURANTE o Lookup, e não só para ler o mapa: o WIP entra em
// pânico nomeado se for usado depois do Fecha, e sem o lock a rotação poderia
// fechá-lo entre pegar o ponteiro e caminhar a cadeia. É lock FOLHA e a
// consulta não toma o wmu por baixo dele. A identidade mora sob o MESMO lock
// que publica mapa e dono juntos — não há janela entre conferir e caminhar.
func (s *Store) fonteDoWIP(seg uint32, campo types.FieldID, h uint64) (segindex.Fonte, bool) {
	s.selMu.RLock()
	defer s.selMu.RUnlock()
	if s.wipsSeg != seg {
		return nil, false
	}
	w := s.wips[campo]
	if w == nil {
		return nil, false
	}
	// O WIP.Lookup materializa a cadeia numa fatia de tamanho exato, já
	// ordenada e compactada. É UMA alocação por segmento corrente, e não por
	// linha: a fatia é a única forma de o iterador sobreviver ao lock, que não
	// pode durar a varredura inteira sem travar a rotação atrás de um
	// consumidor lento.
	return &fonteMaterializada{rows: w.Lookup(h)}, true
}

// A enumeração — a resposta de segmento SEM índice — tem uma forma por modo de
// dataset, e as duas moram no laço do fontesDoCampo:
//
//	histórico     → fonteDaFaixa, por segmento: as faixas de rowId são
//	                estáticas e a fonte não consulta estado concorrente nenhum;
//	materializado → fonteDeTodasAsLocations, para a CONSULTA INTEIRA: a
//	                caminhada única do locations.idx, pelo colapso explicado lá.
//
// NÃO HÁ REGRESSÃO CONTRA O FULL SCAN, e esta é a frase que impede a próxima
// revisão de "otimizar" a enumeração para fora: para o caminho enumerado o
// executor faz EXATAMENTE o que faria varrendo — recebe rowIds crescentes e
// chama GetByRowID em cada um. No materializado a frase é literal: a caminhada
// única É a fonte de candidatos de um full scan.

// fonteDaFaixa monta a enumeração do HISTÓRICO: a faixa [primeiro,
// primeiroDoSeguinte).
//
// Ela é possível por uma propriedade que só a tabela histórica tem: os appends
// são serializados pelo mutex do writer e o rowId é monotônico, então os
// segmentos particionam o espaço de rowId em faixas contíguas e ordenadas. É a
// mesma propriedade que o primeiroRowIDDoSeguinte da retenção já explora, com o
// mesmo pread — e é ela que faz a união escolher CONCATENAÇÃO no histórico, sem
// uma comparação sequer entre fontes.
//
// A fronteira superior salta segmentos VAZIOS em vez de desistir: um segmento
// recém-criado ainda sem record não tem primeiro rowId para ler, e parar ali
// deixaria a faixa curta — que é falso negativo. Quando nenhum seguinte
// responde, o fim é o nextRowID, que é frouxo e seguro.
//
// A LISTAGEM NÃO ESTÁ SOB A MESMA BARREIRA QUE OS PREADS, e o comentário diz
// isso porque a diferença tem consequência. O `refs` é capturado uma vez, no
// fontesDoCampo, fora do aposMu; os preads acontecem aqui, sob ele. Entre os
// dois, a retenção pode aposentar um segmento — e então o arquivo que a listagem
// nomeou já não existe.
//
// Esse desfecho é tratado como fonte VAZIA, e não como erro: linha aposentada é
// linha morta, e é assim que o resto do executor já se comporta — ela some pela
// location, em silêncio. Propagar o erro faria uma consulta inteira falhar por
// uma passada de retenção que aconteceu ao lado, onde o motor antigo — que não
// tocava segmento nenhum — devolvia resultado normalmente.
//
// O aposMu continua aqui pela razão do GetByRowID: ele impede que o descritor
// feche ENTRE o pread e a leitura dos bytes.
func (s *Store) fonteDaFaixa(refs []segment.SegmentRef, i int) (segindex.Fonte, error) {
	s.aposMu.RLock()
	defer s.aposMu.RUnlock()
	var buf []byte
	ini, buf, ok, err := s.primeiroRowIDDe(refs[i], buf)
	if err != nil {
		return nil, err
	}
	if !ok {
		// Segmento sem record nenhum: não há o que enumerar, e uma fonte vazia
		// por declaração some na união sem empurrar ninguém para o merge.
		return &fonteFaixa{}, nil
	}
	fim := types.RowID(s.nextRowID.Load())
	for j := i + 1; j < len(refs); j++ {
		p, b, ok, err := s.primeiroRowIDDe(refs[j], buf)
		buf = b
		if err != nil {
			return nil, err
		}
		if ok {
			fim = p
			break
		}
	}
	return &fonteFaixa{ini: ini, prox: ini, fim: fim}, nil
}

// primeiroRowIDDe lê o rowId do primeiro record do segmento, que mora sempre no
// offset fixo logo após o header. ok=false para segmento sem record.
//
// É o mesmo pread do primeiroRowIDDoSeguinte da retenção, com buffer do
// CHAMADOR: o s.retBuf daquele caminho é do writer, e uma consulta que o
// reutilizasse o corromperia sem tomar o wmu — que é justamente o que este
// ciclo existe para não fazer.
func (s *Store) primeiroRowIDDe(ref segment.SegmentRef, buf []byte) (types.RowID, []byte, bool, error) {
	if ref.SizeBytes <= segment.HeaderSize {
		return 0, buf, false, nil
	}
	rec, out, err := s.segR.ReadRecord(ref.ID, uint32(segment.HeaderSize), buf)
	if err != nil {
		// Segmento aposentado entre a listagem e este pread responde "não tenho
		// primeiro rowId", e não erro: ele já não tem linha viva nenhuma, e a
		// consulta que o encontrou nesse estado devolve o que sobrou — que é o
		// mesmo que ela devolveria se a passada de retenção tivesse acontecido
		// um instante antes. Errar aqui derrubaria a consulta inteira por uma
		// corrida cujo desfecho correto é ignorar o segmento.
		if errors.Is(err, os.ErrNotExist) {
			return 0, out, false, nil
		}
		return 0, out, false, err
	}
	return rec.RowID, out, true, nil
}

// fonteDeTodasAsLocations monta a enumeração do MATERIALIZADO: a caminhada
// única sobre o locations.idx que o colapso do fontesDoCampo usa.
//
// Os limites são a marca-d'água e o nextRowID, tomados uma vez: a fonte é um
// instantâneo do limite superior, como o ScanLive.
func (s *Store) fonteDeTodasAsLocations() segindex.Fonte {
	base := s.RowIDBase()
	return &fonteLocations{s: s, ini: base, prox: base, fim: types.RowID(s.nextRowID.Load())}
}

// fonteMaterializada percorre uma fatia de rowIds já pronta — hoje, só a que o
// WIP do segmento corrente devolve.
//
// Ela é o oposto das outras três: aqui os rowIds JÁ estão em memória, e os
// limites saem das pontas da fatia, que o WIP entrega ordenada e compactada.
type fonteMaterializada struct {
	rows []types.RowID
	i    int
	cur  types.RowID
}

func (f *fonteMaterializada) Next() bool {
	if f.i >= len(f.rows) {
		return false
	}
	f.cur = f.rows[f.i]
	f.i++
	return true
}

func (f *fonteMaterializada) RowID() types.RowID { return f.cur }
func (f *fonteMaterializada) Err() error         { return nil }
func (f *fonteMaterializada) Close() error       { return nil }

// MinRowID devolve math.MaxUint64 na fatia vazia, que é o Min > Max com que o
// contrato da Fonte declara "não vou emitir nada" — e é o que faz a união
// descartá-la em vez de empurrar as vizinhas para o merge.
func (f *fonteMaterializada) MinRowID() types.RowID {
	if len(f.rows) == 0 {
		return math.MaxUint64
	}
	return f.rows[0]
}

func (f *fonteMaterializada) MaxRowID() types.RowID {
	if len(f.rows) == 0 {
		return 0
	}
	return f.rows[len(f.rows)-1]
}

// fonteFaixa enumera os rowIds de um segmento do HISTÓRICO, onde eles formam
// faixa contígua (ver fonteDaFaixa).
//
// PREGUIÇOSA e sem alocação nenhuma: são três inteiros, e a ordem crescente sai
// do próprio incremento. Ela emite rowIds que podem não existir mais — abaixo
// da marca-d'água de retenção, por exemplo —, e isso é correto: o contrato é
// SUPERCONJUNTO de candidatos, e o executor revalida cada um pelo GetByRowID
// (§44), que é exatamente o que ele faria num full scan.
type fonteFaixa struct {
	ini, prox, fim, cur types.RowID
}

func (f *fonteFaixa) Next() bool {
	if f.prox >= f.fim {
		return false
	}
	f.cur = f.prox
	f.prox++
	return true
}

func (f *fonteFaixa) RowID() types.RowID { return f.cur }
func (f *fonteFaixa) Err() error         { return nil }
func (f *fonteFaixa) Close() error       { return nil }

// MinRowID e MaxRowID descrevem a faixa TODA, e não o que resta iterar: o
// contrato da Fonte exige que os dois valham ANTES de consumir, porque é sobre
// eles que a união decide entre concatenação e merge sem tocar em fonte alguma.
func (f *fonteFaixa) MinRowID() types.RowID {
	if f.ini >= f.fim {
		return math.MaxUint64
	}
	return f.ini
}

func (f *fonteFaixa) MaxRowID() types.RowID {
	if f.ini >= f.fim {
		return 0
	}
	return f.fim - 1
}

// fonteLocations enumera TODOS os rowIds com location publicada, no dataset
// MATERIALIZADO — onde a propriedade da fonteFaixa não vale, porque um update
// traz para o segmento corrente uma linha de rowId arbitrário, possivelmente
// menor que o do primeiro record dele.
//
// Ela NÃO filtra por segmento, e a ausência é a correção de uma corrida: a
// versão por segmento decidia a pertinência pelo DONO da location no instante
// da iteração, enquanto o WIP do corrente era um snapshot da construção — a
// linha que um update concorrente movesse entre os dois instantes sumia das
// duas fontes, e falso negativo é o que o §44 não conserta. Sem o filtro, cada
// location é lida UMA vez e a linha sai por onde quer que more.
//
// A varredura é sobre o locations.idx, que é array denso em mmap: 8 milhões de
// linhas são 64 MB sequenciais. Ela é PREGUIÇOSA e não materializa nada — o
// consumo é constante, independente de quantas linhas casem —, e a ordem
// crescente sai da própria caminhada.
//
// MinRowID/MaxRowID são a marca-d'água e o nextRowID: o intervalo inteiro do
// dataset, que é exatamente o que a fonte pode emitir.
type fonteLocations struct {
	s                   *Store
	ini, prox, fim, cur types.RowID
}

// Next avança até o próximo rowId com location publicada.
//
// Não confere tombstone, prazo nem marca-d'água: filtrar aqui criaria uma
// SEGUNDA definição de "vivo" ao lado da do executor, e as duas divergiriam na
// primeira vez que uma delas mudasse. Candidato a mais o §44 descarta; a
// definição duplicada, ninguém.
func (f *fonteLocations) Next() bool {
	for f.prox < f.fim {
		id := f.prox
		f.prox++
		if _, ok := f.s.locs.Get(id); !ok {
			continue
		}
		f.cur = id
		return true
	}
	return false
}

func (f *fonteLocations) RowID() types.RowID { return f.cur }
func (f *fonteLocations) Err() error         { return nil }
func (f *fonteLocations) Close() error       { return nil }

func (f *fonteLocations) MinRowID() types.RowID {
	if f.ini >= f.fim {
		return math.MaxUint64
	}
	return f.ini
}

func (f *fonteLocations) MaxRowID() types.RowID {
	if f.ini >= f.fim {
		return 0
	}
	return f.fim - 1
}
