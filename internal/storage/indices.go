package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/nishisan-dev/nishidb/internal/index/primary"
	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/storage/segment"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// slotsIniciaisOfWIP dimensiona o diretório de um .widx recém-criado.
//
// O número de valores DISTINTOS que um segmento vai ter é desconhecido no
// instante em que o arquivo nasce, e não há de onde derivá-lo: EstimatedRows
// mede linhas do dataset inteiro, não distintos de um segmento. Então o valor é
// uma aposta, e ela é barata dos dois lados — 1.024 slots são 32 KiB de
// diretório mais postings iniciais, pagos por (segmento corrente, campo) e não
// por dataset, e errar para baixo custa apenas os rehashes amortizados que o
// próprio WIP faz. O que 1.024 compra é pular a dezena de rehashes iniciais que
// um piso de 16 slots pagaria em todo segmento de todo campo.
const slotsIniciaisOfWIP = 1024

// dirOfData é onde o índice por segmento mora: AO LADO do .dat, e não num
// diretório próprio, porque o par (dado, índice) nasce e morre junto — é essa
// vizinhança que faz "purgar o segmento leva o índice junto" ser uma linha de
// código. O segment.ListSegments casa só o nome canônico exato, então os
// arquivos novos não perturbam a listagem, a recovery nem o manifest.
func (s *Store) dirOfData() string { return filepath.Join(s.opts.Dir, "data") }

// FieldsIndexados devolve, em ordem crescente, os campos com índice por
// segmento. O conjunto é resolvido no Open e IMUTÁVEL na vida do Store, então a
// resposta é estável — é a mesma promessa que o Indexed faz.
//
// Cópia, e não o slice interno: o conjunto é imutável por contrato, e devolver
// a fatia de dentro deixaria qualquer chamador quebrar a promessa por engano.
func (s *Store) FieldsIndexados() []types.FieldID { return slices.Clone(s.fieldsIndexados) }

// openIndicesOfSegment fixa o conjunto indexado, descarta os WIPs herdados do
// processo anterior, abre os do segmento corrente e os RECONSTRÓI a partir dos
// records dele. É a §2.5 inteira, num ponto só.
//
// Roda no Open DEPOIS do ramo de recovery, e a posição não é escolha de estilo:
// os campos vêm do resolveFieldsIndexados (cuja ordem de LookupOrCreate é
// contratual e não pode ser tocada), mas os WIPs precisam do SEGMENTO CORRENTE,
// e quem o decide é o ramo de recovery — antes dele, curSecID ainda é zero num
// dataset existente. E roda ANTES de o Open devolver o Store, o que é a outra
// metade do requisito: o primeiro Put não pode ser aceito sobre um WIP vazio,
// porque a rotação seguinte selaria esse vazio num .sidx IMUTÁVEL — falso
// negativo petrificado, e a revalidação do §44 só sabe descartar candidato a
// mais.
func (s *Store) openIndicesOfSegment(fields []types.FieldID) error {
	s.fieldsIndexados = fields
	if err := s.discardTodosOsWIPs(); err != nil {
		return err
	}
	// Os temporários da forma SELADA saem no mesmo ponto, e pela mesma razão que
	// o .widx de segmento aposentado: nenhuma enumeração que parta dos dados os
	// alcança. Eles ganharam nome único por escritor na §2.4 — é o que impede a
	// selagem e a reconstrução de rasgarem o arquivo uma da outra —, e o preço
	// disso é que a escrita seguinte já não os trunca por cima. Sem esta limpeza,
	// cada crash entre o WriteFile e o rename deixaria em disco um arquivo que
	// o IndexBytes conta e ninguém explica.
	if err := segindex.RemoveTempFiles(s.dirOfData()); err != nil {
		return err
	}
	// O .sidx ÓRFÃO — de segmento que já não tem .dat — sai aqui, no mesmo
	// ponto em que o .retired e o .widx órfão já saem (spec §2.3): um crash
	// entre o unlink do .dat e a remoção do índice na aposentadoria o deixa
	// para trás, e nenhuma enumeração que parta dos dados o alcança. Sem esta
	// varredura ele ficaria em disco para sempre, contado pelo IndexBytes como
	// disco que nada explica. Roda ANTES do early-return de campos vazios de
	// propósito: um dataset que deixou de indexar continua tendo órfãos a
	// limpar.
	if err := s.deleteSidxOrfaos(); err != nil {
		return err
	}
	if len(fields) == 0 {
		return nil
	}
	s.selados = make(map[uint32]*segindex.Sealed)
	if err := s.openWIPsOfCurrent(); err != nil {
		return err
	}
	return s.reconstroiOIndexOfCurrent()
}

// discardTodosOsWIPs apaga TODO .widx que o diretório de dados carregue de uma
// execução anterior — do segmento corrente e de qualquer outro.
//
// O .widx NUNCA é confiado através de um restart (spec §2.5), e a razão não é
// prudência: o postCount só vai para o header no Fecha, então um processo morto
// por SIGKILL deixa um header ATRASADO sobre um diretório adiantado — slots que
// declaram cabeça e contagem contra um total que o arquivo não confirma. O
// próprio segindex recusa esse arquivo com ErrFormat, e adotá-lo seria trocar
// uma abertura que funciona por um dataset inacessível. Medido: sem este
// descarte, o TestKill9NoMeioDaIngestaoPreservaOEstadoConfirmado passa a
// reabrir com "o slot 572 declara contagem 10426 contra 0 postings".
//
// UM ato só para os três casos da §2.5, e é isso que justifica a varredura ser
// do diretório inteiro: o que muda entre eles não é o descarte, é o que vem
// DEPOIS dele.
//
//	corrente               → reconstroiOIndexOfCurrent, aqui mesmo, lendo o
//	                         próprio segmento antes do primeiro Put;
//	não corrente, com .dat → SegmentosPendentesOfRebuild o devolve, e o
//	                         laço de fundo (§2.4) o refaz quando puder;
//	sem .dat               → nada. O segmento foi aposentado, e o .widx é o
//	                         resíduo de um crash entre o unlink do dado e a
//	                         limpeza do índice. Sem esta varredura ele ficaria
//	                         em disco PARA SEMPRE, porque nenhuma enumeração que
//	                         parta dos dados o alcança — e o IndexBytes o
//	                         contaria como disco que nada explica.
func (s *Store) discardTodosOsWIPs() error {
	segs, err := segindex.SegmentsWithWIP(s.dirOfData())
	if err != nil {
		return classificaNameIlegivelOfIndex(err, s.dirOfData())
	}
	for _, sec := range segs {
		if err := s.deleteWIPsPeloName(sec); err != nil {
			return classificaNameIlegivelOfIndex(err, s.dirOfData())
		}
	}
	return nil
}

// deleteSidxOrfaos remove todo .sidx cujo segmento já não tem .dat — o resíduo
// de um crash entre o unlink do dado e a remoção do índice na aposentadoria
// (spec §2.3). Por NOME, como toda limpeza de lixo deste arquivo: um .sidx
// rasgado é justamente o que não se deixa abrir.
//
// A comparação é contra a listagem canônica de segmentos (segment.ListSegments,
// a mesma que a recovery usa), e não contra o manifest: o filesystem é a
// verdade sobre quais segmentos existem (spec §5.3).
func (s *Store) deleteSidxOrfaos() error {
	withSealed, err := segindex.SegmentsWithSealed(s.dirOfData())
	if err != nil {
		return classificaNameIlegivelOfIndex(err, s.dirOfData())
	}
	if len(withSealed) == 0 {
		return nil
	}
	refs, err := segment.ListSegments(s.dirOfData())
	if err != nil {
		return err
	}
	withDat := make(map[uint32]struct{}, len(refs))
	for _, ref := range refs {
		withDat[ref.ID] = struct{}{}
	}
	for _, sec := range withSealed {
		if _, ok := withDat[sec]; ok {
			continue
		}
		path := segindex.SealedPath(s.dirOfData(), sec)
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("storage: remover o índice órfão %s: %w", path, err)
		}
		s.opts.Log.Info("indice.orfao_removido",
			"o índice de um segmento aposentado sobrou de um crash e saiu na abertura",
			"segmento", sec)
	}
	return nil
}

// classificaNameIlegivelOfIndex põe o ErrFormat do espaço de nomes .widx na
// sentinela que o exit code entende.
//
// Sem isto o erro sobe cru e cai no default do errorCode (cli.go) — exit 1,
// "erro de uso", o único código que manda o operador olhar a LINHA DE COMANDO em
// vez do dataset. E aqui a linha de comando está certa: o que há é um arquivo em
// data/ que este binário não escreveu, e ele impede a abertura porque a varredura
// da §2.5 percorre o diretório inteiro. ErrDatasetCorrupt é a classificação
// honesta — não afirma perda de dado, afirma que a resposta operacional é olhar o
// diretório do dataset —, e leva ao mesmo ExitDataset (3) do manifest ilegível.
// Três, e não quatro: quatro é o ExitWriter, writer em modo falho, que é outra
// categoria e outra ação do operador (reabrir o dataset, não investigá-lo).
//
// Erro que não é de formato passa intacto: EIO ao listar o diretório é falha de
// infra, e chamá-la de corrupção induz a resposta destrutiva (restaurar backup).
// São os três baldes do §101, e este só mexe no primeiro.
func classificaNameIlegivelOfIndex(err error, dir string) error {
	if err == nil || !errors.Is(err, segindex.ErrFormat) {
		return err
	}
	return fmt.Errorf("%w: %s tem um arquivo no espaço de nomes .widx que este binário não escreveu: %w",
		ErrDatasetCorrupt, dir, err)
}

// deleteWIPsPeloName remove os .widx do segmento SEM abrir nenhum deles.
//
// É o único lugar que apaga .widx fora da selagem, e ele é um só de propósito:
// os dois consumidores — o descarte da abertura e a aposentadoria do segmento —
// precisam da MESMA regra, e uma segunda cópia divergiria na primeira vez que a
// regra mudasse.
//
// Por NOME, e nunca pelo segindex.RemoveWIPs: aquele abre o arquivo com o
// OpenWIP, que mapeia e valida o header antes de remover. Um .widx rasgado por
// crash é justamente o que não se deixa abrir — a limpeza do lixo não pode
// depender de o lixo estar íntegro (spec §2.5).
func (s *Store) deleteWIPsPeloName(sec uint32) error {
	fields, err := segindex.ListWIP(s.dirOfData(), sec)
	if err != nil {
		return err
	}
	for _, field := range fields {
		path := segindex.WIPPath(s.dirOfData(), sec, field)
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("storage: remover o índice WIP %s: %w", path, err)
		}
	}
	return nil
}

// openWIPsOfCurrent abre um .widx por campo indexado para o segmento corrente.
//
// Falha no meio fecha o que já abriu: um WIP órfão manteria mapeamento de pé
// depois de o Open ter desistido, e o Close do Store não teria como alcançá-lo
// porque o mapa só é publicado no fim.
//
// Os arquivos são abertos FORA do selMu e o mapa é publicado sob ele: a
// consulta lê s.wips pelo mesmo lock, e segurá-lo por N mmaps travaria toda
// leitura do dataset atrás de uma rotação de segmento.
func (s *Store) openWIPsOfCurrent() error {
	if len(s.fieldsIndexados) == 0 {
		return nil
	}
	sec := s.curSecID.Load()
	wips := make(map[types.FieldID]*segindex.WIP, len(s.fieldsIndexados))
	for _, field := range s.fieldsIndexados {
		w, err := segindex.OpenWIP(s.dirOfData(), sec, field, slotsIniciaisOfWIP)
		if err != nil {
			for _, aberto := range wips {
				_ = aberto.Close()
			}
			return err
		}
		wips[field] = w
	}
	s.selMu.Lock()
	s.wips = wips
	s.wipsSec = sec
	s.selMu.Unlock()
	return nil
}

// closeWIPs desmonta os índices mutáveis do segmento que deixou de ser o
// corrente.
//
// O mapa é zerado SEMPRE, inclusive no erro: Fecha depois de Fecha é uso após o
// fim e entra em pânico nomeado de propósito (segindex/wip.go), então um WIP
// que continuasse no mapa depois de uma falha parcial derrubaria o processo no
// Close em vez de devolver o erro que já está sendo devolvido aqui.
//
// O Fecha acontece DENTRO do selMu, e não só a troca do mapa: uma consulta que
// tivesse pegado o ponteiro caminharia a cadeia de um mapeamento desmontado, e
// o WIP responde a isso com o pânico nomeado do requireOpen. Fechar sob o mesmo
// lock que a consulta segura durante o Lookup é o que torna a janela
// inexistente em vez de improvável.
//
// Entre este fecha e o openWIPsOfCurrent seguinte o mapa fica vazio, e a
// consulta trata isso como qualquer outro segmento sem índice: cai para o
// caminho selado, que responde pelo .sidx quando ele existe ou manda enumerar.
// A sub-janela seguinte — mapa REPOVOADO com os WIPs do segmento novo e uma
// consulta ainda com snapshot velho do corrente — é fechada pela identidade
// wipsSec, conferida no sourceOfWIP: sem ela, a consulta receberia as postings
// do segmento errado com ok=true e o segmento antigo sumiria dos candidatos.
func (s *Store) closeWIPs() error {
	s.selMu.Lock()
	defer s.selMu.Unlock()
	var errs []error
	for _, w := range s.wips {
		errs = append(errs, w.Close())
	}
	s.wips = nil
	s.wipsSec = 0
	return errors.Join(errs...)
}

// aposAppend registra o segmento devolvido pelo Append e, quando ele mudou,
// sela o anterior e abre os WIPs do novo.
//
// Um ponto só, e não um por caller, porque a rotação acontece DENTRO do
// segment.Writer.Append (segment.go:195) e é invisível a quem chama — os
// callers só veem o secID de volta. Cópias divergiriam, e a que esquecesse de
// selar deixaria um segmento com .widx que o Open descarta: um índice inteiro
// jogado fora, sem erro e sem log.
//
// A SELAGEM RODA UMA VEZ SÓ POR SEGMENTO, e o mecanismo é o Swap: quem lê o
// valor anterior é quem acabou de trocá-lo, e uma segunda chamada com o mesmo
// novoSeg vê anterior == novoSeg e sai. Isso resolve, por construção, o
// meio-termo que a selagem admite — .sidx durável com .widx de ALGUNS campos
// ainda de pé, quando o RemoveWIPs falha no meio de um segmento multi-campo. Um
// Sela reinvocado nesse estado ENCOLHERIA o .sidx, porque o Escritor novo só
// enxergaria os campos sobreviventes; aqui a reinvocação não existe. O que
// sobrar desse meio-termo através de um restart é problema do Open, que por
// contrato NUNCA adota .widx (spec §2.5).
//
// Erro daqui é erro de durabilidade pós-Append: quem chama o converte em writer
// FALHO sticky, como faz com o applyRecord. Retentar a selagem depois de ela ter
// falhado seria afirmar uma durabilidade que não aconteceu.
//
// Chamado sob wmu, como os curSecID.Store que substitui.
func (s *Store) aposAppend(newSeg uint32) error {
	anterior := s.curSecID.Swap(newSeg)
	// anterior == 0 é o Store que ainda não adotou segmento nenhum, e não
	// acontece depois do Open — os dois ramos dele publicam um secID >= 1. A
	// guarda fica porque o preço dela é uma comparação e o preço de faltar é
	// selar o "segmento 0", que não existe.
	if anterior == newSeg || anterior == 0 || len(s.fieldsIndexados) == 0 {
		return nil
	}
	if err := s.closeWIPs(); err != nil {
		return err
	}
	// Fechar ANTES de selar não é ordem arbitrária: o postCount só vai para o
	// header no Fecha (segindex/wip.go), e a selagem reabre o .widx pelo
	// arquivo. Selar com o mapeamento ainda de pé leria um header atrasado e
	// perderia as postings gravadas desde o último rehash.
	if err := segindex.Seal(s.dirOfData(), anterior); err != nil {
		return err
	}
	return s.openWIPsOfCurrent()
}

// aplicaIndexSets liga, no índice do segmento CORRENTE, uma posting para cada
// campo indexado presente no payload.
//
// Entra CADA campo indexado do payload novo, e não só o que mudou — é aqui que
// este motor difere do que ele substitui, e a diferença é obrigatória. A
// posting vive no índice do segmento em que o record está; quando um update (ou
// o copy-forward da compaction, que reescreve o record com payload IDÊNTICO)
// move a linha para o segmento novo, pular o campo inalterado deixaria a única
// posting dela no segmento ANTIGO — que a compaction ou a retenção aposenta, e
// com ele a posting. O resultado seria falso NEGATIVO, que é a única coisa que
// o §44 não perdoa. O preço da forma correta é uma posting por linha por campo
// por segmento, que é exatamente a conta de disco da spec §1.3.
//
// Por isso também não há parâmetro `prev`: não existe diff a fazer.
//
// NÃO existe fase de CLEAR para o índice de segmento, e a ausência é o desenho.
// Quando uma linha troca de valor, a posting antiga mora no índice do segmento
// ANTIGO: ela vira falso positivo, que a revalidação do §44 já descarta, e
// morre quando a compaction ou a retenção aposenta aquele segmento. Com o Clear
// somem a lápide e o falso negativo que ela existia para evitar. O preço é uma
// taxa de falso positivo governada por liveRecords/totalRecords — o mesmo
// número que a compaction já persegue e que o manifest já publica.
//
// Zero alocação por evento: o buffer da chave canônica é do Store e é sempre
// reatribuído com o slice devolvido, como o resto do caminho de escrita.
func (s *Store) aplicaIndexSets(row types.RowID, payload []byte) error {
	var r encoding.FieldReader
	r.Reset(payload)
	for {
		id, v, ok := r.Next()
		if !ok {
			return r.Err()
		}
		w := s.wips[id]
		if w == nil {
			continue
		}
		key, err := valuekey.Append(s.keyIndiceBuf[:0], v)
		if err != nil {
			return err
		}
		s.keyIndiceBuf = key
		// O seam de erro fica ANTES do Set, e é o único ponto do produto que
		// produz o phantom commit do §14.12 sob demanda: um erro daqui é
		// posterior ao Append e anterior à publicação da location, que é
		// exatamente a janela sem rollback parcial. Ver o godoc do campo.
		if s.setErrHook != nil {
			if err := s.setErrHook(id); err != nil {
				return err
			}
		}
		// primary.HashKey é a MESMA função que o índice primário usa, e a
		// escolha não é gosto: ela nunca devolve 0 nem 1, e o Set do WIP entra
		// em pânico com hash 0 porque 0 é o sentinela de slot vazio.
		if err := w.Set(primary.HashKey(key), row); err != nil {
			return err
		}
	}
}

// removeIndexOfSegment apaga o índice de um segmento aposentado: o .sidx e
// também os .widx que tenham sobrado dele.
//
// O .widx não é hipótese: um crash entre a rotação e a selagem deixa .widx de um
// segmento que já não é o corrente, e o descarte da abertura só alcança o
// corrente. Sem esta remoção, o .dat e o .sidx sairiam e o .widx ficaria em
// disco PARA SEMPRE — e como o IndexBytes passou a contar os arquivos de índice
// que moram em data/, ele viraria disco visível que nada explica, que é
// exatamente o modo de falha que aquela contagem existe para eliminar
// (spec §2.5, terceiro caso).
//
// Erro NÃO falha a aposentadoria: quando esta função roda, o .dat já saiu e o
// disco já voltou ao dispositivo — desfazer não é possível, e propagar o erro
// faria a passada de retenção parar por um resíduo inofensivo: índice sem dado
// devolve candidatos que não resolvem, e o executor já os descarta na
// revalidação (§44). A spec §2.3 manda limpá-lo no Open, no mesmo lugar em que
// o .retired já é limpo. Fica o WARN nomeado, para que o resíduo tenha rastro
// em vez de silêncio.
//
// Arquivo ausente não é anomalia: o segmento pode nunca ter tido índice (fora
// da janela, ou gravado por um binário anterior).
func (s *Store) removeIndexOfSegment(sec uint32) {
	if _, err := s.removeSidxOfDisk(sec); err != nil {
		s.opts.Log.Warn("indice.orfao_apos_aposentadoria",
			"o índice do segmento aposentado continuou em disco; ele é inofensivo e sai na limpeza de órfãos",
			"segmento", sec, "erro", err.Error())
	}
	if err := s.deleteWIPsPeloName(sec); err != nil {
		s.opts.Log.Warn("indice.wip_orfao_apos_aposentadoria",
			"sobrou .widx de um segmento aposentado; ele ocupa disco que o IndexBytes conta e nada explica",
			"segmento", sec, "erro", err.Error())
	}
}

// removeSidxOfDisk apaga o .sidx do segmento e SÓ DEPOIS larga o mapeamento em
// cache — nesta ordem, e a ordem é a correção de uma corrida.
//
// Com o esquecimento primeiro, uma consulta que lesse a geração DEPOIS do bump
// e abrisse o arquivo ANTES do unlink passava pelas duas guardas do
// publicaSealed e instalava no cache o mapeamento de um arquivo já removido —
// e nada o evictava até a próxima evição DESTE segmento: inode preso, e o
// segmento respondendo por um índice que a expulsão tinha mandado embora. Com o
// arquivo fora do disco primeiro, a abertura tardia falha com ENOENT; a que
// começou antes ou publica com geração velha (descartada) ou é removida pelo
// esquecimento que vem em seguida. Não há terceira janela.
//
// A ordem NÃO muda quando o disco volta ao dispositivo: o munmap espera a
// última Fonte em voo (contagem de vidas) nos dois desenhos — o unlink só
// remove o nome. A justificativa antiga da ordem inversa afirmava o contrário,
// e estava errada.
//
// UM helper para os três donos da remoção — aposentadoria, expulsão da janela e
// o obsoleto da reconstrução —, porque uma segunda cópia divergiria na primeira
// vez que a ordem mudasse. Arquivo ausente não é erro — é o estado normal de
// segmento que nunca teve índice —, mas volta como removido=false: a expulsão
// da janela conta só o que de fato saiu do disco.
func (s *Store) removeSidxOfDisk(sec uint32) (bool, error) {
	err := os.Remove(segindex.SealedPath(s.dirOfData(), sec))
	if s.entreRemoverEEsquecerHook != nil {
		s.entreRemoverEEsquecerHook(sec)
	}
	s.esqueceSealed(sec)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, os.ErrNotExist):
		return false, nil
	default:
		return false, err
	}
}

// esqueceSealed tira o índice do segmento do cache de mapeamentos, fechando-o.
//
// Os donos são os caminhos que mudam o .sidx por fora do cache — a remoção
// (removeSidxOfDisk, que serve à aposentadoria, à expulsão da janela e ao
// obsoleto da reconstrução) e a reescrita (ConstroiIndexOfSegment) — e todos
// precisam da MESMA sequência: fechar sob o selMu e tirar do mapa. A regra de
// ORDEM vale para todos: o esquecimento vem DEPOIS da operação de arquivo
// (unlink ou rename), senão uma consulta que abrisse o arquivo entre o bump da
// geração e a operação publicaria o mapeamento do estado anterior sem ninguém
// para evictá-lo — ver o godoc do removeSidxOfDisk.
//
// O que ela usa é o mecanismo que já existe, e não um segundo: o Fecha do Sealed
// impede leitores NOVOS e adia o munmap até a última Fonte em voo sair (contagem
// de vidas, segindex/sealed.go). Sem essa espera o desfecho não seria resultado
// degradado, seria SIGSEGV — sinal incapturável, longe da linha que errou. O
// selMu é o que impede um Lookup de entrar no Sealed depois do Fecha; ele é lock
// FOLHA, e nada sob ele toma o wmu.
//
// Depois dela, o segmento volta a ser resolvido pelo garanteSealed na consulta
// seguinte: sem cache, o arquivo é reaberto — que é exatamente o que a reescrita
// precisa, porque o mmap antigo sobrevive ao rename e continuaria servindo o
// conteúdo anterior.
//
// A GERAÇÃO SOBE SEMPRE, inclusive quando não havia nada em cache, e isso não é
// contabilidade: é a correção inteira da terceira aresta. O caso perigoso é
// justamente o mapa VAZIO — uma consulta abriu o arquivo velho, ainda não o
// publicou, e a reescrita acontece nesse meio. Se a geração só subisse quando
// houvesse entrada a remover, essa consulta publicaria o mapeamento velho depois
// da reescrita e o segmento responderia pelo índice anterior até a próxima
// abertura do dataset.
//
// Erro do desmonte não sobe: quem chama está aposentando ou republicando um
// índice, e não há resposta operacional para um munmap que falhou além do log.
func (s *Store) esqueceSealed(sec uint32) {
	s.selMu.Lock()
	defer s.selMu.Unlock()
	s.selGeneration++
	sel := s.selados[sec]
	if sel == nil {
		return
	}
	if err := sel.Close(); err != nil {
		s.opts.Log.Warn("indice.selado_nao_fechou",
			"o mapeamento do índice do segmento não pôde ser desmontado",
			"segmento", sec, "erro", err.Error())
	}
	delete(s.selados, sec)
}

// closeIndices desmonta tudo que o índice por segmento mantém mapeado. Roda no
// Close, junto do resto dos índices, e tolera Store meio aberto: o Open chama
// Close no caminho de erro.
func (s *Store) closeIndices() error {
	errs := []error{s.closeWIPs()}
	s.selMu.Lock()
	for _, sel := range s.selados {
		errs = append(errs, sel.Close())
	}
	s.selados = nil
	s.selMu.Unlock()
	return errors.Join(errs...)
}
