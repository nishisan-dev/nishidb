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
	"github.com/nishisan-dev/nishidb/internal/types"
)

// slotsIniciaisDoWIP dimensiona o diretório de um .widx recém-criado.
//
// O número de valores DISTINTOS que um segmento vai ter é desconhecido no
// instante em que o arquivo nasce, e não há de onde derivá-lo: EstimatedRows
// mede linhas do dataset inteiro, não distintos de um segmento. Então o valor é
// uma aposta, e ela é barata dos dois lados — 1.024 slots são 32 KiB de
// diretório mais postings iniciais, pagos por (segmento corrente, campo) e não
// por dataset, e errar para baixo custa apenas os rehashes amortizados que o
// próprio WIP faz. O que 1.024 compra é pular a dezena de rehashes iniciais que
// um piso de 16 slots pagaria em todo segmento de todo campo.
const slotsIniciaisDoWIP = 1024

// dirDeDados é onde o índice por segmento mora: AO LADO do .dat, e não num
// diretório próprio, porque o par (dado, índice) nasce e morre junto — é essa
// vizinhança que faz "purgar o segmento leva o índice junto" ser uma linha de
// código. O segment.ListSegments casa só o nome canônico exato, então os
// arquivos novos não perturbam a listagem, a recovery nem o manifest.
func (s *Store) dirDeDados() string { return filepath.Join(s.opts.Dir, "data") }

// CamposIndexados devolve, em ordem crescente, os campos com índice por
// segmento. O conjunto é resolvido no Open e IMUTÁVEL na vida do Store, então a
// resposta é estável — é a mesma promessa que o Indexado faz.
//
// Cópia, e não o slice interno: o conjunto é imutável por contrato, e devolver
// a fatia de dentro deixaria qualquer chamador quebrar a promessa por engano.
func (s *Store) CamposIndexados() []types.FieldID { return slices.Clone(s.camposIndexados) }

// abreIndicesDeSegmento fixa o conjunto indexado, descarta os WIPs herdados do
// processo anterior, abre os do segmento corrente e os RECONSTRÓI a partir dos
// records dele. É a §2.5 inteira, num ponto só.
//
// Roda no Open DEPOIS do ramo de recovery, e a posição não é escolha de estilo:
// os campos vêm do resolveCamposIndexados (cuja ordem de LookupOrCreate é
// contratual e não pode ser tocada), mas os WIPs precisam do SEGMENTO CORRENTE,
// e quem o decide é o ramo de recovery — antes dele, curSegID ainda é zero num
// dataset existente. E roda ANTES de o Open devolver o Store, o que é a outra
// metade do requisito: o primeiro Put não pode ser aceito sobre um WIP vazio,
// porque a rotação seguinte selaria esse vazio num .sidx IMUTÁVEL — falso
// negativo petrificado, e a revalidação do §44 só sabe descartar candidato a
// mais.
func (s *Store) abreIndicesDeSegmento(campos []types.FieldID) error {
	s.camposIndexados = campos
	if err := s.descartaTodosOsWIPs(); err != nil {
		return err
	}
	if len(campos) == 0 {
		return nil
	}
	s.selados = make(map[uint32]*segindex.Selado)
	if err := s.abreWIPsDoCorrente(); err != nil {
		return err
	}
	return s.reconstroiOIndiceDoCorrente()
}

// descartaTodosOsWIPs apaga TODO .widx que o diretório de dados carregue de uma
// execução anterior — do segmento corrente e de qualquer outro.
//
// O .widx NUNCA é confiado através de um restart (spec §2.5), e a razão não é
// prudência: o postCount só vai para o header no Fecha, então um processo morto
// por SIGKILL deixa um header ATRASADO sobre um diretório adiantado — slots que
// declaram cabeça e contagem contra um total que o arquivo não confirma. O
// próprio segindex recusa esse arquivo com ErrFormato, e adotá-lo seria trocar
// uma abertura que funciona por um dataset inacessível. Medido: sem este
// descarte, o TestKill9NoMeioDaIngestaoPreservaOEstadoConfirmado passa a
// reabrir com "o slot 572 declara contagem 10426 contra 0 postings".
//
// UM ato só para os três casos da §2.5, e é isso que justifica a varredura ser
// do diretório inteiro: o que muda entre eles não é o descarte, é o que vem
// DEPOIS dele.
//
//	corrente               → reconstroiOIndiceDoCorrente, aqui mesmo, lendo o
//	                         próprio segmento antes do primeiro Put;
//	não corrente, com .dat → SegmentosPendentesDeReconstrucao o devolve, e o
//	                         laço de fundo (§2.4) o refaz quando puder;
//	sem .dat               → nada. O segmento foi aposentado, e o .widx é o
//	                         resíduo de um crash entre o unlink do dado e a
//	                         limpeza do índice. Sem esta varredura ele ficaria
//	                         em disco PARA SEMPRE, porque nenhuma enumeração que
//	                         parta dos dados o alcança — e o IndexBytes o
//	                         contaria como disco que nada explica.
func (s *Store) descartaTodosOsWIPs() error {
	segs, err := segindex.SegmentosComWIP(s.dirDeDados())
	if err != nil {
		return classificaNomeIlegivelDeIndice(err, s.dirDeDados())
	}
	for _, seg := range segs {
		if err := s.apagaWIPsPeloNome(seg); err != nil {
			return classificaNomeIlegivelDeIndice(err, s.dirDeDados())
		}
	}
	return nil
}

// classificaNomeIlegivelDeIndice põe o ErrFormato do espaço de nomes .widx na
// sentinela que o exit code entende.
//
// Sem isto o erro sobe cru e cai no default do codigoDoErro (cli.go) — exit 1,
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
func classificaNomeIlegivelDeIndice(err error, dir string) error {
	if err == nil || !errors.Is(err, segindex.ErrFormato) {
		return err
	}
	return fmt.Errorf("%w: %s tem um arquivo no espaço de nomes .widx que este binário não escreveu: %w",
		ErrDatasetCorrupt, dir, err)
}

// apagaWIPsPeloNome remove os .widx do segmento SEM abrir nenhum deles.
//
// É o único lugar que apaga .widx fora da selagem, e ele é um só de propósito:
// os dois consumidores — o descarte da abertura e a aposentadoria do segmento —
// precisam da MESMA regra, e uma segunda cópia divergiria na primeira vez que a
// regra mudasse.
//
// Por NOME, e nunca pelo segindex.ApagaWIPs: aquele abre o arquivo com o
// AbreWIP, que mapeia e valida o header antes de remover. Um .widx rasgado por
// crash é justamente o que não se deixa abrir — a limpeza do lixo não pode
// depender de o lixo estar íntegro (spec §2.5).
func (s *Store) apagaWIPsPeloNome(seg uint32) error {
	campos, err := segindex.ListaWIP(s.dirDeDados(), seg)
	if err != nil {
		return err
	}
	for _, campo := range campos {
		caminho := segindex.CaminhoWIP(s.dirDeDados(), seg, campo)
		if err := os.Remove(caminho); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("storage: remover o índice WIP %s: %w", caminho, err)
		}
	}
	return nil
}

// abreWIPsDoCorrente abre um .widx por campo indexado para o segmento corrente.
//
// Falha no meio fecha o que já abriu: um WIP órfão manteria mapeamento de pé
// depois de o Open ter desistido, e o Close do Store não teria como alcançá-lo
// porque o mapa só é publicado no fim.
//
// Os arquivos são abertos FORA do selMu e o mapa é publicado sob ele: a
// consulta lê s.wips pelo mesmo lock, e segurá-lo por N mmaps travaria toda
// leitura do dataset atrás de uma rotação de segmento.
func (s *Store) abreWIPsDoCorrente() error {
	if len(s.camposIndexados) == 0 {
		return nil
	}
	seg := s.curSegID.Load()
	wips := make(map[types.FieldID]*segindex.WIP, len(s.camposIndexados))
	for _, campo := range s.camposIndexados {
		w, err := segindex.AbreWIP(s.dirDeDados(), seg, campo, slotsIniciaisDoWIP)
		if err != nil {
			for _, aberto := range wips {
				_ = aberto.Fecha()
			}
			return err
		}
		wips[campo] = w
	}
	s.selMu.Lock()
	s.wips = wips
	s.selMu.Unlock()
	return nil
}

// fechaWIPs desmonta os índices mutáveis do segmento que deixou de ser o
// corrente.
//
// O mapa é zerado SEMPRE, inclusive no erro: Fecha depois de Fecha é uso após o
// fim e entra em pânico nomeado de propósito (segindex/wip.go), então um WIP
// que continuasse no mapa depois de uma falha parcial derrubaria o processo no
// Close em vez de devolver o erro que já está sendo devolvido aqui.
//
// O Fecha acontece DENTRO do selMu, e não só a troca do mapa: uma consulta que
// tivesse pegado o ponteiro caminharia a cadeia de um mapeamento desmontado, e
// o WIP responde a isso com o pânico nomeado do exigeAberto. Fechar sob o mesmo
// lock que a consulta segura durante o Lookup é o que torna a janela
// inexistente em vez de improvável.
//
// Entre este fecha e o abreWIPsDoCorrente seguinte o mapa fica vazio, e a
// consulta trata isso como qualquer outro segmento sem índice: ENUMERA. É
// correto por construção e dispensa raciocinar sobre a ordem entre a troca do
// curSegID e a publicação da location do record que causou a rotação.
func (s *Store) fechaWIPs() error {
	s.selMu.Lock()
	defer s.selMu.Unlock()
	var errs []error
	for _, w := range s.wips {
		errs = append(errs, w.Fecha())
	}
	s.wips = nil
	return errors.Join(errs...)
}

// aposAppend registra o segmento devolvido pelo Append e, quando ele mudou,
// sela o anterior e abre os WIPs do novo.
//
// Um ponto só, e não um por caller, porque a rotação acontece DENTRO do
// segment.Writer.Append (segment.go:195) e é invisível a quem chama — os
// callers só veem o segID de volta. Cópias divergiriam, e a que esquecesse de
// selar deixaria um segmento com .widx que o Open descarta: um índice inteiro
// jogado fora, sem erro e sem log.
//
// A SELAGEM RODA UMA VEZ SÓ POR SEGMENTO, e o mecanismo é o Swap: quem lê o
// valor anterior é quem acabou de trocá-lo, e uma segunda chamada com o mesmo
// novoSeg vê anterior == novoSeg e sai. Isso resolve, por construção, o
// meio-termo que a selagem admite — .sidx durável com .widx de ALGUNS campos
// ainda de pé, quando o ApagaWIPs falha no meio de um segmento multi-campo. Um
// Sela reinvocado nesse estado ENCOLHERIA o .sidx, porque o Escritor novo só
// enxergaria os campos sobreviventes; aqui a reinvocação não existe. O que
// sobrar desse meio-termo através de um restart é problema do Open, que por
// contrato NUNCA adota .widx (spec §2.5).
//
// Erro daqui é erro de durabilidade pós-Append: quem chama o converte em writer
// FALHO sticky, como faz com o applyRecord. Retentar a selagem depois de ela ter
// falhado seria afirmar uma durabilidade que não aconteceu.
//
// Chamado sob wmu, como os curSegID.Store que substitui.
func (s *Store) aposAppend(novoSeg uint32) error {
	anterior := s.curSegID.Swap(novoSeg)
	// anterior == 0 é o Store que ainda não adotou segmento nenhum, e não
	// acontece depois do Open — os dois ramos dele publicam um segID >= 1. A
	// guarda fica porque o preço dela é uma comparação e o preço de faltar é
	// selar o "segmento 0", que não existe.
	if anterior == novoSeg || anterior == 0 || len(s.camposIndexados) == 0 {
		return nil
	}
	if err := s.fechaWIPs(); err != nil {
		return err
	}
	// Fechar ANTES de selar não é ordem arbitrária: o postCount só vai para o
	// header no Fecha (segindex/wip.go), e a selagem reabre o .widx pelo
	// arquivo. Selar com o mapeamento ainda de pé leria um header atrasado e
	// perderia as postings gravadas desde o último rehash.
	if err := segindex.Sela(s.dirDeDados(), anterior); err != nil {
		return err
	}
	return s.abreWIPsDoCorrente()
}

// aplicaIndiceSets liga, no índice do segmento CORRENTE, uma posting para cada
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
func (s *Store) aplicaIndiceSets(row types.RowID, payload []byte) error {
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
		chave, err := valuekey.Append(s.chaveIndiceBuf[:0], v)
		if err != nil {
			return err
		}
		s.chaveIndiceBuf = chave
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
		if err := w.Set(primary.HashKey(chave), row); err != nil {
			return err
		}
	}
}

// removeIndiceDoSegmento apaga o índice de um segmento aposentado: o .sidx e
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
func (s *Store) removeIndiceDoSegmento(seg uint32) {
	s.esqueceSelado(seg)
	if err := os.Remove(segindex.CaminhoSelado(s.dirDeDados(), seg)); err != nil && !errors.Is(err, os.ErrNotExist) {
		s.opts.Log.Warn("indice.orfao_apos_aposentadoria",
			"o índice do segmento aposentado continuou em disco; ele é inofensivo e sai na limpeza de órfãos",
			"segmento", seg, "erro", err.Error())
	}
	if err := s.apagaWIPsPeloNome(seg); err != nil {
		s.opts.Log.Warn("indice.wip_orfao_apos_aposentadoria",
			"sobrou .widx de um segmento aposentado; ele ocupa disco que o IndexBytes conta e nada explica",
			"segmento", seg, "erro", err.Error())
	}
}

// esqueceSelado tira o índice do segmento do cache de mapeamentos, fechando-o.
//
// DOIS donos, UMA regra: a aposentadoria do segmento (removeIndiceDoSegmento) e
// a reescrita do .sidx pela reconstrução (ConstroiIndiceDoSegmento). Os dois
// precisam da MESMA sequência — fechar sob o selMu e tirar do mapa —, e uma
// segunda cópia divergiria na primeira vez que a regra mudasse.
//
// O que ela usa é o mecanismo que já existe, e não um segundo: o Fecha do Selado
// impede leitores NOVOS e adia o munmap até a última Fonte em voo sair (contagem
// de vidas, segindex/selado.go). Sem essa espera o desfecho não seria resultado
// degradado, seria SIGSEGV — sinal incapturável, longe da linha que errou. O
// selMu é o que impede um Lookup de entrar no Selado depois do Fecha; ele é lock
// FOLHA, e nada sob ele toma o wmu.
//
// Depois dela, o segmento volta a ser resolvido pelo garanteSelado na consulta
// seguinte: sem cache, o arquivo é reaberto — que é exatamente o que a reescrita
// precisa, porque o mmap antigo sobrevive ao rename e continuaria servindo o
// conteúdo anterior.
//
// Erro do desmonte não sobe: quem chama está aposentando ou republicando um
// índice, e não há resposta operacional para um munmap que falhou além do log.
func (s *Store) esqueceSelado(seg uint32) {
	s.selMu.Lock()
	defer s.selMu.Unlock()
	sel := s.selados[seg]
	if sel == nil {
		return
	}
	if err := sel.Fecha(); err != nil {
		s.opts.Log.Warn("indice.selado_nao_fechou",
			"o mapeamento do índice do segmento não pôde ser desmontado",
			"segmento", seg, "erro", err.Error())
	}
	delete(s.selados, seg)
}

// fechaIndices desmonta tudo que o índice por segmento mantém mapeado. Roda no
// Close, junto do resto dos índices, e tolera Store meio aberto: o Open chama
// Close no caminho de erro.
func (s *Store) fechaIndices() error {
	errs := []error{s.fechaWIPs()}
	s.selMu.Lock()
	for _, sel := range s.selados {
		errs = append(errs, sel.Fecha())
	}
	s.selados = nil
	s.selMu.Unlock()
	return errors.Join(errs...)
}
