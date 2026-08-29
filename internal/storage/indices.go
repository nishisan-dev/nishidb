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
// resposta é estável — é a mesma promessa que HasBitmap e HasHash já fazem.
//
// Cópia, e não o slice interno: o conjunto é imutável por contrato, e devolver
// a fatia de dentro deixaria qualquer chamador quebrar a promessa por engano.
func (s *Store) CamposIndexados() []types.FieldID { return slices.Clone(s.camposIndexados) }

// abreIndicesDeSegmento fixa o conjunto indexado e abre os índices mutáveis do
// segmento corrente.
//
// Roda no Open DEPOIS do ramo de recovery, e a posição não é escolha de estilo:
// os campos vêm do resolveCamposIndexados (cuja ordem de LookupOrCreate é
// contratual e não pode ser tocada), mas os WIPs precisam do SEGMENTO CORRENTE,
// e quem o decide é o ramo de recovery — antes dele, curSegID ainda é zero num
// dataset existente.
func (s *Store) abreIndicesDeSegmento(campos []types.FieldID) error {
	s.camposIndexados = campos
	if err := s.descartaWIPsDoCorrente(); err != nil {
		return err
	}
	if len(campos) == 0 {
		return nil
	}
	s.selados = make(map[uint32]*segindex.Selado)
	return s.abreWIPsDoCorrente()
}

// descartaWIPsDoCorrente apaga os .widx que o segmento corrente trouxe de uma
// execução anterior.
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
// A remoção é por os.Remove sobre o NOME, e não pelo Apaga do segindex, porque
// o Apaga precisa mapear e validar o arquivo antes — exatamente o que um .widx
// rasgado não permite.
//
// Esta é a primeira METADE do §2.5: descartar. A segunda — reconstruir o índice
// do segmento corrente lendo o próprio segmento — é da tarefa seguinte, e a
// ausência dela não deixa nada incorreto aqui, porque nenhuma consulta lê deste
// motor ainda.
func (s *Store) descartaWIPsDoCorrente() error {
	seg := s.curSegID.Load()
	campos, err := segindex.ListaWIP(s.dirDeDados(), seg)
	if err != nil {
		return err
	}
	for _, campo := range campos {
		caminho := segindex.CaminhoWIP(s.dirDeDados(), seg, campo)
		if err := os.Remove(caminho); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("storage: descartar o índice %s do segmento corrente: %w", caminho, err)
		}
	}
	return nil
}

// uniaoDeCampos funde os conjuntos dos dois motores antigos em um só,
// ordenado e sem duplicatas — um campo declarado nos dois tem UM índice de
// segmento, não dois.
func uniaoDeCampos(a, b []types.FieldID) []types.FieldID {
	out := make([]types.FieldID, 0, len(a)+len(b))
	out = append(out, a...)
	out = append(out, b...)
	slices.Sort(out)
	return slices.Compact(out)
}

// abreWIPsDoCorrente abre um .widx por campo indexado para o segmento corrente.
//
// Falha no meio fecha o que já abriu: um WIP órfão manteria mapeamento de pé
// depois de o Open ter desistido, e o Close do Store não teria como alcançá-lo
// porque o mapa só é publicado no fim.
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
	s.wips = wips
	return nil
}

// fechaWIPs desmonta os índices mutáveis do segmento que deixou de ser o
// corrente.
//
// O mapa é zerado SEMPRE, inclusive no erro: Fecha depois de Fecha é uso após o
// fim e entra em pânico nomeado de propósito (segindex/wip.go), então um WIP
// que continuasse no mapa depois de uma falha parcial derrubaria o processo no
// Close em vez de devolver o erro que já está sendo devolvido aqui.
func (s *Store) fechaWIPs() error {
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
		// primary.HashKey é a MESMA função que o índice primário usa, e a
		// escolha não é gosto: ela nunca devolve 0 nem 1, e o Set do WIP entra
		// em pânico com hash 0 porque 0 é o sentinela de slot vazio.
		if err := w.Set(primary.HashKey(chave), row); err != nil {
			return err
		}
	}
}

// removeIndiceDoSegmento apaga o .sidx de um segmento aposentado.
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
	s.selMu.Lock()
	if sel := s.selados[seg]; sel != nil {
		if err := sel.Fecha(); err != nil {
			s.opts.Log.Warn("indice.selado_nao_fechou",
				"o mapeamento do índice do segmento aposentado não pôde ser desmontado",
				"segmento", seg, "erro", err.Error())
		}
		delete(s.selados, seg)
	}
	s.selMu.Unlock()
	if err := os.Remove(segindex.CaminhoSelado(s.dirDeDados(), seg)); err != nil && !errors.Is(err, os.ErrNotExist) {
		s.opts.Log.Warn("indice.orfao_apos_aposentadoria",
			"o índice do segmento aposentado continuou em disco; ele é inofensivo e sai na limpeza de órfãos",
			"segmento", seg, "erro", err.Error())
	}
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
