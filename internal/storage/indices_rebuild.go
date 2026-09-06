package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/nishisan-dev/nishidb/internal/index/primary"
	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/storage/mmap"
	"github.com/nishisan-dev/nishidb/internal/storage/segment"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// errSegmentoEhOCorrente marca a recusa da guarda de segmento corrente do
// constroiIndiceDoSegmento, por baixo do ErrInvalidArgument que o chamador
// externo enxerga. A sentinela existe porque o laço de fundo precisa separar
// esta recusa do ErrInvalidArgument genuíno (campo não indexado, que é bug do
// chamador e TEM de parar o laço): a fila é uma fotografia, e o .dat do
// segmento novo nasce antes de o aposAppend trocar o curSegID — o segmento
// pode ser listado como pendente e JÁ ser o corrente quando o laço o alcança.
var errSegmentoEhOCorrente = errors.New("o segmento é o corrente")

// IniciaReconstrucao sobe a goroutine que consome a fila de reconstrução por
// conta própria (§2.4). É ela que fecha o débito 22: até aqui o mecanismo
// existia e a fila era derivada corretamente, mas NADA a drenava — o operador
// declarava `index: true` num dataset povoado, os segmentos entravam na fila e
// o campo continuava enumerando indefinidamente, que é o mesmo desfecho de
// antes por outro motivo.
//
// Molde de IniciaCompaction e IniciaRetencao: primeira passada imediata,
// medidor atômico observável, cancelamento por contexto, e o Close cancela e
// AGUARDA antes do quiesce.
//
// A JANELA É PARÂMETRO, e tem de ser a MESMA que a expulsão usa. Se o laço e a
// expulsão responderem "este segmento deve ter índice?" por contas diferentes, a
// que responde "sim" reconstrói o que a outra acabou de apagar — para sempre, a
// até 512 MiB de leitura por volta. Quem chama pelo servidor tira as duas do
// mesmo `janelaDoDataset`, e é por isso que ela entra pela assinatura em vez de
// ficar guardada no Store: o compilador cobra o que um comentário não cobraria.
//
// Chamar duas vezes não sobe duas goroutines. Duas passadas concorrentes
// varreriam o mesmo segmento e publicariam o mesmo arquivo duas vezes; pior, a
// segunda partida sobrescreveria o recCancela, e o ParaReconstrucao do Close
// ficaria esperando para sempre pela primeira.
//
// Dataset SEM campo indexado não sobe goroutine: não há fila, e a passada seria
// um readdir por minuto para responder sempre a mesma coisa. Janela finita sobre
// um dataset materializado é ERRO de quem monta os limites (a janela é política
// exclusiva do histórico), e recusar aqui — com o ERROR nomeado — é o que impede
// o laço de virar uma enxurrada de log de uma passada que nunca pode dar certo.
func (s *Store) IniciaReconstrucao(ctx context.Context, j JanelaDeIndice) {
	if len(s.camposIndexados) == 0 {
		return
	}
	if err := s.exigeJanelaAplicavel(j); err != nil {
		s.opts.Log.Error("reconstrucao.janela_recusada",
			"a janela de indexação não vale para este dataset e a reconstrução NÃO subiu; os segmentos pendentes "+
				"continuam respondendo por enumeração",
			"dataset", s.opts.Dataset, "erro", err.Error())
		return
	}
	s.recMu.Lock()
	defer s.recMu.Unlock()
	if s.recCancela != nil {
		return
	}
	ctx, cancela := context.WithCancel(ctx)
	s.recCancela = cancela
	// O medidor sobe AQUI, e não dentro da goroutine, pela regra do §92: quem
	// chamou tem direito de ver o estado que acabou de instalar, e uma leitura
	// entre a chamada e o primeiro escalonamento não pode responder que a
	// reconstrução não subiu.
	s.recAtiva.Store(true)
	s.recEmVoo.Add(1)
	go func() {
		defer s.recEmVoo.Done()
		// Declarado DEPOIS do Done e por isso executado ANTES dele: o
		// ParaReconstrucao devolve o controle já com o medidor em 0, e não com
		// uma janela em que a goroutine acabou e a série ainda diz 1.
		defer s.recAtiva.Store(false)
		s.rodaReconstrucao(ctx, j)
	}()
}

// ParaReconstrucao cancela a goroutine e ESPERA por ela. As duas coisas, e nesta
// ordem: o cancelamento sozinho devolveria o controle com uma varredura ainda
// lendo pelo s.segR, que o Close fecha inteiro logo em seguida — os.ErrClosed no
// meio de uma leitura de 512 MiB.
//
// É idempotente e pode ser chamada sem nunca ter havido reconstrução — é o que
// permite ao Close chamá-la sem perguntar se o dataset tem campo indexado.
func (s *Store) ParaReconstrucao() {
	s.recMu.Lock()
	cancela := s.recCancela
	s.recCancela = nil
	s.recMu.Unlock()
	if cancela != nil {
		cancela()
	}
	s.recEmVoo.Wait()
}

// ReconstrucaoAtiva reporta se a goroutine da §2.4 está de pé.
func (s *Store) ReconstrucaoAtiva() bool { return s.recAtiva.Load() }

// rodaReconstrucao é o laço perpétuo: uma passada, um cochilo, outra passada.
//
// A primeira passada é IMEDIATA. Um dataset que acabou de ganhar `index: true`
// num campo já povoado tem a fila cheia no instante da subida, e fazê-lo esperar
// o intervalo é adiar o único trabalho que tira o campo da enumeração.
//
// O PACING É O MESMO ORÇAMENTO DE CICLO DE TRABALHO DA COMPACTION —
// literalmente a mesma constante, não um número igual escrito duas vezes. Sem
// ele este laço vira o terceiro concorrente de fundo pelo disco, que é o defeito
// que o ciclo anterior mediu e nomeou. O cochilo entre passadas é o que garante
// que uma fila que nunca esvazia (ela é um superconjunto por construção) não
// vire varredura contínua.
//
// Para na PRIMEIRA falha, e para de vez, como a compaction e a retenção: o erro
// que chega aqui é falha de infra ou dataset recusado, e a passada seguinte
// reencontraria o mesmo estado e o mesmo erro. Quem observa isso é o medidor
// (que desce junto) — o log fala com quem já foi lê-lo. Cancelamento e dataset
// fechado NÃO são falha: são o Close, e anunciá-los como parada faria todo
// encerramento normal emitir um ERROR.
func (s *Store) rodaReconstrucao(ctx context.Context, j JanelaDeIndice) {
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		feitos, err := s.passadaDeReconstrucao(ctx, j)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, ErrDatasetClosed) {
				return
			}
			s.opts.Log.Error("reconstrucao.parou",
				"a reconstrução do índice por segmento parou e NÃO volta sozinha; os segmentos que faltam continuam "+
					"respondendo por enumeração até a reabertura do dataset (§2.4)",
				"dataset", s.opts.Dataset, "erro", err.Error())
			return
		}
		if feitos > 0 {
			s.opts.Log.Info("reconstrucao.passada",
				"a passada de reconstrução publicou o índice de segmentos que estavam sem ele",
				"dataset", s.opts.Dataset, "segmentos", feitos)
		}
		t.Reset(intervaloEntrePassadas)
	}
}

// passadaDeReconstrucao reconstrói, em ordem, o índice dos segmentos pendentes
// de UMA consulta à fila. Devolve quantos segmentos foram varridos.
//
// A fila é fotografada no começo e não é relida durante a passada, como a
// eleição da compaction: o que ela não alcançar, a próxima alcança.
//
// UMA PASSADA POR VEZ, pelo recPassadaMu — e ele é tomado ANTES do
// retPassadaMu (que a pendência toma lá dentro) e antes do entra() de cada
// segmento. A ordem de locks fica recPassadaMu → retPassadaMu → vida → wmu →
// aposMu, e nenhum dos internos volta a tomar os externos.
func (s *Store) passadaDeReconstrucao(ctx context.Context, j JanelaDeIndice) (int, error) {
	s.recPassadaMu.Lock()
	defer s.recPassadaMu.Unlock()
	pendentes, err := s.SegmentosPendentesDeReconstrucao(j)
	if err != nil {
		return 0, err
	}
	return s.reconstroiOsPendentes(ctx, pendentes)
}

// reconstroiOsPendentes é o corpo da passada sobre uma fila JÁ fotografada.
//
// Separado da consulta à fila para que o teste possa entregar uma fila que
// descreve um instante que já passou — que é exatamente o que a passada real
// recebe: entre a fotografia e a varredura, a retenção pode ter aposentado um
// dos segmentos.
//
// O chamador segura o recPassadaMu, e é ele que dá o happens-before do
// recPontoFixo: o mapa é tocado só daqui, e duas passadas nunca se sobrepõem.
func (s *Store) reconstroiOsPendentes(ctx context.Context, pendentes []uint32) (int, error) {
	s.podaOsPontosFixos(pendentes)
	var feitos int
	for _, seg := range pendentes {
		if err := ctx.Err(); err != nil {
			return feitos, err
		}
		fixo, err := s.noPontoFixo(seg)
		if err != nil {
			return feitos, err
		}
		if fixo {
			continue
		}
		publicou, err := s.reconstroiUmSegmento(ctx, seg)
		if err != nil {
			if oSegmentoSaiuDoDataset(err) {
				// A retenção ou a compaction aposentou o segmento entre a
				// fotografia da fila e esta varredura. Não é falha da passada e
				// não pode PARAR o laço: num histórico a aposentadoria acontece a
				// cada minuto, então tratá-la como erro faria o laço morrer na
				// primeira sobreposição e o dataset inteiro voltar a enumerar. O
				// segmento não existe mais, logo não há índice a construir para
				// ele, e ele já não aparece na fila da próxima passada.
				continue
			}
			if errors.Is(err, errSegmentoEhOCorrente) {
				// A corrida SIMÉTRICA da de cima: o .dat do segmento novo nasce
				// dentro do Append, ANTES de o aposAppend trocar o curSegID, então
				// a fotografia da fila pode listá-lo enquanto o Load ainda devolve
				// o anterior — e quando o laço o alcança, ele JÁ é o corrente. O
				// desfecho certo é pular: a própria rotação o sela quando ele
				// deixar de ser corrente. Parar aqui mataria o laço até a
				// reabertura do dataset por uma janela de milissegundos que se
				// resolve sozinha.
				continue
			}
			return feitos, fmt.Errorf("reconstruir o índice do segmento %d: %w", seg, err)
		}
		feitos++
		// O contador sobe por segmento que PUBLICOU índice, e não por segmento
		// varrido nem por passada. As três contagens são diferentes e cada uma
		// tem dono:
		//
		//   - `varreduras` conta LEITURAS de segmento, que é o custo;
		//   - `feitos` conta os visitados nesta passada, que é o que os testes
		//     da §2.4 afirmam sobre a convergência da fila;
		//   - `recFeitas` conta os que produziram arquivo, que é o que a série
		//     nishidb_index_reconstrucoes_total promete.
		//
		// Contar aqui o segmento que não publicou seria contar o que não
		// aconteceu — e pior de todos os jeitos possíveis: o segmento sem campo
		// indexado em record algum fica na fila DE PROPÓSITO (§2.4), então num
		// dataset estável ele faria a série crescer para sempre. Uma taxa que
		// nunca zera é exatamente o sintoma de um moinho, que é o que esta série
		// existe para denunciar.
		if publicou {
			s.recFeitas.Add(1)
		}
		if err := s.anotaOPontoFixo(seg); err != nil {
			return feitos, err
		}
	}
	return feitos, nil
}

// reconstroiUmSegmento é a reconstrução de UM segmento sob o lock de vida.
//
// O entra() é por SEGMENTO, e não por passada: segurá-lo pela passada inteira
// faria o Close esperar por todos os segmentos pendentes em vez de por um. Ele é
// a segunda linha de defesa, e não a primeira — quem garante que nenhuma
// varredura está em voo quando o Close fecha o segR é o ParaReconstrucao, que
// cancela e AGUARDA antes do quiesce. Este bracket cobre a passada chamada na
// mão (é assim que o teste alcança o dataset já fechado) e devolve
// ErrDatasetClosed em vez de os.ErrClosed vindo de dentro de um pread.
//
// Os campos pedidos são o conjunto indexado INTEIRO, e não só os que faltam, e a
// cobertura resultante é a mesma: o coberturaDaReconstrucao une o pedido com o
// que o arquivo já tem e continua declarado, e essa união é o próprio conjunto
// declarado. Pedir só os que faltam seria mais uma leitura da tabela de seções
// para chegar ao mesmo arquivo — e teria um buraco: um segmento pendente APENAS
// por ter a seção de um campo que deixou de ser indexado tem conjunto faltante
// VAZIO, e ConstroiIndiceDoSegmento com lista vazia não faz nada. Ele ficaria
// pendente para sempre, com a seção morta em disco, que é o outro lado da §2.6.
func (s *Store) reconstroiUmSegmento(ctx context.Context, seg uint32) (bool, error) {
	if err := s.entra(); err != nil {
		return false, err
	}
	defer s.sai()
	return s.constroiIndiceDoSegmento(ctx, seg, s.camposIndexados)
}

// oSegmentoSaiuDoDataset separa a corrida BENIGNA — o segmento aposentado entre
// a fotografia da fila e a varredura — do erro que tem de parar o laço.
//
// São os dois desfechos que a aposentadoria produz em quem está lendo: ENOENT
// quando o descritor ainda não estava aberto (o unlink já aconteceu) e
// os.ErrClosed quando o fechaODescritorEApaga fechou o fd cacheado no meio da
// varredura. A barreira daquele fechamento é o aposMu, e esta passada NÃO o toma
// pela varredura inteira de propósito: segurá-lo por até 512 MiB de leitura
// travaria toda consulta atrás do escritor pendente da aposentadoria, que é
// exatamente o que este ciclo existe para não fazer. A §2.4 já aceita o resíduo
// simétrico (publicar o .sidx de um segmento recém-aposentado, que o Open
// limpa), e este é o mesmo trade com o sinal trocado.
//
// Um segmento interrompido no meio NÃO publica índice parcial: o erro sobe antes
// do Escreve, e o arquivo só nasce de uma varredura completa.
func oSegmentoSaiuDoDataset(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrClosed)
}

// noPontoFixo responde se reconstruir este segmento produziria exatamente o
// arquivo que já está lá — isto é, se a passada anterior já fez tudo que havia
// para fazer e ele continua na fila por ser ela um superconjunto.
//
// A comparação é contra o conjunto de seções ANOTADO na última reconstrução, e
// não contra um booleano, porque é ela que faz a memória se invalidar sozinha
// quando o arquivo muda por fora — a expulsão da janela é o caso real, e é o
// gatilho "janela alargada" da §2.4.
//
// Erro de leitura sobe: um .sidx que não se deixa ler por EIO é falha de infra,
// e responder "está no ponto fixo" a partir dela silenciaria um disco doente
// (§101). Os estados que NÃO são anomalia — ausente, formato desconhecido, zero
// bytes — são "sem seção nenhuma", pela mesma lista que a fila usa
// (indiceIlegivel); ter duas listas diferentes daria um segmento que a fila
// julga refazível e a memória julga fatal.
func (s *Store) noPontoFixo(seg uint32) (bool, error) {
	anotado, tem := s.recPontoFixo[seg]
	if !tem {
		return false, nil
	}
	atual, err := s.secoesDoSelado(seg)
	if err != nil {
		return false, err
	}
	return slices.Equal(atual, anotado), nil
}

// anotaOPontoFixo registra o que o arquivo cobre DEPOIS da reconstrução.
//
// Só o segmento que continua PENDENTE precisa da anotação — quem ficou completo
// sai da fila sozinho —, mas anotar os dois custa uma entrada que a poda da
// passada seguinte remove, e é menos código do que a distinção.
func (s *Store) anotaOPontoFixo(seg uint32) error {
	atual, err := s.secoesDoSelado(seg)
	if err != nil {
		return err
	}
	if s.recPontoFixo == nil {
		s.recPontoFixo = make(map[uint32][]types.FieldID, 1)
	}
	s.recPontoFixo[seg] = atual
	return nil
}

// podaOsPontosFixos descarta a memória dos segmentos que já não estão na fila:
// os que ficaram completos, os que a aposentadoria removeu e os que saíram da
// janela. Sem ela, um dataset que aposenta segmentos o dia inteiro acumularia
// uma entrada por segmento morto, para sempre — o mesmo cuidado do
// podaTamanhosExpulsos.
//
// A busca é binária porque a fila sai ordenada por ID do ListSegments, e essa
// ordem é contratual.
func (s *Store) podaOsPontosFixos(pendentes []uint32) {
	for seg := range s.recPontoFixo {
		if _, achou := slices.BinarySearch(pendentes, seg); !achou {
			delete(s.recPontoFixo, seg)
		}
	}
}

// secoesDoSelado devolve os campos com seção no .sidx do segmento, com os três
// estados não-anômalos colapsados em "nenhuma seção".
func (s *Store) secoesDoSelado(seg uint32) ([]types.FieldID, error) {
	campos, err := segindex.CamposSelados(segindex.CaminhoSelado(s.dirDeDados(), seg))
	switch {
	case indiceIlegivel(err):
		return nil, nil
	case err != nil:
		return nil, err
	}
	return campos, nil
}

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
// a ALIMENTAÇÃO é equivalente, que é onde o erro moraria. A igualdade vale para a
// mesma COBERTURA, e é por isso que reconstruir só o campo que falta produz o
// mesmo arquivo que reconstruir tudo — a cobertura é a mesma nos dois casos
// (TestReconstruirSoOsCamposQueFaltamPreservaOsQueJaEstavam).
//
// Segmento sem posting nenhuma não gera arquivo (contrato de Escritor.Escreve):
// "sem índice" é um estado que o Lookup resolve por enumeração, e um .sidx vazio
// faria o segmento passar por indexado. Um .sidx ANTERIOR nesse caso é removido,
// porque ele só pode conter seções de campos que deixaram de ser indexados.
//
// `campos` são os campos PENDENTES daquele segmento, e podem ser um subconjunto
// do conjunto indexado — é a §2.6, que precisa acrescentar um campo ao schema de
// um dataset povoado. O arquivo publicado cobre a UNIÃO deles com o que o .sidx
// atual já cobria e continua declarado (coberturaDaReconstrucao), e a união é
// obrigatória: o arquivo é imutável, "acrescentar seção" é reescrevê-lo inteiro,
// e reescrevê-lo só com o campo pedido apagaria as seções que já estavam lá.
//
// A PUBLICAÇÃO NÃO TOMA O wmu, e a §2.4 emendada é explícita quanto a isso: a
// exigência anterior — tomá-lo "só no rename" — era inexequível, porque
// Escritor.Escreve faz escrita-tmp, fsync, rename e fsync do diretório numa
// chamada só, e segurar o lock pela escrita inteira é o que a mesma seção
// proíbe. O que substitui o lock são três fatos: o rename já é atômico, o
// temporário tem nome único por escritor (caminhoTemporario, desde a Tarefa 14),
// e a selagem e a reconstrução produzem arquivos BYTE A BYTE IDÊNTICOS, o que o
// golden prova. Com a igualdade provada, uma corrida no nome final é neutra em
// conteúdo: quem renomear por último grava os mesmos bytes.
//
// O QUE O CHAMADOR PRECISA GARANTIR, e é concreto: esta função lê pelo s.segR, e
// a aposentadoria fecha o descritor por segR.FechaSegmento, que exige que
// nenhuma leitura esteja em voo — barreira que no Store é o aposMu. O Close fecha
// o segR inteiro. Quem chama do laço é a passadaDeReconstrucao, e ela resolve os
// dois: contra o Close, o ParaReconstrucao cancela e AGUARDA antes do quiesce;
// contra a aposentadoria de UM segmento, ela classifica o os.ErrClosed/ENOENT
// como corrida benigna e segue para o próximo (oSegmentoSaiuDoDataset) em vez de
// tomar o aposMu por uma varredura inteira, o que travaria toda consulta atrás do
// escritor pendente. Varredura interrompida não publica índice parcial: o erro
// sobe antes do Escreve.
func (s *Store) ConstroiIndiceDoSegmento(ctx context.Context, seg uint32, campos []types.FieldID) error {
	_, err := s.constroiIndiceDoSegmento(ctx, seg, campos)
	return err
}

// constroiIndiceDoSegmento é o corpo, e devolve TAMBÉM se um arquivo de índice
// foi de fato publicado.
//
// O booleano existe por causa da série nishidb_index_reconstrucoes_total, que
// promete contar "segmentos cujo índice a reconstrução PUBLICOU". Sem ele o
// contador subia também para o segmento que a varredura visitou e do qual não
// saiu arquivo nenhum — o segmento sem campo indexado em record algum, que a
// §2.4 mantém na fila de propósito. Num dataset estável esse segmento faz a
// série crescer para sempre, e uma taxa que nunca zera é indistinguível de um
// moinho real: exatamente o defeito que a série existe para denunciar.
//
// A distinção não sai do erro nem do `feitos` da passada: os dois já significam
// outra coisa. `feitos` conta segmentos VARRIDOS, que é o custo da passada e o
// que os testes da §2.4 afirmam; publicar é o resultado. Um segmento visitado que
// não publica é trabalho legítimo com produto vazio, não falha.
func (s *Store) constroiIndiceDoSegmento(ctx context.Context, seg uint32, campos []types.FieldID) (bool, error) {
	if len(campos) == 0 {
		return false, nil
	}
	// A recusa que sobrou é a que continua verdadeira: campo FORA do conjunto
	// declarado. Gravar a seção dele produziria um índice que a ingestão não
	// alimenta — o próximo record daquele segmento não teria posting, e o
	// arquivo passaria a afirmar mais do que sabe.
	//
	// A recusa de SUBCONJUNTO saiu, e a ordem em que isso pôde acontecer é o
	// desenho, não o histórico: ela existia porque a pendência era decidida por
	// "tem .sidx?", e um .sidx imutável e parcial faria o segmento contar como
	// PRONTO com campos que ninguém indexou — falso negativo petrificado, sem
	// erro e sem log. Só depois de o teste de pendência passar a olhar SEÇÃO POR
	// CAMPO (SegmentosPendentesDeReconstrucao) um arquivo incompleto deixou de
	// poder se declarar completo, e só então pedir "reconstrua o campo que
	// falta" virou pergunta legítima.
	for _, campo := range campos {
		if !slices.Contains(s.camposIndexados, campo) {
			return false, fmt.Errorf("%w: reconstrução do segmento %d pedida para o campo %d, que o dataset não indexa (indexados: %v); "+
				"a seção existiria sem ninguém para alimentá-la",
				ErrInvalidArgument, seg, campo, s.camposIndexados)
		}
	}
	// O segmento CORRENTE é recusado, e a guarda não é defensiva: o .sidx é
	// IMUTÁVEL, e gravá-lo para um segmento que ainda recebe records
	// petrificaria um índice sem tudo que vier depois. Falso negativo
	// permanente, que é exatamente o desfecho que esta tarefa existe para
	// eliminar. O índice do corrente é o WIP, e ele se reconstrói na abertura.
	if corrente := s.curSegID.Load(); seg == corrente {
		return false, fmt.Errorf("%w: o segmento %d é o corrente; o índice dele é o WIP, não um .sidx imutável: %w",
			ErrInvalidArgument, seg, errSegmentoEhOCorrente)
	}
	// O tamanho é medido ANTES da varredura, por stat DIRETO no arquivo — e as
	// duas escolhas são a diferença entre pegar corrupção e matar o laço numa
	// corrida benigna. A primeira forma desta guarda media DEPOIS, pela
	// listagem, e morria de dois jeitos que a re-revisão encontrou: a retenção
	// podia aposentar o segmento entre a varredura completa e a listagem — e a
	// ausência era devolvida como ErrDatasetCorrupt, que nenhum classificador
	// benigno reconhece —, e o segmento PRÉ-SWAP (listado como pendente antes
	// de o curSegID trocar) podia crescer entre a varredura e a medida. Com a
	// medida anterior os dois viram desfechos benignos: o aposentado falha o
	// stat com ENOENT, que é a corrida que o laço já tolera; e o que cresceu dá
	// fimValido >= tamanho, que não é corrupção — o .sidx que ele viesse a
	// publicar é o resíduo que a §2.4 aceita, reescrito pela selagem da própria
	// rotação.
	info, err := os.Stat(segment.Path(s.dirDeDados(), seg))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, fmt.Errorf("o segmento %d saiu do dataset antes da reconstrução: %w", seg, os.ErrNotExist)
		}
		return false, err
	}
	tamanho := info.Size()
	caminho := segindex.CaminhoSelado(s.dirDeDados(), seg)
	cobertura, err := s.coberturaDaReconstrucao(caminho, campos)
	if err != nil {
		return false, err
	}
	e := segindex.NovoEscritor()
	fimValido, err := s.varreOsRecordsDoSegmento(ctx, seg, cobertura,
		func(campo types.FieldID, h uint64, row types.RowID) error {
			e.Adiciona(campo, h, row)
			return nil
		})
	if err != nil {
		return false, err
	}
	// A varredura que PAROU antes do fim medido é corrupção no meio de um
	// segmento selado — o ScanFrom para no primeiro record inválido sem erro
	// (§52), e num selado não existe tail legítimo. Publicar o .sidx desta
	// varredura seria o pior desfecho do produto: as seções sairiam completas,
	// a fila o contaria como pronto, o crc do arquivo — escrito corretamente a
	// partir de uma varredura incompleta — passaria, e as rows posteriores à
	// corrupção nunca virariam candidatas enquanto o locations continua
	// servindo-as. Falso negativo permanente. `<` e não `!=`: fimValido acima
	// do tamanho medido é o segmento pré-swap que cresceu durante a varredura,
	// e isso é a corrida benigna acima, não corrupção.
	if fimValido < tamanho {
		return false, fmt.Errorf(
			"%w: a varredura de reconstrução do segmento %d parou em %d de %d bytes; "+
				"um .sidx desta varredura declararia completo um índice sem as rows posteriores à corrupção",
			ErrDatasetCorrupt, seg, fimValido, tamanho)
	}
	if e.Vazio() {
		// Nenhuma posting sob a cobertura inteira: o índice CORRETO deste
		// segmento é arquivo AUSENTE, e é isso que o Escritor vazio já diz ao
		// não criar nada. O que ele não faz — porque não conhece o que estava lá
		// antes — é tirar do caminho um .sidx anterior, e aqui ele só pode ser
		// uma de duas coisas: seções de campos que deixaram de ser indexados, ou
		// um arquivo que não descreve este segmento. Nos dois casos remover é o
		// certo; deixá-lo seria manter em disco um arquivo que ninguém consulta
		// e que o IndexBytes conta, que é o mesmo modo de falha que o .widx
		// órfão obrigou a fechar.
		return false, s.removeSeladoObsoleto(seg, caminho)
	}
	if err := e.Escreve(caminho, seg); err != nil {
		return false, err
	}
	// A evição vem DEPOIS do rename, e sem ela a reconstrução seria trabalho
	// invisível: o s.selados guarda o mapeamento do arquivo ANTERIOR, e o mmap
	// sobrevive ao rename (é o inode que ele segura, não o nome). Uma consulta
	// que já tivesse tocado este segmento continuaria lendo o índice velho —
	// sem a seção do campo novo, portanto ENUMERANDO — até a próxima abertura
	// do dataset, e ainda prenderia o inode substituído.
	//
	// Ela usa o mecanismo que já existe, e não um segundo: o Fecha do Selado
	// impede leitores novos e adia o munmap até a última Fonte em voo sair
	// (contagem de vidas). É a mesma barreira da aposentadoria de segmento, pelo
	// mesmo selMu — e é ela que torna a reescrita segura contra uma varredura em
	// voo. A varredura que sobrevive lê o arquivo ANTIGO até terminar, o que é
	// correto: índice é superconjunto de candidatos, e o executor revalida (§44).
	s.esqueceSelado(seg)
	return true, nil
}

// coberturaDaReconstrucao decide quais campos o arquivo novo vai cobrir: os
// pedidos MAIS os que o .sidx atual já cobre e continuam declarados.
//
// A união não é generosidade, é a consequência de o arquivo ser IMUTÁVEL.
// "Acrescentar uma seção" é reescrever o arquivo inteiro (§2.6), e um arquivo
// reescrito só com o campo pedido APAGARIA as seções que já estavam lá. O falso
// negativo petrificado voltaria pela porta oposta — e pior, oscilando: a passada
// seguinte veria o campo antigo pendente, reescreveria com ele e perderia o
// novo, para sempre.
//
// Preservar é RE-DERIVAR, e não copiar do arquivo velho, e essa escolha é o
// desenho inteiro em uma linha: os records de um segmento não corrente são
// imutáveis, então as postings derivadas deles são bit a bit as mesmas que estão
// no .sidx atual — e a varredura do segmento, que é o custo inteiro da operação,
// já vai acontecer de qualquer jeito para o campo que falta. Copiar do índice
// velho seria mais código para os mesmos bytes, com um segundo caminho de
// produção do .sidx ao lado do Escritor e uma dependência nova: confiar em dado
// derivado para produzir dado derivado, que é justamente o que a §2.5 recusa
// fazer com o .widx.
//
// Um .sidx ausente ou ilegível não impede nada: não há o que preservar, e a
// cobertura é o que o chamador pediu. A lista de ilegíveis é a MESMA do
// segmentoPendente, e tem de continuar sendo: um arquivo que a fila julga
// refazível e a cobertura julga fatal produziria um segmento eternamente
// pendente cuja reconstrução sempre falha. Erro fora dela sobe — EIO é falha de
// infra, e reescrever o índice por cima dela seria decidir sobre um arquivo que
// ninguém conseguiu ler (§101).
func (s *Store) coberturaDaReconstrucao(caminho string, campos []types.FieldID) ([]types.FieldID, error) {
	cobertura := slices.Clone(campos)
	existentes, err := segindex.CamposSelados(caminho)
	switch {
	case indiceIlegivel(err):
		existentes = nil
	case err != nil:
		return nil, err
	}
	for _, campo := range existentes {
		// O filtro pelo conjunto declarado é o que faz a §2.6 valer nos DOIS
		// sentidos: a seção de um campo que deixou de ser indexado não entra na
		// cobertura, e por isso a reescrita a apaga. É a única forma de remover
		// seção de um arquivo imutável, e é a mesma operação da selagem.
		if slices.Contains(s.camposIndexados, campo) && !slices.Contains(cobertura, campo) {
			cobertura = append(cobertura, campo)
		}
	}
	// Ordenada e sem repetição: é o que o varreOsRecordsDoSegmento espera e o
	// que torna o arquivo produzido independente da ORDEM em que o chamador
	// listou os campos pendentes — sem isso, dois chamadores com o mesmo
	// conjunto poderiam produzir arquivos diferentes e o golden da §6.3 passaria
	// a depender de quem chamou.
	slices.Sort(cobertura)
	return slices.Compact(cobertura), nil
}

// removeSeladoObsoleto tira do caminho um .sidx que a reconstrução acabou de
// declarar sem conteúdo. Ausência do arquivo não é anomalia: é o estado normal
// de um segmento sem posting nenhuma. A ordem unlink-antes-de-esquecer é do
// removeSidxDoDisco, e o godoc de lá explica a corrida que a inversa abria —
// a justificativa antiga desta função para a ordem inversa (o inode preso)
// estava errada: o munmap espera as vidas nos dois desenhos.
func (s *Store) removeSeladoObsoleto(seg uint32, caminho string) error {
	if _, err := s.removeSidxDoDisco(seg); err != nil {
		return fmt.Errorf("storage: remover o índice obsoleto %s: %w", caminho, err)
	}
	return nil
}

// SegmentosPendentesDeReconstrucao devolve, em ordem crescente, os segmentos
// cujo índice selado não descreve o conjunto indexado do dataset.
//
// A fila é DERIVADA do disco a cada chamada, e não uma lista persistida, pela
// mesma razão que o índice é derivado dos records: um metadado a menos é uma
// janela de crash a menos. Uma fila em arquivo teria de ser durabilizada em
// ordem contra a selagem e contra a aposentadoria, e uma fila em memória se
// perderia no restart — que é justamente quando ela é mais necessária.
//
// O TESTE É POR (SEGMENTO, CAMPO), E NÃO POR "TEM .sidx?" — é a metade do débito
// 22 que esta função entrega. Um segmento selado ANTES de um campo ser declarado
// indexado TEM o
// arquivo e não tem a seção daquele campo: pela pergunta antiga ele contava como
// pronto, e o campo novo enumerava indefinidamente, a um custo proporcional ao
// dataset inteiro e não ao resultado. A comparação é de CONJUNTO, nos dois
// sentidos, e a segunda direção é o outro lado da §2.6: seção de um campo que
// deixou de ser indexado também põe o segmento na fila, porque a reescrita é o
// único jeito de tirá-la de um arquivo imutável.
//
// O PREÇO DA PERGUNTA NOVA é um open + mmap + munmap por segmento, no lugar de
// um stat: só o header e a tabela de seções são tocados (segindex.CamposSelados),
// e nem o crc nem uma página de diretório entram na conta. São dezenas de
// microssegundos por arquivo contra a varredura de até 512 MiB que a resposta
// agenda — e é o mínimo que responde a pergunta certa, porque "existe o arquivo?"
// não distingue os dois estados que importam.
//
// Quem entra, então, é o segmento com .dat cujo .sidx está ausente, ilegível ou
// com um conjunto de seções diferente do declarado — exceto o corrente, cujo
// índice é o WIP que a abertura reconstrói antes do primeiro Put. Um .widx de
// segmento sem .dat nunca aparece aqui porque a enumeração parte dos DADOS —
// enfileirá-lo seria mandar a reconstrução varrer um arquivo que não existe,
// para sempre. A remoção dele é da abertura (§2.5, terceiro caso).
//
// Dataset sem campo indexado devolve fila vazia: não há o que reconstruir, e é
// por isso que tirar o ÚLTIMO `index: true` não apaga os .sidx que sobraram —
// sem campo declarado não há reconstrução que os reescreva. Eles não respondem
// nada (o Lookup recusa campo não indexado antes de chegar ao arquivo) e saem de
// disco quando a compaction ou a retenção aposenta o segmento, pelo
// removeIndiceDoSegmento. Uma varredura de limpeza na abertura seria a
// alternativa, e ela é pior: apagaria o índice inteiro de um dataset aberto por
// qualquer caminho que simplesmente não declare os campos.
//
// A FILA É UM SUPERCONJUNTO DO TRABALHO REAL, e isso é deliberado — é a mesma
// doutrina do índice que ela alimenta (§44): candidato a mais custa trabalho,
// candidato a menos é falso negativo, e aqui o falso negativo é um campo que
// enumera para sempre. Dois casos dizem "pendente" sem terem o que fazer, e é
// por isso que o laço (§2.4) NÃO trata a fila como trabalho que encolhe a cada
// passada — ele guarda o PONTO FIXO de cada segmento que reconstruiu
// (recPontoFixo) e só o refaz quando o arquivo em disco deixa de ser o que ele
// deixou lá:
//
//   - segmento cujos records não têm campo indexado nenhum: o Escritor vazio não
//     cria arquivo, de propósito, e "ausente" continua sendo pendente;
//   - segmento em que UM dos campos declarados não aparece em record algum: a
//     seção dele não existe e não tem como existir. Distinguir isso de "a seção
//     ainda não foi construída" exigiria o formato dizer quais campos foram
//     CONSIDERADOS, e não só quais produziram posting — e o formato do .sidx não
//     muda neste ciclo.
//
// DUAS ARESTAS que o laço de fundo (§2.4) precisa tratar, e que ficam escritas
// aqui porque nenhuma delas é visível de dentro desta função. As duas foram
// fechadas pela Tarefa 14, e o registro fica porque as duas voltam a se abrir com
// uma linha:
//
//   - A resposta é um instantâneo, e o segmento recém-rotacionado aparece nela
//     por uma janela. O aposAppend troca o curSegID ANTES de selar o anterior
//     (indices.go), então entre as duas coisas o anterior tem dado e ainda não
//     tem .sidx. A corrida no nome FINAL é neutra — a igualdade byte a byte é
//     provada pelo golden —, e a do temporário deixou de existir porque ele passou
//     a ter nome único por escritor (segindex.caminhoTemporario), e não por
//     exclusão mútua. Com nome fixo, os dois caminhos escreviam no mesmo arquivo
//     e o .sidx saía rasgado.
//   - Um leitor que abra o .sidx ANTES da reescrita e o publique no cache DEPOIS
//     da evição que a reescrita faz (ConstroiIndiceDoSegmento) ficaria com o
//     mapeamento velho até a próxima abertura do dataset. Quem fecha isso é a
//     geração do cache de selados (selGeracao): o mapeamento aberto antes de uma
//     evição é descartado em vez de publicado. Ela nasceu junto com o laço, que é
//     o chamador de fundo que tornou a janela alcançável.
//
// DOS QUATRO GATILHOS DA §2.4, ESTA FUNÇÃO COBRE TRÊS: o formato novo sobre
// dataset antigo (migração), o campo acrescentado ao schema (§2.6) e a JANELA
// ALARGADA — um segmento que volta para dentro da janela reaparece aqui, porque
// o .sidx dele foi expulso e a ausência é pendência. O crc32c divergente (§1.5)
// só é conhecido na primeira consulta da seção, e quem o detectar precisa
// apagar o arquivo para o segmento voltar a ser pendente por AUSÊNCIA, que é a
// pergunta barata desta função.
//
// # A JANELA É PARÂMETRO, E É ELA QUE IMPEDE O MOINHO
//
// Um segmento FORA da janela não tem .sidx — ele foi expulso —, e pela pergunta
// por (segmento, campo) isso o tornaria PERMANENTEMENTE pendente. O laço de
// fundo da §2.4 o reconstruiria, a passada de retenção o expulsaria, ele
// voltaria para a fila: um moinho que se alimenta sozinho, varrendo até 512 MiB
// por volta.
//
// Fora da janela não é "pendente": é DELIBERADAMENTE SEM ÍNDICE, e é por isso
// que a janela entra pela assinatura em vez de ficar guardada no Store. Quem
// montar o laço não tem como esquecê-la — o compilador cobra o que um
// comentário não cobraria.
//
// A conta de "dentro da janela" é a MESMA da expulsão (janelaDeSegmentos), e
// tem de ser: se as duas respondessem por contas diferentes, a que responde
// "sim" reconstruiria o que a outra acabou de apagar. É a doutrina de nunca ter
// a mesma coisa em dois lugares, aplicada a uma decisão.
//
// Janela INFINITA — o default, e o que todo dataset materializado tem por
// construção — não filtra nada, e o comportamento é exatamente o de antes de a
// janela existir.
//
// # ELA É TAMBÉM QUEM PUBLICA A SÉRIE nishidb_index_segmentos
//
// Os quatro estados (§6.6) saem daqui, e não de uma coleta própria, porque é
// aqui que os quatro são decididos pela MESMA conta que decide a fila. Uma
// segunda passada só para contar seria um terceiro lugar respondendo "este
// segmento deve ter índice?", ao lado da expulsão e da pendência — e o godoc
// inteiro desta função existe para explicar por que dois já seriam um moinho.
//
// A publicação é em ATÔMICO, e o scrape só os lê. A alternativa — uma coleta que
// chamasse esta função — pediria o retPassadaMu de fora da cadeia
// vida → wmu → aposMu, sobre a qual ele fica; a inversão só apareceria sob carga,
// que é exatamente quando o /metrics precisa responder.
//
// ELA TOMA O retPassadaMu, e não é para serializar duas pendências: é para
// serializar contra a EXPULSÃO. As duas leem o mesmo disco, e uma pendência que
// caia no MEIO de uma expulsão vê parte dos .sidx já removidos — estimador
// menor, mais segmentos admitidos — e agenda a reconstrução exatamente do que
// está sendo apagado. Isolado isso é desperdício; combinado com o estimador
// baseado no maior arquivo, é o moinho por outro gatilho. É também o que protege
// o tamanhoExpulso, que a expulsão escreve e esta função lê.
func (s *Store) SegmentosPendentesDeReconstrucao(j JanelaDeIndice) ([]uint32, error) {
	if err := s.exigeJanelaAplicavel(j); err != nil {
		return nil, err
	}
	s.retPassadaMu.Lock()
	defer s.retPassadaMu.Unlock()
	if len(s.camposIndexados) == 0 {
		return nil, nil
	}
	dir := s.dirDeDados()
	refs, err := segment.ListSegments(dir)
	if err != nil {
		return nil, err
	}
	// dentro nil significa "não filtre": é a janela infinita, e distinguir isso
	// de um conjunto vazio é o que impede a janela infinita de esvaziar a fila.
	var dentro map[uint32]struct{}
	if !j.Infinita() {
		estado, err := s.janelaDeSegmentos(j, refs)
		if err != nil {
			return nil, err
		}
		dentro = estado.dentro
	}
	corrente := s.curSegID.Load()
	var (
		pendentes []uint32
		estados   EstadosDeSegmento
	)
	for _, ref := range refs {
		if ref.ID == corrente {
			estados.Corrente++
			continue
		}
		if dentro != nil {
			if _, ok := dentro[ref.ID]; !ok {
				estados.Fora++
				continue
			}
		}
		pendente, err := s.segmentoPendente(dir, ref.ID)
		if err != nil {
			return nil, err
		}
		if pendente {
			pendentes = append(pendentes, ref.ID)
			estados.Pendente++
			continue
		}
		estados.Selado++
	}
	// A publicação vem depois do laço COMPLETO, e por isso não acontece nos
	// retornos de erro acima: uma passada que morreu no meio descreve um dataset
	// pela metade, e publicá-la faria a fila parecer ter encolhido quando o que
	// encolheu foi a varredura. O retrato anterior fica, que é o que um medidor
	// deve fazer quando a medição falhou.
	s.publicaEstadosDeSegmento(estados)
	return pendentes, nil // ListSegments já devolve ordenado por ID
}

// segmentoPendente responde se o .sidx do segmento descreve exatamente o
// conjunto indexado do dataset.
//
// Os dois conjuntos são comparados com slices.Equal porque os DOIS são
// crescentes por contrato: a tabela de seções é validada como ordenada na
// abertura (segindex.leTabelaDeSecoes) e o s.camposIndexados sai ordenado do
// resolveCamposIndexados. Ordenar aqui de novo seria esconder uma quebra dessas
// duas garantias em vez de deixá-la aparecer.
//
// Arquivo ILEGÍVEL conta como pendente, e não como erro: um .sidx que este
// binário não decodifica é dado derivado que não serve, e a resposta para dado
// derivado que não serve é derivá-lo de novo (indiceIlegivel). Erro fora dessa
// lista sobe intacto — EIO ao abrir o índice é falha de infra, e chamá-la de
// "reconstrua" mandaria o laço varrer 512 MiB por um disco doente (§101, três
// baldes).
func (s *Store) segmentoPendente(dir string, seg uint32) (bool, error) {
	campos, err := segindex.CamposSelados(segindex.CaminhoSelado(dir, seg))
	switch {
	case indiceIlegivel(err):
		return true, nil
	case err != nil:
		return false, err
	}
	return !slices.Equal(campos, s.camposIndexados), nil
}

// indiceIlegivel separa "este .sidx não serve, refaça" de "o disco falhou,
// pare". É a fronteira do §101 aplicada ao índice, e ela é UMA função porque os
// dois chamadores — o teste de pendência e a cobertura da reconstrução —
// precisam concordar: um arquivo que a fila julgasse refazível e a cobertura
// julgasse fatal daria um segmento eternamente pendente cuja reconstrução sempre
// falha.
//
// São três estados, e nenhum deles é anomalia de infra:
//
//   - AUSENTE: o segmento nunca teve índice (fora da janela, ou gravado por um
//     binário anterior);
//   - FORMATO inválido: bytes que este binário não decodifica;
//   - VAZIO: zero bytes, que o mmap recusa ANTES do parser — por isso ele não
//     chega como ErrFormato e precisa do sentinela próprio. Um .sidx de zero
//     bytes só existe como escrita truncada (o Escritor se recusa a emitir
//     índice vazio), e o único caminho conhecido para produzi-lo — a corrida de
//     dois escritores no `<sidx>.tmp` de nome FIXO — foi fechado pelo nome único
//     por escritor da §2.4. A tolerância fica: ela vale para o arquivo que um
//     binário anterior deixou em disco, e é o próprio godoc do
//     mmap.ErrArquivoVazio que manda tratá-lo como "sem índice".
func indiceIlegivel(err error) bool {
	return errors.Is(err, os.ErrNotExist) ||
		errors.Is(err, segindex.ErrFormato) ||
		errors.Is(err, mmap.ErrArquivoVazio)
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
	// O fim válido é descartado DE PROPÓSITO, e só aqui: no segmento corrente a
	// parada antes do fim é o estado normal — o OpenWriter já truncou o lixo
	// além do fim válido, e o WIP é mutável e continua sendo alimentado. A
	// guarda de varredura incompleta é do caminho SELADO, onde o arquivo
	// publicado seria imutável.
	_, err := s.varreOsRecordsDoSegmento(context.Background(), s.curSegID.Load(), s.camposIndexados,
		func(campo types.FieldID, h uint64, row types.RowID) error {
			// O mapa não pode errar: ele foi construído a partir DESTE mesmo
			// s.camposIndexados, no abreWIPsDoCorrente, algumas linhas acima.
			return s.wips[campo].Set(h, row)
		})
	return err
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
// inválido SEM erro (§52), e é por isso que o fim válido volta ao chamador em
// vez de ser descartado: no segmento CORRENTE a parada é o estado normal (o
// OpenWriter já truncou o lixo além do fim válido), mas num segmento SELADO
// ela é corrupção no MEIO do arquivo — e a recovery não a pegaria, porque ela
// só varre [CurrentSegment..maxID] e dos anteriores confere apenas o tamanho.
// Quem decide o que fazer com o fim válido é o chamador, porque só ele sabe em
// qual dos dois casos está.
//
// A pertinência do campo sai de `campos`, que é sempre um SUBCONJUNTO do que a
// ingestão indexa (s.camposIndexados): o WIP do corrente usa o conjunto inteiro e
// a reconstrução usa a cobertura decidida pelo coberturaDaReconstrucao. É essa
// contenção que faz a alimentação incremental e a reconstruída coincidirem campo
// a campo — um campo FORA do conjunto indexado ganharia seção que a ingestão não
// alimenta, e é por isso que ConstroiIndiceDoSegmento o recusa antes de chegar
// aqui. slices.Contains sobre um conjunto ordenado de punhado de campos é mais
// barato que o mapa que o substituiria.
//
// A chave canônica é montada com valuekey.Append sobre um buffer LOCAL, e não
// sobre o s.chaveIndiceBuf do caminho de escrita: aquele é reutilizado sob o wmu,
// e esta varredura roda fora dele.
func (s *Store) varreOsRecordsDoSegmento(
	ctx context.Context,
	seg uint32,
	campos []types.FieldID,
	emite func(campo types.FieldID, h uint64, row types.RowID) error,
) (int64, error) {
	// O contador sobe na ENTRADA, e conta a varredura pedida — não a concluída.
	// É o que ele precisa medir: a leitura de até 512 MiB é paga assim que
	// começa, e uma varredura cancelada no meio já custou o prefixo. Zerá-la aqui
	// deixaria o observável parado exatamente na passada sobre a qual alguém está
	// perguntando, que é o mesmo argumento dos contadores da compaction.
	s.varreduras.Add(1)
	var (
		chave  []byte
		leitor encoding.FieldReader
	)
	fimValido, err := s.segR.ScanFrom(seg, int64(segment.HeaderSize), func(_ uint32, rec types.Record) error {
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
	return fimValido, err
}
