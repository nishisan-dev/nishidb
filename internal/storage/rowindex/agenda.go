package rowindex

import (
	"fmt"
	"math"
	"path/filepath"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// Agenda é o índice de QUANDO cada linha vence, e existe para que o coletor
// do idle timeout pergunte em vez de procurar.
//
// A varredura que ela substitui era a segunda maior fatia de CPU da ingestão
// medida em produção — 19%, 171 µs por evento —, e o custo não vinha das
// linhas vencidas: vinha das que não venceram. Com uma regra condicional de
// minutos, o coletor dava a volta completa nos milhões de linhas do dataset a
// cada poucos minutos para encontrar as poucas décimas de por cento que
// tinham vencido.
//
// # A estrutura
//
// Min-tree B-ária sobre um array denso indexado por rowId. O nível 0 são as
// FOLHAS — um u64 por linha, o instante de vencimento em epoch millis — e
// cada nível acima é um SUMÁRIO: uma entrada por bloco de agendaB filhos.
//
// O que decide esta família contra as alternativas é o requisito que aparece
// em TODO evento: atualizar o prazo de uma entidade. Só ela dá atualização
// O(1) esperada, porque separa o VALOR (denso, endereçado por rowId, escrito
// no lugar) da ORDENAÇÃO (o sumário hierárquico, que quase nunca precisa ser
// recomputado). Um heap persistente pagaria ~log(n) escritas aleatórias por
// evento e um crash no meio do sift quebraria a invariante sem sintoma;
// buckets por janela de tempo morrem no espaço, porque com renovação a cada
// evento cada escrita insere uma entrada que vive o prazo inteiro.
//
// # A invariante, que é a corretude inteira
//
//	nivel[i][b] <= min{ vencimento efetivo dos filhos de b }
//
// em TODO instante, para um leitor sem lock. O nó é limite INFERIOR do
// mínimo, nunca superior, e a assimetria é deliberada:
//
//   - nó ABAIXO do mínimo real (frouxo) custa uma descida desperdiçada, e o
//     Store revalida cada linha sob o lock do writer antes de marcar
//     qualquer coisa. É erro barato.
//   - nó ACIMA do mínimo real faz a descida PULAR o ramo, e a linha vencida
//     nunca mais é recuperada. É perda silenciosa de dado vivo.
//
// É essa assimetria que fixa a ordem de escrita do Atualiza, e é ela também
// que torna a estrutura segura contra crash sem journal nenhum: toda
// interrupção no meio de uma atualização deixa nós MENORES do que o exato —
// o lado barato.
//
// # Concorrência
//
// Escrita é exclusiva do writer, como todo denseFile. Leitura (Get,
// ProximoVencimento, Vencidos) é segura para um leitor concorrente sem lock:
// cada entrada é uma word alinhada lida com AtomicLoad64, e a invariante vale
// entre escritas individuais, não só ao fim de uma atualização completa.
type Agenda struct {
	// niveis[0] são as folhas; niveis[len-1] é o topo, varrido por inteiro.
	niveis []*denseFile

	// aposEscrita é chamado depois de CADA entrada gravada, e é o que dá aos
	// testes acesso aos estados intermediários de uma atualização. A ordem de
	// escrita é a invariante mais importante desta estrutura e a violação que
	// ela evita é transitória: existe só entre duas escritas de um mesmo
	// Atualiza, e some antes que ele retorne. Uma conferência que só rodasse
	// entre operações completas aprovaria a ordem invertida. Seam de teste;
	// nil em produção.
	aposEscrita func()

	// aposRecompute é chamado a cada mínimo recomputado, e existe porque o
	// custo desta estrutura NÃO está nas escritas: está nas leituras. Remover
	// a parada da subida deixa o número de escritas idêntico — o corte por
	// `novo == atual` ainda evita gravar — e multiplica por agendaNiveis o
	// número de blocos varridos. Um gate que só contasse escritas aprovaria
	// essa mutação. Seam de teste; nil em produção.
	aposRecompute func()
}

// AgendaBlockShift e AgendaNiveis são a GEOMETRIA da árvore, exportadas
// porque é o manifest que as grava: uma agenda lida com fanout ou
// profundidade diferentes dos que a construíram endereça o bloco errado em
// cada nível, e é para detectar essa divergência que os dois viajam no
// manifest em vez de serem inferidos dos arquivos.
const (
	AgendaBlockShift = agendaShift
	AgendaNiveis     = agendaNiveis
)

const (
	// agendaShift dá B = 64 filhos por nó. O tamanho é o de uma linha de
	// cache de 512 bytes vezes oito: um recompute lê 64 words contíguas, que
	// é trabalho sequencial de mmap já aquecido, e a árvore fica rasa o
	// bastante para o topo caber numa varredura desprezível.
	agendaShift = 6
	agendaB     = 1 << agendaShift

	// agendaNiveis é a folha mais três sumários. O topo cobre 64³ = 262.144
	// linhas por entrada: um dataset de 8M — o alvo declarado do produto —
	// tem 31 entradas no topo, e varrê-lo inteiro para achar o próximo
	// vencimento é mais barato que descer uma árvore com raiz única.
	agendaNiveis = 4
)

// Nunca é o vencimento da linha que NÃO expira, e é também o valor natural de
// toda entrada que o arquivo ainda não tem.
//
// Zero significar ∞ é o que faz crescer a agenda não recomputar nada: o bloco
// novo nasce zerado pelo fallocate, e zero já é a resposta certa para linhas
// que ainda não existem. A alternativa — ∞ como MaxUint64 — obrigaria a
// preencher cada bloco novo antes de publicá-lo, e um crash no meio do
// preenchimento deixaria zeros lidos como "vencido em 1970".
const Nunca uint64 = 0

// Carencia é o vencimento da linha que tem location publicada mas nenhum
// instante de escrita conhecido: a entidade materializada antes de a expiração
// ser ligada.
//
// Ela precisa da atenção do coletor AGORA — para receber o prazo cheio de
// carência —, e não em algum instante futuro. Um valor derivado do relógio
// não serviria: a agenda é reconstruída a partir de expiry.idx e prazos.idx, e
// um sentinela que dependesse de QUANDO a reconstrução rodou daria árvores
// diferentes para o mesmo dataset. Epoch millis 1 é anterior a qualquer
// vencimento real e não colide com nada.
const Carencia uint64 = 1

// PrimeiroVencimentoReal é o menor valor que uma linha com prazo de verdade
// pode ocupar. Vencimentos calculados abaixo dele são elevados até aqui, para
// que nenhum se confunda com os dois sentinelas acima.
const PrimeiroVencimentoReal uint64 = 2

var (
	folhasMagic  = [4]byte{'N', 'D', 'B', 'V'}
	sumarioMagic = [4]byte{'N', 'D', 'B', 'S'}
)

// ordem mapeia o valor gravado para a chave de comparação, traduzindo o
// sentinela Nunca para o infinito que ele representa. Toda comparação da
// agenda passa por aqui: comparar os valores crus faria a linha imortal
// parecer a mais urgente de todas.
func ordem(v uint64) uint64 {
	if v == Nunca {
		return math.MaxUint64
	}
	return v
}

// OpenAgenda cria/reabre os arquivos de todos os níveis em dir.
//
// O nome carrega o nível (`vencimentos.idx`, `vencimentos.s1.idx`, ...) e a
// magic separa folha de sumário: trocar dois arquivos de lugar produziria uma
// árvore que responde sem erro e agenda tudo errado.
func OpenAgenda(dir string, minRows uint64) (*Agenda, error) {
	a := &Agenda{niveis: make([]*denseFile, 0, agendaNiveis)}
	for nivel := 0; nivel < agendaNiveis; nivel++ {
		nome, magic := "vencimentos.idx", folhasMagic
		if nivel > 0 {
			nome, magic = fmt.Sprintf("vencimentos.s%d.idx", nivel), sumarioMagic
		}
		d, err := openDense(filepath.Join(dir, nome), magic, minRows>>(agendaShift*nivel))
		if err != nil {
			// Os níveis já abertos são fechados antes de desistir: um Open
			// que falha no terceiro arquivo não pode deixar dois mapeamentos
			// vivos sem dono. O erro do fechamento é descartado de propósito
			// — quem chamou precisa da causa da FALHA, não do resíduo dela.
			_ = a.fecha()
			return nil, err
		}
		a.niveis = append(a.niveis, d)
	}
	return a, nil
}

// valor lê a entrada `idx` do nível, com o ∞ implícito de tudo que está além
// do arquivo.
func (a *Agenda) valor(nivel int, idx uint64) uint64 {
	v, ok := a.niveis[nivel].load(types.RowID(idx))
	if !ok {
		return Nunca
	}
	return v
}

// Get devolve o vencimento agendado para a linha, ou Nunca.
func (a *Agenda) Get(rowID types.RowID) uint64 { return a.valor(0, uint64(rowID)) }

// Atualiza grava o vencimento da linha mantendo a invariante em todo instante.
// Exclusivo do writer.
//
// A ordem das escritas é assimétrica, e as duas metades existem por razões
// diferentes:
//
// ABAIXANDO (o vencimento chegou para mais perto, ou a linha entrou na
// agenda): baixar a folha antes dos ancestrais deixaria, entre uma escrita e
// outra, um ancestral ACIMA do mínimo dos filhos — e um leitor que descesse
// nessa janela pularia o ramo e perderia a linha. Por isso os ancestrais vêm
// primeiro, do topo para baixo, e a folha por último: em todo instante
// intermediário o ancestral já é <= o valor novo.
//
// SUBINDO (renovação, que é o caso comum em regime): aumentar a folha NÃO
// viola nada — o ancestral continua sendo limite inferior de um mínimo que só
// cresceu. Escreve-se a folha primeiro e sobe-se recomputando, e a subida
// para no primeiro nó cujo mínimo vinha de OUTRO filho, porque daí para cima
// nada mudou.
//
// É essa parada que decide o custo em regime. Medido por
// TestRenovacaoNaoRecomputaAArvoreInteira: 1,0165 escritas e 0,0165
// recomputes por renovação — ou seja, quase toda renovação é UMA escrita de
// folha e nenhuma leitura de bloco. Sem a parada os recomputes vão a 1,02 por
// evento (um por nível de sumário), e o número de ESCRITAS não muda: é por
// isso que o gate conta as duas coisas.
func (a *Agenda) Atualiza(rowID types.RowID, venc uint64) error {
	antigo := a.valor(0, uint64(rowID))
	if antigo == venc {
		return nil
	}
	if ordem(venc) < ordem(antigo) {
		for nivel := len(a.niveis) - 1; nivel >= 1; nivel-- {
			idx := uint64(rowID) >> (agendaShift * nivel)
			if ordem(a.valor(nivel, idx)) > ordem(venc) {
				if err := a.grava(nivel, idx, venc); err != nil {
					return err
				}
			}
		}
		return a.grava(0, uint64(rowID), venc)
	}

	if err := a.grava(0, uint64(rowID), venc); err != nil {
		return err
	}
	// anterior é o valor que o nó do nível de BAIXO tinha antes desta
	// atualização. No primeiro giro é a própria folha; depois é o valor que o
	// nó do nível anterior tinha, e é contra ele que se decide se o pai ainda
	// dependia deste ramo.
	anterior := antigo
	for nivel := 1; nivel < len(a.niveis); nivel++ {
		idx := uint64(rowID) >> (agendaShift * nivel)
		atual := a.valor(nivel, idx)
		if ordem(atual) < ordem(anterior) {
			// O mínimo deste nó vinha de outro filho: nada acima muda.
			return nil
		}
		if a.aposRecompute != nil {
			a.aposRecompute()
		}
		novo := a.minimoDosFilhos(nivel, idx)
		if novo == atual {
			return nil
		}
		if err := a.grava(nivel, idx, novo); err != nil {
			return err
		}
		anterior = atual
	}
	return nil
}

// grava escreve uma entrada e aciona o seam. Todo caminho de escrita da
// agenda passa por aqui: uma escrita direta no denseFile ficaria invisível
// para o teste que confere a ordem.
func (a *Agenda) grava(nivel int, idx, v uint64) error {
	if err := a.niveis[nivel].store(types.RowID(idx), v); err != nil {
		return err
	}
	if a.aposEscrita != nil {
		a.aposEscrita()
	}
	return nil
}

// minimoDosFilhos recomputa o mínimo exato de um nó, lendo os agendaB filhos.
func (a *Agenda) minimoDosFilhos(nivel int, idx uint64) uint64 {
	base := idx << agendaShift
	melhor := Nunca
	for i := uint64(0); i < agendaB; i++ {
		v := a.valor(nivel-1, base+i)
		if v == Nunca {
			continue
		}
		if melhor == Nunca || v < melhor {
			melhor = v
		}
	}
	return melhor
}

// entradas devolve quantas entradas cabem no arquivo do nível.
func (a *Agenda) entradas(nivel int) uint64 {
	n := a.niveis[nivel].region.Size() - headerSize
	if n <= 0 {
		return 0
	}
	return uint64(n / entrySize)
}

// ProximoVencimento devolve o instante do vencimento mais próximo, e ok=false
// quando nenhuma linha está agendada.
//
// É um limite INFERIOR do vencimento real, pela invariante: dormir até aqui
// pode acordar o coletor cedo — que custa uma descida sem achado —, nunca
// tarde.
func (a *Agenda) ProximoVencimento() (uint64, bool) {
	topo := len(a.niveis) - 1
	melhor := Nunca
	for idx, n := uint64(0), a.entradas(topo); idx < n; idx++ {
		v := a.valor(topo, idx)
		if v == Nunca {
			continue
		}
		if melhor == Nunca || v < melhor {
			melhor = v
		}
	}
	return melhor, melhor != Nunca
}

// Vencidos visita, em ordem de rowId, cada linha cujo vencimento agendado é
// menor ou igual a `ate`. A visita que devolve false interrompe a descida.
//
// Devolve quantas ENTRADAS a descida tocou — os nós de sumário em que ela
// entrou mais as folhas entregues. É a medida do esforço, e ela existe para
// poder ser afirmada: uma implementação que varresse o dataset entregaria as
// mesmas linhas e não tocaria entrada nenhuma da árvore, então contar só o que
// foi entregue não distinguiria as duas.
//
// A entrega é COMPLETA pela invariante: se uma linha vence, todo ancestral
// dela tem valor <= o vencimento dela <= `ate`, então nenhum ramo que a
// contenha é pulado. Pode entregar linhas a mais — um nó frouxo faz descer
// num ramo sem nada vencido —, e é o chamador que revalida.
func (a *Agenda) Vencidos(ate uint64, visita func(types.RowID) bool) int {
	topo := len(a.niveis) - 1
	tocadas := 0
	for idx, n := uint64(0), a.entradas(topo); idx < n; idx++ {
		v := a.valor(topo, idx)
		if v == Nunca || v > ate {
			continue
		}
		tocadas++
		if !a.desce(topo, idx, ate, visita, &tocadas) {
			return tocadas
		}
	}
	return tocadas
}

// desce percorre os filhos de um nó, e devolve false quando a visita pediu
// para parar. `tocadas` acumula as entradas em que a descida ENTROU.
func (a *Agenda) desce(nivel int, idx, ate uint64, visita func(types.RowID) bool, tocadas *int) bool {
	base := idx << agendaShift
	for i := uint64(0); i < agendaB; i++ {
		filho := base + i
		v := a.valor(nivel-1, filho)
		if v == Nunca || v > ate {
			continue
		}
		*tocadas++
		if nivel-1 == 0 {
			if !visita(types.RowID(filho)) {
				return false
			}
			continue
		}
		if !a.desce(nivel-1, filho, ate, visita, tocadas) {
			return false
		}
	}
	return true
}

// PrecisaConstruir reporta se a árvore em disco não serve para o dataset que
// tem `limite` linhas, e tem de ser reconstruída antes de qualquer uso.
//
// Existe porque o manifest declara os PARÂMETROS da agenda, não a existência
// dela. Um dataset cujos arquivos de vencimento sumiram — apagados por engano,
// perdidos numa cópia parcial, truncados por um filesystem cheio — abriria com
// a seção batendo e uma árvore VAZIA, e árvore vazia significa "nada vence
// nunca": o mesmo modo de falha silencioso que a seção crítica existe para
// impedir, entrando pela porta que ela não cobre.
//
// Duas perguntas, porque são dois estragos diferentes e nenhuma vê o outro:
//
//   - NASCEU nesta abertura. É o sinal decisivo, e o único que enxerga o
//     arquivo apagado: openDense recria o que não existe já no tamanho de
//     piso, então um dataset pequeno passaria folgado por qualquer checagem
//     de tamanho.
//   - não COBRE as linhas. Pega o arquivo que sobreviveu com o header intacto
//     e perdeu a cauda, que nenhuma checagem de existência veria.
//
// As duas custam um campo e um Size() por nível — nada é varrido.
func (a *Agenda) PrecisaConstruir(limite types.RowID) bool {
	for nivel := range a.niveis {
		if a.niveis[nivel].nasceu {
			return true
		}
	}
	if limite == 0 {
		return false
	}
	for nivel := range a.niveis {
		if a.entradas(nivel) <= uint64(limite-1)>>(agendaShift*nivel) {
			return true
		}
	}
	return false
}

// Reconstroi reescreve a árvore inteira a partir de venc, que devolve o
// vencimento de cada linha. Exclusivo do writer, e sem leitores — é o caminho
// do Open.
//
// Preenche as folhas linearmente e só então computa os sumários de baixo para
// cima, em vez de chamar Atualiza linha a linha. As duas formas dão a mesma
// árvore; esta paga uma escrita por entrada em vez de uma por entrada mais os
// abaixamentos em cascata que a ordem de chegada provocaria, e lê cada nível
// uma vez só, sequencialmente. É o que mantém a construção de milhões de
// linhas na casa dos milissegundos, e é o que torna "construir na abertura"
// preferível a "exigir recriação do dataset".
//
// Todo o arquivo de cada nível é reescrito, e não só a parte derivada de
// limite: quando a reconstrução acontece por divergência de parâmetros, o que
// está em disco é uma árvore VÁLIDA de outro mundo, e deixar qualquer entrada
// dela para trás misturaria as duas.
func (a *Agenda) Reconstroi(limite types.RowID, venc func(types.RowID) uint64) error {
	for id := types.RowID(0); id < limite; id++ {
		if err := a.niveis[0].store(id, venc(id)); err != nil {
			return err
		}
	}
	for nivel := 1; nivel < len(a.niveis); nivel++ {
		n := max(a.entradas(nivel), uint64(limite)>>(agendaShift*nivel)+1)
		for idx := uint64(0); idx < n; idx++ {
			if err := a.niveis[nivel].store(types.RowID(idx), a.minimoDosFilhos(nivel, idx)); err != nil {
				return err
			}
		}
	}
	return nil
}

// Confere verifica a invariante em toda a árvore, e é o que dá ao teste de
// crash uma afirmação a fazer.
//
// Reprova só o que é FATAL: nó acima do mínimo dos filhos, que faz a descida
// pular linhas vencidas. Nó abaixo passa, porque é o estado que toda escrita
// interrompida deixa e é recuperável por conta própria — o recompute da
// primeira renovação daquele bloco o aperta de volta.
func (a *Agenda) Confere() error {
	for nivel := 1; nivel < len(a.niveis); nivel++ {
		for idx, n := uint64(0), a.entradas(nivel); idx < n; idx++ {
			no := a.valor(nivel, idx)
			minimo := a.minimoDosFilhos(nivel, idx)
			if ordem(no) > ordem(minimo) {
				return fmt.Errorf("rowindex: agenda com nível %d bloco %d em %d, acima do mínimo %d dos filhos: a descida pularia esse ramo",
					nivel, idx, no, minimo)
			}
		}
	}
	return nil
}

// Msync força as páginas sujas de todos os níveis para disco.
func (a *Agenda) Msync() error {
	for nivel, d := range a.niveis {
		if err := d.region.Msync(); err != nil {
			return fmt.Errorf("rowindex: msync do nível %d da agenda: %w", nivel, err)
		}
	}
	return nil
}

// Close libera os mapeamentos.
func (a *Agenda) Close() error { return a.fecha() }

func (a *Agenda) fecha() error {
	var primeiro error
	for _, d := range a.niveis {
		if err := d.region.Close(); err != nil && primeiro == nil {
			primeiro = err
		}
	}
	return primeiro
}
