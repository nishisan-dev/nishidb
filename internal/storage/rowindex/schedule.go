package rowindex

import (
	"fmt"
	"math"
	"path/filepath"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// Schedule é o índice de QUANDO cada linha vence, e existe para que o coletor
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
// cada nível acima é um SUMÁRIO: uma entrada por bloco de scheduleB filhos.
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
// É essa assimetria que fixa a ordem de escrita do Update, e é ela também
// que torna a estrutura segura contra crash sem journal nenhum: toda
// interrupção no meio de uma atualização deixa nós MENORES do que o exato —
// o lado barato.
//
// # Concorrência
//
// Escrita é exclusiva do writer, como todo denseFile. Leitura (Get,
// NextExpiry, VisitExpired) é segura para um leitor concorrente sem lock:
// cada entrada é uma word alinhada lida com AtomicLoad64, e a invariante vale
// entre escritas individuais, não só ao fim de uma atualização completa.
type Schedule struct {
	// levels[0] são as folhas; levels[len-1] é o topo, varrido por inteiro.
	levels []*denseFile

	// afterWrite é chamado depois de CADA entrada gravada, e é o que dá aos
	// testes acesso aos estados intermediários de uma atualização. A ordem de
	// escrita é a invariante mais importante desta estrutura e a violação que
	// ela evita é transitória: existe só entre duas escritas de um mesmo
	// Update, e some antes que ele retorne. Uma conferência que só rodasse
	// entre operações completas aprovaria a ordem invertida. Seam de teste;
	// nil em produção.
	afterWrite func()

	// afterRecompute é chamado a cada mínimo recomputado, e existe porque o
	// custo desta estrutura NÃO está nas escritas: está nas leituras. Remover
	// a parada da subida deixa o número de escritas idêntico — o corte por
	// `recomputed == current` ainda evita gravar — e multiplica por
	// scheduleLevels o número de blocos varridos. Um gate que só contasse
	// escritas aprovaria essa mutação. Seam de teste; nil em produção.
	afterRecompute func()
}

// ScheduleBlockShift e ScheduleLevels são a GEOMETRIA da árvore, exportadas
// porque é o manifest que as grava: uma agenda lida com fanout ou
// profundidade diferentes dos que a construíram endereça o bloco errado em
// cada nível, e é para detectar essa divergência que os dois viajam no
// manifest em vez de serem inferidos dos arquivos.
const (
	ScheduleBlockShift = scheduleShift
	ScheduleLevels     = scheduleLevels
)

const (
	// scheduleShift dá B = 64 filhos por nó. O tamanho é o de uma linha de
	// cache de 512 bytes vezes oito: um recompute lê 64 words contíguas, que
	// é trabalho sequencial de mmap já aquecido, e a árvore fica rasa o
	// bastante para o topo caber numa varredura desprezível.
	scheduleShift = 6
	scheduleB     = 1 << scheduleShift

	// scheduleLevels é a folha mais três sumários. O topo cobre 64³ = 262.144
	// linhas por entrada: um dataset de 8M — o alvo declarado do produto —
	// tem 31 entradas no topo, e varrê-lo inteiro para achar o próximo
	// vencimento é mais barato que descer uma árvore com raiz única.
	scheduleLevels = 4
)

// Never é o vencimento da linha que NÃO expira, e é também o valor natural de
// toda entrada que o arquivo ainda não tem.
//
// Zero significar ∞ é o que faz crescer a agenda não recomputar nada: o bloco
// novo nasce zerado pelo fallocate, e zero já é a resposta certa para linhas
// que ainda não existem. A alternativa — ∞ como MaxUint64 — obrigaria a
// preencher cada bloco novo antes de publicá-lo, e um crash no meio do
// preenchimento deixaria zeros lidos como "vencido em 1970".
const Never uint64 = 0

// Grace é o vencimento da linha que tem location publicada mas nenhum
// instante de escrita conhecido: a entidade materializada antes de a expiração
// ser ligada.
//
// Ela precisa da atenção do coletor AGORA — para receber o prazo cheio de
// carência —, e não em algum instante futuro. Um valor derivado do relógio
// não serviria: a agenda é reconstruída a partir de expiry.idx e prazos.idx, e
// um sentinela que dependesse de QUANDO a reconstrução rodou daria árvores
// diferentes para o mesmo dataset. Epoch millis 1 é anterior a qualquer
// vencimento real e não colide com nada.
const Grace uint64 = 1

// FirstRealExpiry é o menor valor que uma linha com prazo de verdade
// pode ocupar. Vencimentos calculados abaixo dele são elevados até aqui, para
// que nenhum se confunda com os dois sentinelas acima.
const FirstRealExpiry uint64 = 2

var (
	leavesMagic  = [4]byte{'N', 'D', 'B', 'V'}
	summaryMagic = [4]byte{'N', 'D', 'B', 'S'}
)

// orderKey mapeia o valor gravado para a chave de comparação, traduzindo o
// sentinela Never para o infinito que ele representa. Toda comparação da
// agenda passa por aqui: comparar os valores crus faria a linha imortal
// parecer a mais urgente de todas.
func orderKey(v uint64) uint64 {
	if v == Never {
		return math.MaxUint64
	}
	return v
}

// OpenSchedule cria/reabre os arquivos de todos os níveis em dir.
//
// O nome carrega o nível (`vencimentos.idx`, `vencimentos.s1.idx`, ...) e a
// magic separa folha de sumário: trocar dois arquivos de lugar produziria uma
// árvore que responde sem erro e agenda tudo errado.
func OpenSchedule(dir string, minRows uint64) (*Schedule, error) {
	a := &Schedule{levels: make([]*denseFile, 0, scheduleLevels)}
	for level := 0; level < scheduleLevels; level++ {
		name, magic := "vencimentos.idx", leavesMagic
		if level > 0 {
			name, magic = fmt.Sprintf("vencimentos.s%d.idx", level), summaryMagic
		}
		d, err := openDense(filepath.Join(dir, name), magic, minRows>>(scheduleShift*level))
		if err != nil {
			// Os níveis já abertos são fechados antes de desistir: um Open
			// que falha no terceiro arquivo não pode deixar dois mapeamentos
			// vivos sem dono. O erro do fechamento é descartado de propósito
			// — quem chamou precisa da causa da FALHA, não do resíduo dela.
			_ = a.closeAll()
			return nil, err
		}
		a.levels = append(a.levels, d)
	}
	return a, nil
}

// value lê a entrada `idx` do nível, com o ∞ implícito de tudo que está além
// do arquivo.
func (a *Schedule) value(level int, idx uint64) uint64 {
	v, ok := a.levels[level].load(types.RowID(idx))
	if !ok {
		return Never
	}
	return v
}

// Get devolve o vencimento agendado para a linha, ou Never.
func (a *Schedule) Get(rowID types.RowID) uint64 { return a.value(0, uint64(rowID)) }

// Update grava o vencimento da linha mantendo a invariante em todo instante.
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
func (a *Schedule) Update(rowID types.RowID, expiry uint64) error {
	old := a.value(0, uint64(rowID))
	if old == expiry {
		return nil
	}
	if orderKey(expiry) < orderKey(old) {
		for level := len(a.levels) - 1; level >= 1; level-- {
			idx := uint64(rowID) >> (scheduleShift * level)
			if orderKey(a.value(level, idx)) > orderKey(expiry) {
				if err := a.write(level, idx, expiry); err != nil {
					return err
				}
			}
		}
		return a.write(0, uint64(rowID), expiry)
	}

	if err := a.write(0, uint64(rowID), expiry); err != nil {
		return err
	}
	// prev é o valor que o nó do nível de BAIXO tinha antes desta
	// atualização. No primeiro giro é a própria folha; depois é o valor que o
	// nó do nível anterior tinha, e é contra ele que se decide se o pai ainda
	// dependia deste ramo.
	prev := old
	for level := 1; level < len(a.levels); level++ {
		idx := uint64(rowID) >> (scheduleShift * level)
		current := a.value(level, idx)
		if orderKey(current) < orderKey(prev) {
			// O mínimo deste nó vinha de outro filho: nada acima muda.
			return nil
		}
		if a.afterRecompute != nil {
			a.afterRecompute()
		}
		recomputed := a.minOfChildren(level, idx)
		if recomputed == current {
			return nil
		}
		if err := a.write(level, idx, recomputed); err != nil {
			return err
		}
		prev = current
	}
	return nil
}

// write escreve uma entrada e aciona o seam. Todo caminho de escrita da
// agenda passa por aqui: uma escrita direta no denseFile ficaria invisível
// para o teste que confere a ordem.
func (a *Schedule) write(level int, idx, v uint64) error {
	if err := a.levels[level].store(types.RowID(idx), v); err != nil {
		return err
	}
	if a.afterWrite != nil {
		a.afterWrite()
	}
	return nil
}

// minOfChildren recomputa o mínimo exato de um nó, lendo os scheduleB filhos.
func (a *Schedule) minOfChildren(level int, idx uint64) uint64 {
	base := idx << scheduleShift
	childMin := Never
	for i := uint64(0); i < scheduleB; i++ {
		v := a.value(level-1, base+i)
		if v == Never {
			continue
		}
		if childMin == Never || v < childMin {
			childMin = v
		}
	}
	return childMin
}

// entries devolve quantas entradas cabem no arquivo do nível.
func (a *Schedule) entries(level int) uint64 {
	n := a.levels[level].region.Size() - headerSize
	if n <= 0 {
		return 0
	}
	return uint64(n / entrySize)
}

// NextExpiry devolve o instante do vencimento mais próximo, e ok=false
// quando nenhuma linha está agendada.
//
// É um limite INFERIOR do vencimento real, pela invariante: dormir até aqui
// pode acordar o coletor cedo — que custa uma descida sem achado —, nunca
// tarde.
func (a *Schedule) NextExpiry() (uint64, bool) {
	top := len(a.levels) - 1
	minExpiry := Never
	for idx, n := uint64(0), a.entries(top); idx < n; idx++ {
		v := a.value(top, idx)
		if v == Never {
			continue
		}
		if minExpiry == Never || v < minExpiry {
			minExpiry = v
		}
	}
	return minExpiry, minExpiry != Never
}

// VisitExpired visita, em ordem de rowId, cada linha cujo vencimento agendado é
// menor ou igual a `cutoff`. A visita que devolve false interrompe a descida.
//
// Devolve quantas ENTRADAS a descida tocou — os nós de sumário em que ela
// entrou mais as folhas entregues. É a medida do esforço, e ela existe para
// poder ser afirmada: uma implementação que varresse o dataset entregaria as
// mesmas linhas e não tocaria entrada nenhuma da árvore, então contar só o que
// foi entregue não distinguiria as duas.
//
// A entrega é COMPLETA pela invariante: se uma linha vence, todo ancestral
// dela tem valor <= o vencimento dela <= `cutoff`, então nenhum ramo que a
// contenha é pulado. Pode entregar linhas a mais — um nó frouxo faz descer
// num ramo sem nada vencido —, e é o chamador que revalida.
func (a *Schedule) VisitExpired(cutoff uint64, visit func(types.RowID) bool) int {
	top := len(a.levels) - 1
	touched := 0
	for idx, n := uint64(0), a.entries(top); idx < n; idx++ {
		v := a.value(top, idx)
		if v == Never || v > cutoff {
			continue
		}
		touched++
		if !a.descend(top, idx, cutoff, visit, &touched) {
			return touched
		}
	}
	return touched
}

// descend percorre os filhos de um nó, e devolve false quando a visita pediu
// para parar. `touched` acumula as entradas em que a descida ENTROU.
func (a *Schedule) descend(level int, idx, cutoff uint64, visit func(types.RowID) bool, touched *int) bool {
	base := idx << scheduleShift
	for i := uint64(0); i < scheduleB; i++ {
		child := base + i
		v := a.value(level-1, child)
		if v == Never || v > cutoff {
			continue
		}
		*touched++
		if level-1 == 0 {
			if !visit(types.RowID(child)) {
				return false
			}
			continue
		}
		if !a.descend(level-1, child, cutoff, visit, touched) {
			return false
		}
	}
	return true
}

// NeedsRebuild reporta se a árvore em disco não serve para o dataset que
// tem `limit` linhas, e tem de ser reconstruída antes de qualquer uso.
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
func (a *Schedule) NeedsRebuild(limit types.RowID) bool {
	for level := range a.levels {
		if a.levels[level].born {
			return true
		}
	}
	if limit == 0 {
		return false
	}
	for level := range a.levels {
		if a.entries(level) <= uint64(limit-1)>>(scheduleShift*level) {
			return true
		}
	}
	return false
}

// Rebuild reescreve a árvore inteira a partir de expiry, que devolve o
// vencimento de cada linha. Exclusivo do writer, e sem leitores — é o caminho
// do Open.
//
// Preenche as folhas linearmente e só então computa os sumários de baixo para
// cima, em vez de chamar Update linha a linha. As duas formas dão a mesma
// árvore; esta paga uma escrita por entrada em vez de uma por entrada mais os
// abaixamentos em cascata que a ordem de chegada provocaria, e lê cada nível
// uma vez só, sequencialmente. É o que mantém a construção de milhões de
// linhas na casa dos milissegundos, e é o que torna "construir na abertura"
// preferível a "exigir recriação do dataset".
//
// Todo o arquivo de cada nível é reescrito, e não só a parte derivada de
// limit: quando a reconstrução acontece por divergência de parâmetros, o que
// está em disco é uma árvore VÁLIDA de outro mundo, e deixar qualquer entrada
// dela para trás misturaria as duas.
func (a *Schedule) Rebuild(limit types.RowID, expiry func(types.RowID) uint64) error {
	for id := types.RowID(0); id < limit; id++ {
		if err := a.levels[0].store(id, expiry(id)); err != nil {
			return err
		}
	}
	for level := 1; level < len(a.levels); level++ {
		n := max(a.entries(level), uint64(limit)>>(scheduleShift*level)+1)
		for idx := uint64(0); idx < n; idx++ {
			if err := a.levels[level].store(types.RowID(idx), a.minOfChildren(level, idx)); err != nil {
				return err
			}
		}
	}
	return nil
}

// Check verifica a invariante em toda a árvore, e é o que dá ao teste de
// crash uma afirmação a fazer.
//
// Reprova só o que é FATAL: nó acima do mínimo dos filhos, que faz a descida
// pular linhas vencidas. Nó abaixo passa, porque é o estado que toda escrita
// interrompida deixa e é recuperável por conta própria — o recompute da
// primeira renovação daquele bloco o aperta de volta.
func (a *Schedule) Check() error {
	for level := 1; level < len(a.levels); level++ {
		for idx, n := uint64(0), a.entries(level); idx < n; idx++ {
			node := a.value(level, idx)
			childMin := a.minOfChildren(level, idx)
			if orderKey(node) > orderKey(childMin) {
				return fmt.Errorf("rowindex: agenda com nível %d bloco %d em %d, acima do mínimo %d dos filhos: a descida pularia esse ramo",
					level, idx, node, childMin)
			}
		}
	}
	return nil
}

// Msync força as páginas sujas de todos os níveis para disco.
func (a *Schedule) Msync() error {
	for level, d := range a.levels {
		if err := d.region.Msync(); err != nil {
			return fmt.Errorf("rowindex: msync do nível %d da agenda: %w", level, err)
		}
	}
	return nil
}

// Close libera os mapeamentos.
func (a *Schedule) Close() error { return a.closeAll() }

func (a *Schedule) closeAll() error {
	var first error
	for _, d := range a.levels {
		if err := d.region.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
