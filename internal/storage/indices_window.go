package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/storage/rowindex"
	"github.com/nishisan-dev/nishidb/internal/storage/segment"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// JanelaDeIndice é quanto do dataset vale indexar (spec §4).
//
// Fora da janela a consulta CONTINUA respondendo: o segmento sem índice é
// varrido por enumeração, que é correta e mais lenta. A janela decide custo, e
// nunca resultado — e é por isso que ela pode ser apertada sem risco de
// resposta errada.
//
// A simetria com a retenção é deliberada: `history.maxBytes` limita o DADO,
// `history.index.maxBytes` limita o ÍNDICE, e nos dois casos o teto de bytes é
// a política forte que engole o prazo declarado. Quem entendeu uma entende a
// outra sem documentação nova.
//
// Ela é EXCLUSIVA da tabela histórica (D1). No dataset materializado a linha
// vive no segmento corrente independente da idade dela, e a janela dele é
// infinita por construção.
type JanelaDeIndice struct {
	// Duracao é a fronteira por tempo. Zero é SEM fronteira por tempo — nunca
	// "janela de duração zero", que o schema recusa no load por ser ambíguo
	// entre "nada" e "tudo".
	Duracao time.Duration
	// MaxBytes é o teto dos bytes de ÍNDICE, e não de dado indexado. A razão
	// está no comentário do bytesDosSegmentos: um teto que a purga não consegue
	// perseguir seria um teto que mente, e o que a expulsão da janela devolve é
	// .sidx. Zero é sem teto.
	MaxBytes int64
}

// Infinita reporta a janela que não corta nada: indexa tudo que a retenção
// mantém. É o bloco `history.index` ausente, o default, e o comportamento que o
// dataset materializado tem por construção.
func (j JanelaDeIndice) Infinita() bool { return j.Duracao <= 0 && j.MaxBytes <= 0 }

// janelaDeSegmentos é o retrato que a EXPULSÃO e a PENDÊNCIA compartilham.
//
// Elas compartilham porque a alternativa é o moinho, e o moinho não é
// hipotético: se as duas responderem "este segmento deve ter índice?" por
// contas diferentes, a que responde "sim" reconstrói o que a que responde "não"
// acabou de expulsar, para sempre. É a doutrina de nunca ter a mesma coisa em
// dois lugares, aplicada a uma DECISÃO em vez de a um número.
type janelaDeSegmentos struct {
	// dentro são os segmentos selados que DEVEM ter índice.
	dentro map[uint32]struct{}
	// comIndice são os segmentos selados que TÊM .sidx hoje, em ordem
	// crescente. A diferença entre os dois conjuntos é exatamente o que a
	// expulsão remove.
	comIndice []uint32
	// tamanho são os bytes do .sidx de cada segmento SELADO, zero para quem não
	// tem. As chaves dele são, por construção, os segmentos vivos que a janela
	// governa — e é isso que a expulsão usa para podar o tamanhoExpulso.
	tamanho map[uint32]int64
}

// AplicaJanela expulsa os .sidx dos segmentos que saíram da janela e devolve
// quantos saíram.
//
// Ela roda DENTRO da passada de retenção (§4.2), que já é de um minuto, já é
// exclusiva do histórico e já tem em mãos a lista de segmentos e os tamanhos —
// um laço de fundo a menos. A RECONSTRUÇÃO é goroutine própria porque vale para
// os dois modos de dataset; a expulsão, não.
//
// A janela finita sobre um dataset materializado é ERRO de chamador, e não
// configuração a ignorar: ali a linha vive no segmento corrente independente da
// idade dela, e cortar por tempo tiraria o índice de dado corrente. O schema já
// recusa `history.index` sem `history:`, então chegar aqui com janela finita
// num materializado só acontece por bug de quem monta os limites — e ignorar em
// silêncio esconderia esse bug atrás de um índice que some.
func (s *Store) AplicaJanela(ctx context.Context, j JanelaDeIndice) (int, error) {
	if err := s.exigeJanelaAplicavel(j); err != nil {
		return 0, err
	}
	if j.Infinita() {
		return 0, nil
	}
	// Uma passada por vez, e o MESMO lock da retenção: a expulsão e a pendência
	// precisam se enxergar como atômicas uma para a outra — ver o godoc do
	// SegmentosPendentesDeReconstrucao.
	s.retPassadaMu.Lock()
	defer s.retPassadaMu.Unlock()
	if err := s.entra(); err != nil {
		return 0, err
	}
	defer s.sai()
	return s.aplicaJanela(ctx, j)
}

// aplicaJanela é o corpo sem o lock de vida, para quem já o tem na mão — a
// passada de retenção. Aninhar dois RLock do mesmo RWMutex trava contra um
// escritor pendente, então o par exportado/interno aqui não é estilo, é
// necessidade.
func (s *Store) aplicaJanela(ctx context.Context, j JanelaDeIndice) (int, error) {
	if j.Infinita() || len(s.camposIndexados) == 0 {
		return 0, nil
	}
	refs, err := s.listaSegmentos()
	if err != nil {
		return 0, err
	}
	estado, err := s.janelaDeSegmentos(j, refs)
	if err != nil {
		return 0, err
	}
	var expulsos int
	for _, seg := range estado.comIndice {
		if ctx.Err() != nil {
			// Cancelamento não é falha: o que sobrou sai na próxima passada,
			// que é daqui a um minuto. É a mesma leitura que o
			// aposentaOQueVenceu faz do ctx.
			return expulsos, nil
		}
		if _, dentro := estado.dentro[seg]; dentro {
			continue
		}
		// O tamanho é lembrado ANTES do unlink, porque depois dele o arquivo já
		// não pode ser medido — e é justamente essa medida que impede a passada
		// seguinte de readmitir o segmento por subestimá-lo.
		s.lembraTamanhoExpulso(seg, estado.tamanho[seg])
		// O unlink vem ANTES do esquecimento, e é o removeSidxDoDisco quem
		// impõe a ordem — o godoc dele explica a corrida que a inversa abria.
		removido, err := s.removeSidxDoDisco(seg)
		if err != nil {
			// Erro NÃO derruba a passada, e a assimetria com o resto da retenção
			// é deliberada: quem chama isto para de vez na primeira falha
			// (rodaRetencao), e um unlink que falhou num arquivo DERIVADO não
			// justifica parar de devolver o disco do DADO. O índice que sobrou é
			// inofensivo — ele responde candidatos que o executor revalida
			// (§44) — e a passada seguinte tenta de novo.
			s.opts.Log.Warn("indice.janela_nao_expulsou",
				"o índice do segmento fora da janela continuou em disco; ele é inofensivo e a próxima passada tenta de novo",
				"dataset", s.opts.Dataset, "segmento", seg, "erro", err.Error())
			continue
		}
		if removido {
			expulsos++
		}
	}
	// A poda vem no fim da passada, e não a cada expulsão: por expulsão ela
	// seria O(n) dentro de um laço de n, para um mapa que só encolhe.
	s.podaTamanhosExpulsos(estado.tamanho)
	return expulsos, nil
}

// exigeJanelaAplicavel recusa janela finita fora da tabela histórica. Uma
// função porque os DOIS consumidores da janela — a expulsão e a pendência —
// precisam recusar o mesmo, e uma segunda cópia divergiria.
func (s *Store) exigeJanelaAplicavel(j JanelaDeIndice) error {
	if j.Infinita() || s.EhHistorico() {
		return nil
	}
	return fmt.Errorf("%w: janela de indexação finita sobre %q, que é materializado; "+
		"a janela é política da tabela histórica, e no materializado ela é infinita por construção",
		ErrInvalidArgument, s.opts.Dataset)
}

// janelaDeSegmentos decide, numa conta só, quais segmentos selados devem ter
// índice.
//
// # A FRONTEIRA POR TEMPO USA O RECORD MAIS NOVO
//
// O segmento está fora quando o record MAIS NOVO dele é anterior a
// `agora − Duracao`. O mais novo, e não o mais antigo, porque a janela promete
// "consigo consultar rápido as últimas N horas": um segmento que ATRAVESSA a
// fronteira contém records que a promessa cobre, e tirar o índice dele seria
// entregar menos do que se prometeu, sem erro e sem log. É a assimetria
// espelhada da retenção, que só aposenta o segmento INTEIRAMENTE coberto pela
// marca-d'água (candidatoDeRetencao).
//
// Ela não custa metadado novo: o record mais novo do segmento N é o
// imediatamente anterior ao primeiro do N+1, e o primeiro do N+1 já é lido com
// um pread pelo mesmo caminho que a retenção usa.
//
// A varredura é do MAIS NOVO para o mais antigo e PARA no primeiro que ficou
// fora, porque o _writtenAt é não-decrescente por rowId na tabela histórica —
// a mesma propriedade que permite a busca binária da marca-d'água. Instante
// DESCONHECIDO não para a varredura e não expulsa ninguém: é a mesma regra do
// avancaMarcaDagua, e presumir vencido apagaria índice por omissão.
//
// # O TETO DE BYTES, E POR QUE O SEGMENTO NÃO CONSTRUÍDO É COBRADO
//
// Pelos bytes, a soma vai do mais novo para o mais antigo e para onde passa do
// teto. O que decide a correção é o preço do segmento que AINDA NÃO TEM índice,
// e cobrar-lhe zero — que é o que a soma literal dos .sidx existentes faz — é o
// moinho, exatamente:
//
//  1. dois segmentos cabem no orçamento porque nenhum dos dois custa nada
//     ainda; os dois entram na fila e são construídos;
//  2. a expulsão soma os dois .sidx de verdade, estoura o teto e expulsa o mais
//     antigo;
//  3. a pendência pergunta de novo, o expulso volta a custar zero, cabe, entra
//     na fila — e volta ao passo 2. Para sempre, sem oscilação externa nenhuma.
//
// O que se cobra dele é o MAIOR entre duas coisas:
//
//	max( maior .sidx que existe em disco,  o tamanho que ESTE segmento tinha
//	     quando a janela o expulsou )
//
// A segunda parcela (tamanhoExpulso) não é opcional, e a primeira sozinha É
// DEFEITUOSA — o estimador "maior em disco" NÃO é monótono, porque a própria
// expulsão apaga arquivos e portanto move o maior. Traço com cinco selados, o
// mais velho atípico (40M) e os outros quatro pequenos (4M), teto 20M:
//
//	passada 1   maior=40M → s1 não cabe (16+40 > 20) → expulso
//	pendência   maior=4M  ← o de 40M ACABOU de ser apagado
//	                      → s1 cobrado 4M → 16+4 = 20 ≤ 20 → ADMITIDO
//	                      → reconstruído, e volta a ocupar 40M
//	passada 2   maior=40M → estoura → expulso. Para sempre.
//
// É o mesmo moinho de cima com "custo zero" trocado por "custo subestimado", e
// a condição geral é `tamanho_real(b) > max(mantidos)` com folga de orçamento —
// exatamente o segmento de cardinalidade atípica. Com o tamanhoExpulso, o passo
// da pendência cobra 40M de s1 porque foi isso que ele custou: para o caso do
// moinho a cobrança é EXATA, não estimativa, e não há aproximação a errar.
//
// Para o segmento NUNCA construído continua valendo o maior em disco. Maior, e
// não a média, porque a estimativa tem de errar para o lado seguro:
// superestimar deixa um segmento sem índice, e sem índice ele responde por
// ENUMERAÇÃO, que é correta; subestimar publica um índice que a passada
// seguinte expulsa, que é o moinho. É barato e é razoável porque os segmentos
// de um dataset rotacionam no mesmo tamanho e o dado é homogêneo, então os
// .sidx ficam próximos (§1.3: 16 × distintos + 4 × linhas). O maior é medido
// sobre TODOS os selados, inclusive os que a fronteira por tempo já deixou de
// fora: eles são a mesma amostra e o stat deles já aconteceu.
//
// A CONSEQUÊNCIA, e ela é surpreendente para quem só leu o `maxBytes`: um .sidx
// atípico ENCOLHE a janela efetiva, porque passa a ser o preço cobrado de quem
// ainda não foi construído. Onde o operador enxerga isso é a JanelaEfetiva: ela
// mostra a cobertura REAL ficando menor que o `window` declarado, e é essa
// comparação que transforma o conservadorismo em ajuste de configuração em vez
// de chamado de suporte.
//
// O tamanhoExpulso vive em MEMÓRIA. No restart o estimador volta ao
// conservador, e o dataset paga UMA reconstrução desperdiçada por segmento
// atípico antes de estabilizar de novo. É o preço honesto de não persistir, e é
// limitado: a segunda passada já lembra o tamanho real.
//
// ENQUANTO NENHUM .sidx EXISTE não há o que medir, e aí o teto admite SÓ O MAIS
// NOVO — nunca "todo mundo por custar zero". A diferença decide a migração: num
// dataset com milhares de selados aberto pela primeira vez, admitir todos
// mandaria o laço da §2.4 varrer o dataset INTEIRO antes de a primeira expulsão
// cortar. Admitindo um, a passada seguinte já tem a medição e admite quantos
// couberem. É a mesma forma do aposentaAteCaber, que fica acima do teto quando
// não sobrou o que aposentar — um teto que ainda não tem contra o que ser
// aferido não pode ser perseguido.
//
// O MAIS NOVO DENTRO DA JANELA POR TEMPO NUNCA SAI SÓ POR BYTES, espelhando o
// "só sobrou o segmento corrente" da retenção: sem isso, um teto menor que um
// único índice zeraria o índice do dataset inteiro e a janela viraria
// decorativa.
//
// # AS ALTERNATIVAS RECUSADAS, E ELAS VOLTAM A SER TENTADORAS
//
// Três, e todas parecem simplificações:
//
//   - A FRONTEIRA PELO SEGMENTO INDEXADO MAIS ANTIGO QUE SOBROU — quem foi
//     expulso é mais velho que ele, logo está fora, e nunca volta. Mata o
//     moinho e vira CATRACA: alargar a janela ou subir o teto não traria
//     ninguém de volta, e a reversibilidade é metade do que a §4.4 promete.
//   - ESTIMAR PELO PRÓPRIO .dat (§1.3, com `distintos ≤ linhas` dando o limite
//     de 20 bytes por linha): é monótona por construção e sem estado nenhum —
//     mas superestima ~4,6× contra os ~4,3 bytes/linha medidos para `node`, e
//     encolheria a janela declarada por um fator que o operador não pediu.
//   - "O MAIOR JÁ OBSERVADO", guardado uma vez: mesma simplicidade do
//     tamanhoExpulso, mas infla PERMANENTEMENTE a estimativa de TODO segmento
//     por causa de um único atípico, em vez de só a dele.
func (s *Store) janelaDeSegmentos(j JanelaDeIndice, refs []segment.SegmentRef) (janelaDeSegmentos, error) {
	corrente := s.curSegID.Load()
	// Candidatos são os SELADOS, do mais novo para o mais antigo. O corrente
	// fica de fora porque o índice dele é o WIP, que não é .sidx e que a
	// abertura reconstrói — a janela não tem o que expulsar ali.
	var cand []int
	for i := len(refs) - 1; i >= 0; i-- {
		if refs[i].ID != corrente {
			cand = append(cand, i)
		}
	}
	est := janelaDeSegmentos{
		dentro:  make(map[uint32]struct{}, len(cand)),
		tamanho: make(map[uint32]int64, len(cand)),
	}
	tamanho := est.tamanho
	var maior int64
	for _, i := range cand {
		b := s.bytesDoIndiceDoSegmento(refs[i].ID)
		tamanho[refs[i].ID] = b
		if b > maior {
			maior = b
		}
	}
	// comIndice sai em ordem CRESCENTE porque é ela que a expulsão percorre, e
	// um log de expulsão fora de ordem esconderia a forma do corte.
	for i := len(cand) - 1; i >= 0; i-- {
		if id := refs[cand[i]].ID; tamanho[id] > 0 {
			est.comIndice = append(est.comIndice, id)
		}
	}

	dentroPorTempo := cand
	if j.Duracao > 0 {
		corte := s.now().Add(-j.Duracao).UnixMilli()
		dentroPorTempo = nil
		var buf []byte
		for _, i := range cand {
			ms, out, conhecido, err := s.instanteDoRecordMaisNovo(refs, i, buf)
			buf = out
			if err != nil {
				return janelaDeSegmentos{}, err
			}
			if conhecido && ms < corte {
				break
			}
			dentroPorTempo = append(dentroPorTempo, i)
		}
	}

	if j.MaxBytes <= 0 {
		for _, i := range dentroPorTempo {
			est.dentro[refs[i].ID] = struct{}{}
		}
		return est, nil
	}
	var acumulado int64
	for k, i := range dentroPorTempo {
		id := refs[i].ID
		custo := tamanho[id]
		if custo == 0 {
			// Não construído: o maior em disco, ou o que ELE mesmo custou da
			// última vez, o que for maior. O max é o que torna a cobrança
			// monótona — sem ele, apagar o maior arquivo BAIXA a estimativa de
			// quem acabou de ser apagado.
			custo = max(maior, s.tamanhoExpulso[id])
		}
		// custo == 0 aqui só acontece com NADA medido no dataset inteiro: nem
		// arquivo em disco, nem memória de expulsão. Admitir todo mundo por
		// "custar zero" mandaria o laço da §2.4 varrer um dataset inteiro antes
		// de a primeira expulsão cortar; admitir só o mais novo dá a medição
		// para a passada seguinte decidir de verdade.
		if k > 0 && (custo == 0 || acumulado+custo > j.MaxBytes) {
			break
		}
		acumulado += custo
		est.dentro[id] = struct{}{}
	}
	return est, nil
}

// lembraTamanhoExpulso guarda o que o .sidx do segmento ocupava no instante em
// que a janela o apagou. Sob o retPassadaMu, como todo acesso ao mapa.
func (s *Store) lembraTamanhoExpulso(seg uint32, bytes int64) {
	if bytes <= 0 {
		return
	}
	if s.tamanhoExpulso == nil {
		s.tamanhoExpulso = make(map[uint32]int64, 1)
	}
	s.tamanhoExpulso[seg] = bytes
}

// podaTamanhosExpulsos descarta a memória de segmentos que já não existem.
//
// A poda é contra a lista de segmentos VIVOS, e não por idade: um dataset que
// aposenta segmentos o dia inteiro acumularia uma entrada por segmento morto,
// para sempre. `vivos` é o mapa de tamanhos da passada, cujas chaves são por
// construção os selados que a janela governa.
func (s *Store) podaTamanhosExpulsos(vivos map[uint32]int64) {
	for id := range s.tamanhoExpulso {
		if _, vivo := vivos[id]; !vivo {
			delete(s.tamanhoExpulso, id)
		}
	}
}

// bytesDoIndiceDoSegmento devolve os BLOCOS alocados pelo .sidx do segmento, ou
// zero quando ele não existe.
//
// Blocos, e não o tamanho aparente, pela mesma razão do blocosDoDiretorioSe: o
// teto é uma promessa sobre DISCO, e é em bloco que o disco é cobrado. Somar o
// tamanho aparente faria o teto perseguir um número que o dispositivo não
// conhece.
//
// Erro de leitura devolve zero, e não erro: um .sidx que sumiu entre a listagem
// e este stat é o estado normal de um segmento recém-aposentado, e derrubar a
// passada de retenção por causa dele seria parar de devolver disco por uma
// corrida cujo desfecho correto é ignorar o arquivo.
func (s *Store) bytesDoIndiceDoSegmento(seg uint32) int64 {
	info, err := os.Stat(segindex.SealedPath(s.dirDeDados(), seg))
	if err != nil {
		return 0
	}
	return blocosDe(info)
}

// instanteDoRecordMaisNovo devolve o _writtenAt do ÚLTIMO record do segmento
// refs[i].
//
// Ele é lido sem metadado novo: o último record do segmento N é o rowId
// imediatamente anterior ao primeiro do N+1, e o primeiro do N+1 está sempre no
// offset fixo logo após o header. Um metadado a menos é uma janela de crash a
// menos, que é a mesma escolha do primeiroRowIDDoSeguinte.
//
// Segmentos VAZIOS entre N e o próximo povoado são pulados, e não fazem a
// função desistir: um segmento recém-criado ainda sem record não tem primeiro
// rowId para ler, e parar ali diria "instante desconhecido" para um segmento
// cujo instante é perfeitamente conhecido.
//
// conhecido=false quando não há segmento seguinte povoado, quando a location do
// último record já não existe (prefixo purgado pela marca-d'água) ou quando o
// record não carrega o campo de escrita. Nos três casos o chamador MANTÉM o
// índice: é a mesma regra do avancaMarcaDagua, onde instante desconhecido nunca
// vence — presumir o contrário apagaria índice por omissão.
//
// O par (resolver a location, ler os bytes) roda sob o aposMu, que é a barreira
// que impede o descritor do segmento de fechar no meio do pread. O buffer é
// PARÂMETRO e não o s.retBuf: aquele é reutilizado sob o retPassadaMu, e esta
// função também é chamada pela pendência, que roda fora dele.
func (s *Store) instanteDoRecordMaisNovo(refs []segment.SegmentRef, i int, buf []byte) (int64, []byte, bool, error) {
	s.aposMu.RLock()
	defer s.aposMu.RUnlock()
	var (
		primeiro types.RowID
		achou    bool
	)
	for k := i + 1; k < len(refs); k++ {
		p, out, ok, err := s.primeiroRowIDDe(refs[k], buf)
		buf = out
		if err != nil {
			return 0, buf, false, err
		}
		if ok {
			primeiro, achou = p, true
			break
		}
	}
	if !achou || primeiro == 0 {
		return 0, buf, false, nil
	}
	packed, ok := s.locs.Get(primeiro - 1)
	if !ok {
		return 0, buf, false, nil
	}
	seg, off, _ := rowindex.Unpack(packed)
	rec, out, err := s.segR.ReadRecord(seg, off, buf)
	buf = out
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// Segmento aposentado entre a listagem e o pread: ele já não tem
			// índice a expulsar, e errar aqui derrubaria a passada por uma
			// corrida cujo desfecho correto é ignorar.
			return 0, buf, false, nil
		}
		return 0, buf, false, err
	}
	ms, conhecido := writeInstant(rec.Payload, s.writeFieldId)
	return ms, buf, conhecido, nil
}

// JanelaEfetiva é o intervalo de tempo REALMENTE coberto pelos segmentos
// indexados.
//
// Ela existe para uma pergunta só, e é a pergunta que o operador faz quando a
// consulta ficou lenta: o `window` que eu declarei é o que está valendo? Comparada
// com ele, ela diz numa olhada se o teto de bytes assumiu o comando — do mesmo
// jeito que a série de instante mais antigo mostra o `history.maxBytes`
// engolindo o `expireAfterWrite`. Sem ela, o conservadorismo da cobrança do
// segmento não construído seria invisível, e comportamento conservador que
// ninguém enxerga vira chamado de suporte.
//
// O segmento CORRENTE conta como indexado, e ele é o único que conta sem .sidx:
// o índice dele é o WIP, que a abertura reconstrói antes do primeiro Put. Sem
// isso, um teto tão apertado que expulsasse todos os selados publicaria janela
// efetiva ZERO num dataset cujo segmento corrente responde por índice — um
// número que mente na direção alarmante.
//
// Ela é medida da MARCA-D'ÁGUA para cá, e não do primeiro record físico, porque
// a série irmã (InstanteMaisAntigo) também é — e é a comparação entre as duas
// que os documentos mandam o operador fazer. Ver instanteVisivelDe.
//
// Zero significa "não há janela a reportar": dataset materializado, dataset sem
// campo indexado, tabela vazia, ou dado sem o campo de escrita. É número de
// painel, então erro de leitura devolve zero em vez de subir — um diretório que
// mudou no meio da varredura não justifica derrubar o /v1/stats.
//
// O dataset MATERIALIZADO devolve zero pela mesma razão que o
// instanteMaisAntigo devolve: a pergunta não existe ali. A janela dele é
// infinita por construção (D1), e publicar um número finito sugeriria uma
// política que ninguém declarou — além de custar um pread por scrape para
// responder o que já se sabe.
func (s *Store) JanelaEfetiva() time.Duration {
	if !s.EhHistorico() || len(s.camposIndexados) == 0 {
		return 0
	}
	refs, err := s.listaSegmentos()
	if err != nil {
		return 0
	}
	corrente := s.curSegID.Load()
	var buf []byte
	for _, ref := range refs {
		if ref.ID != corrente && s.bytesDoIndiceDoSegmento(ref.ID) == 0 {
			continue
		}
		ms, out, ok := s.instanteVisivelDe(ref, buf)
		buf = out
		if !ok {
			// Segmento vazio, inteiro abaixo da marca, ou sem campo de escrita:
			// o seguinte responde. Não é anomalia, e desistir aqui publicaria
			// zero para um dataset com índice.
			continue
		}
		if d := time.Duration(s.now().UnixMilli()-ms) * time.Millisecond; d > 0 {
			return d
		}
		return 0
	}
	return 0
}

// instanteVisivelDe lê o _writtenAt do record mais antigo do segmento que ainda
// está VISÍVEL — o primeiro dele, ou a marca-d'água, o que vier depois.
//
// A marca-d'água entra na conta porque a JanelaEfetiva existe para ser comparada
// com o InstanteMaisAntigo, e aquele é lido NA marca. Medir daqui do primeiro
// record FÍSICO faria as duas séries falarem de pontos de partida diferentes: o
// prefixo de um segmento pode já ter vencido sem que o arquivo tenha saído do
// disco, e a janela efetiva pareceria cobrir linhas que a consulta já não vê.
// Justamente a comparação que os documentos mandam o operador fazer é a que
// ficaria errada.
//
// ok=false quando o segmento está vazio, quando ele está INTEIRO abaixo da marca
// (a location do início cai noutro segmento) ou quando o record não carrega o
// campo de escrita. Nos três casos o chamador tenta o segmento seguinte.
//
// Sob o aposMu pela razão do primeiroRowIDDe: ele impede o descritor de fechar
// entre o pread e a leitura dos bytes.
func (s *Store) instanteVisivelDe(ref segment.SegmentRef, buf []byte) (int64, []byte, bool) {
	if ref.SizeBytes <= segment.HeaderSize {
		return 0, buf, false
	}
	s.aposMu.RLock()
	defer s.aposMu.RUnlock()
	primeiro, out, ok, err := s.primeiroRowIDDe(ref, buf)
	buf = out
	if err != nil || !ok {
		return 0, buf, false
	}
	inicio := max(primeiro, s.RowIDBase())
	if uint64(inicio) >= s.nextRowID.Load() {
		return 0, buf, false
	}
	packed, ok := s.locs.Get(inicio)
	if !ok {
		return 0, buf, false
	}
	seg, off, _ := rowindex.Unpack(packed)
	if seg != ref.ID {
		// A marca já passou deste segmento inteiro: ele não tem linha visível, e
		// o instante dele não descreve cobertura nenhuma.
		return 0, buf, false
	}
	rec, out, err := s.segR.ReadRecord(seg, off, buf)
	buf = out
	if err != nil {
		return 0, buf, false
	}
	ms, ok := writeInstant(rec.Payload, s.writeFieldId)
	return ms, buf, ok
}
