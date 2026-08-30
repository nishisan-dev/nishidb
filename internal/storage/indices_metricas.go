package storage

import (
	"github.com/nishisan-dev/nishidb/internal/index/segindex"
)

// EstadosDeSegmento é a contagem de segmentos por estado de índice, e os quatro
// PARTICIONAM o dataset (spec §6.6).
//
// A partição é o produto inteiro desta struct, porque a pergunta que a migração
// faz não é "quantos segmentos estão sem índice?" — é "o laço está avançando?".
// Um rótulo só para as duas ausências não a responde: num dataset grande, o
// segmento fora da janela é a maioria esmagadora e é o sistema funcionando como
// projetado, enquanto o segmento dentro da janela sem índice é trabalho por
// fazer. Somados, o número é enorme, normal e mudo.
//
// Zero em tudo é o estado de um dataset SEM campo indexado, e também o do
// intervalo entre a subida e a primeira passada do laço de reconstrução — que é
// imediata. Ver o godoc de SegmentosPendentesDeReconstrucao, que é quem publica.
type EstadosDeSegmento struct {
	// Selado é o segmento selado, dentro da janela, cujo .sidx descreve o
	// conjunto indexado inteiro. É o estado final e o que se quer ver crescer.
	Selado int64
	// Corrente é o segmento que ainda recebe records, e é 1 ou 0: o índice dele
	// é o WIP, que a abertura reconstrói e a rotação sela.
	Corrente int64
	// Pendente é o segmento DENTRO da janela cujo índice não descreve o conjunto
	// indexado — ausente, ilegível ou com seções de menos ou de mais. É a FILA, e
	// é a série cuja queda diz que o laço está trabalhando.
	Pendente int64
	// Fora é o segmento que a janela deixou de cobrir. Ele está deliberadamente
	// sem índice, responde por enumeração e NÃO é trabalho pendente: contá-lo
	// junto com o Pendente faria a fila parecer nunca esvaziar.
	Fora int64
}

// EstatisticasDeIndice é o retrato do índice por segmento de um dataset.
//
// É a estrutura ÚNICA que o /v1/stats e o /metrics leem — nenhum contador é
// mantido dos dois lados, pela razão de sempre: dois contadores para o mesmo
// fato divergem no dia em que um caminho de erro esquece de somar, e em produção
// alguém lê o errado.
//
// Ela vale para os DOIS modos de dataset (decisão D1). No materializado a janela
// é infinita por construção, então Fora é sempre zero — e isso é informação, não
// buraco: diz que nenhum segmento daquele dataset responde por enumeração por
// política.
type EstatisticasDeIndice struct {
	// Estados são as quatro contagens, lidas dos atômicos que a passada de
	// pendência publica.
	Estados EstadosDeSegmento
	// Bytes são os BLOCOS que o índice por segmento ocupa: os .sidx selados, os
	// temporários e os .widx do segmento corrente.
	//
	// É a parcela do nishidb_history_index_bytes que este subsistema governa, e
	// não o mesmo número: aquele soma também indexes/ e checkpoint/. A separação
	// é o que torna verificável a promessa central do ciclo — com a tabela em
	// regime, ESTE número para de crescer, e ele é hoje o único componente que
	// não para mesmo com a retenção funcionando.
	Bytes int64
	// Reconstrucoes são os segmentos cujo índice a reconstrução publicou desde a
	// abertura do dataset.
	Reconstrucoes int64
	// ReconstrucaoAtiva é o 0/1 do laço da §2.4. Ele para de vez na primeira
	// falha, e sem esta série isso é indistinguível de "não há o que reconstruir".
	ReconstrucaoAtiva bool
	// CRCFalhas conta os .sidx reprovados no crc32c, uma vez por ABERTURA. Ver o
	// godoc do campo crcFalhas.
	CRCFalhas int64
	// PorIndice e PorEnumeracao contam os segmentos que os Lookups resolveram por
	// cada caminho (§3.4). São o único observável da degradação — para o planner,
	// o executor e o driver, os dois caminhos são o mesmo iterador ascendente.
	PorIndice, PorEnumeracao int64
}

// EstatisticasDeIndice lê o retrato sem tomar lock nenhum do dataset.
//
// Os seis contadores saem de atômicos, e a única coisa que custa I/O é o Bytes —
// um readdir sobre data/ com um stat por arquivo, no mesmo molde do
// blocosDoDiretorio que a EstatisticasDeRetencao já paga por scrape. Nos datasets
// históricos os dois convivem, e a passada acontece duas vezes por raspagem: é
// custo duplicado, mas não NÚMERO duplicado — os dois leem o disco pela mesma
// função, e é a função que define o que conta como índice por segmento.
//
// Nada aqui pede o retPassadaMu, e essa é a propriedade que a função existe para
// garantir: ele fica acima da cadeia vida → wmu → aposMu, e um scrape que fosse
// buscar a contagem de pendentes debaixo dele inverteria a ordem de locks num
// caminho que só aparece sob carga.
func (s *Store) EstatisticasDeIndice() EstatisticasDeIndice {
	return EstatisticasDeIndice{
		Estados: EstadosDeSegmento{
			Selado:   s.segSelados.Load(),
			Corrente: s.segCorrentes.Load(),
			Pendente: s.segPendentes.Load(),
			Fora:     s.segFora.Load(),
		},
		Bytes:             s.bytesDoIndicePorSegmento(),
		Reconstrucoes:     s.recFeitas.Load(),
		ReconstrucaoAtiva: s.ReconstrucaoAtiva(),
		CRCFalhas:         s.crcFalhas.Load(),
		PorIndice:         s.segPorIndice.Load(),
		PorEnumeracao:     s.segPorEnumeracao.Load(),
	}
}

// bytesDoIndicePorSegmento soma os BLOCOS dos arquivos de índice que moram em
// data/, ao lado do .dat.
//
// UMA função para os DOIS leitores — a série nishidb_index_bytes e a terceira
// parcela do IndexBytes da EstatisticasDeRetencao —, e não duas somas iguais:
// o espaço de nomes dos arquivos é do segindex, e uma segunda cópia do predicado
// pararia de contar sem avisar no dia em que a extensão mudasse.
//
// Blocos, e não tamanho aparente, pela mesma razão do blocosDoDiretorio: a série
// fala de DISCO, e é em bloco que o disco é cobrado.
func (s *Store) bytesDoIndicePorSegmento() int64 {
	return blocosDoDiretorioSe(s.dirDeDados(), segindex.EhArquivoDeIndice)
}

// publicaEstadosDeSegmento instala o retrato que a passada de pendência acabou
// de derivar.
//
// Chamada por quem já segura o retPassadaMu, e só por ele: os quatro números
// saem da MESMA varredura que decide a fila, então publicá-los de outro lugar
// exigiria uma segunda conta de "este segmento deve ter índice?" — que é
// exatamente o moinho que a janela-parâmetro existe para impedir, agora com a
// métrica como terceiro discordante.
//
// Os quatro são atômicos INDEPENDENTES, então uma raspagem pode apanhar dois
// deles de passadas diferentes e ver a soma furada por um segmento. É aceito de
// propósito: publicar um instantâneo coerente exigiria um ponteiro atômico para
// uma struct, ou seja uma alocação por passada, para servir um painel cuja
// leitura é uma TENDÊNCIA — a fila encolhendo — e não um total exato.
func (s *Store) publicaEstadosDeSegmento(e EstadosDeSegmento) {
	s.segSelados.Store(e.Selado)
	s.segCorrentes.Store(e.Corrente)
	s.segPendentes.Store(e.Pendente)
	s.segFora.Store(e.Fora)
}
