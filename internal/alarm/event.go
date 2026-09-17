package alarm

import "time"

// Estados e motivos de um evento de alarme. São CONTRATO com quem consome o
// canal: strings estáveis em inglês, como as chaves de log e os nomes de
// métrica.
const (
	// StateFiring: a condição se sustentou pelo prazo declarado.
	StateFiring = "firing"
	// StateResolved: o alarme que estava disparado deixou de estar.
	StateResolved = "resolved"

	// ReasonConditionCleared: o problema passou — a condição não vale mais.
	ReasonConditionCleared = "condition_cleared"
	// ReasonGroupRetired: o assunto desapareceu — o grupo saiu do contador.
	// É distinto do anterior de propósito: um equipamento removido do
	// inventário não é um equipamento que voltou ao normal, e quem gera evento
	// a partir disto trata os dois de formas diferentes.
	ReasonGroupRetired = "group_retired"
	// ReasonDisqualified: o grupo deixou de ser elegível — o `qualifyWhen`
	// passou a ser falso com a condição ainda valendo. É distinto dos dois
	// anteriores: a OLT morta há mais de um dia não voltou (a condição vale) e
	// não saiu do inventário (o grupo existe); ela deixou de ser o assunto do
	// alarme, e só volta a disparar depois de requalificar.
	ReasonDisqualified = "disqualified"
)

// Column é uma coluna da linha do grupo alarmado: do groupBy, em
// Event.Columns, ou de uma métrica de janela, em Event.Metrics.
//
// O valor vai como TEXTO porque o canal é uma linha de log JSON, e é o mesmo
// texto que a coluna teria num SELECT da tabela do contador. Quem precisar do
// tipo o encontra em `_meta.counters` e `_meta.counter_metrics` — carregá-lo
// aqui duplicaria no evento um dado que já é consultável.
type Column struct {
	Name  string
	Value string
}

// Event é o que sai quando um alarme dispara ou resolve.
//
// É UM tipo, e não um por canal: o log é o único destino hoje, e quando um
// tópico ou um webhook entrarem eles publicam ESTE evento. Um segundo formato
// por canal divergiria do primeiro na primeira correção, e quem consome os dois
// veria dois fatos onde há um.
type Event struct {
	Dataset string
	Counter string
	Alarm   string
	// State é StateFiring ou StateResolved.
	State string
	// Reason acompanha StateResolved; vazio no disparo.
	Reason string
	// GroupKey é o texto canônico do grupo, como a coluna groupKey o mostra.
	GroupKey string
	// Columns são as colunas do groupBy abertas, na ordem declarada. Existem
	// além do GroupKey porque quem recebe o evento costuma querer filtrar por
	// uma delas, e repartir o texto do grupo exigiria conhecer o keyFormat.
	Columns []Column
	// Total é o valor observado no instante da decisão — não o do disparo
	// original, quando o evento é uma resolução.
	Total int64
	// ValueColumn é a coluna de `_counters.<contador>` cujo valor decidiu o
	// disparo — `total` por padrão, ou a coluna declarada em `value:` do
	// alarme. Vai SÓ no disparo (vazia na resolução): a resolução não
	// descreve um valor novo, descreve que o problema passou.
	ValueColumn string
	// FiringValue é o valor de ValueColumn lido do MESMO payload que decidiu
	// o disparo — a mesma garantia de Metrics, e não uma releitura. Só faz
	// sentido com HasFiringValue, e só acompanha o disparo.
	FiringValue float64
	// HasFiringValue diz se ValueColumn estava presente no payload do
	// disparo. Uma coluna de métrica de janela sem amostra ainda (NULL) sai
	// com false: um zero aqui afirmaria um valor que ninguém observou.
	HasFiringValue bool
	// Condition e SustainedFor são a regra que produziu o evento. Vão no
	// evento porque quem o recebe não tem o YAML, e "total baixo" sem o limiar
	// não é acionável.
	Condition    string
	SustainedFor time.Duration
	// QualifyCondition é o `qualifyWhen` declarado; vazio quando o alarme não
	// o declara. Vai no evento pelo mesmo motivo de Condition: um
	// `disqualified` sem o limiar de elegibilidade não diz o que mudou.
	QualifyCondition string
	// Metrics são as colunas das métricas de janela do grupo no instante da
	// decisão, na ordem declarada (métrica, depois agregado). Coluna sem
	// amostra (NULL) não entra: um zero aqui afirmaria uma média que ninguém
	// mediu. Vazio para o grupo aposentado, que a varredura não viu.
	//
	// São os números que explicam a decisão — `auth_24h_max` abaixo do limiar
	// é o porquê de um `disqualified` —, e quem recebe o evento não tem como
	// consultá-los depois: a janela terá andado.
	Metrics []Column
	// Since é quando a condição passou a valer, e At o instante do evento.
	Since time.Time
	At    time.Time
	// Restored diz que o Since foi lido do disco, e não observado por este
	// processo.
	Restored bool
	// DowntimeMs é a distância entre o instante em que o estado foi salvo e
	// esta observação, e só tem sentido com Restored. Sem ele, um disparo
	// sobre um período que ninguém acompanhou é indistinguível de um observado
	// de ponta a ponta.
	//
	// O que ele MEDE depende de como o processo saiu, e a diferença importa:
	//
	//   - encerramento gracioso: o Stop grava o estado, então o número é o
	//     tempo de parada, com precisão de milissegundos.
	//   - crash: sobra o último save, feito na última transição. O número
	//     passa a ser "desde quando não há registro", que é MAIOR que a
	//     parada — e é a resposta honesta, porque a lacuna real é
	//     desconhecida.
	//
	// Antes de o Stop gravar, os dois casos se comportavam como o segundo, e o
	// campo anunciava nove minutos de lacuna para uma parada de segundos — o
	// defeito que o piloto encontrou em 12/09/2026.
	DowntimeMs int64
	// Enrichment são os resultados das consultas declaradas, na ordem. Só o
	// disparo os carrega. Um bloco sem linhas traz o motivo, e nenhum deles
	// jamais impede o evento de sair.
	Enrichment []Enrichment
}

// Sink é para onde os eventos vão.
//
// Interface de um método, com uma implementação (o log) neste ciclo. É o ponto
// de extensão para tópico e webhook: eles entram como outro Sink, sobre o
// MESMO Event.
type Sink interface {
	Emit(Event)
}
