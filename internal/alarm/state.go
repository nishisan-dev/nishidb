// Package alarm observa condições declaradas sobre as linhas de um contador e
// emite um evento quando uma delas se sustenta por um prazo.
//
// Mora fora de internal/counters e de internal/query porque precisa dos dois —
// o schema da tabela do contador para compilar a condição, e o motor de
// consulta para enriquecer o evento — e nenhum dos dois pode importar o outro.
// É o mesmo argumento de internal/counters e internal/metakafka.
package alarm

import "time"

// observation é o que a varredura enxerga de UM grupo num instante.
//
// present e conditionHolds são fatos separados, e não um bool só: um grupo
// aposentado não é um grupo cuja condição deixou de valer. O primeiro resolve o
// alarme porque o assunto desapareceu, o segundo porque o problema passou, e
// quem recebe o evento precisa distinguir os dois.
type observation struct {
	// present diz se o grupo ainda está no universo do contador. Grupo
	// aposentado (refs == 0 por mais que retainEmptyGroups) sai com false.
	present bool
	// conditionHolds diz se o `when` do alarme vale para a linha do grupo.
	// Indefinido quando present é false.
	conditionHolds bool
	// qualified diz se o grupo é elegível ao alarme: o `qualifyWhen` vale, ou
	// o alarme não o declara. Indefinido quando conditionHolds é false — a
	// passada nem o avalia, porque condição falsa já decide o desfecho.
	//
	// É um terceiro fato, e não parte de conditionHolds, pela mesma razão que
	// separa present de conditionHolds: "o problema passou" e "o grupo deixou de
	// ser elegível" resolvem o alarme por motivos diferentes. A OLT morta há
	// mais de um dia continua com zero autenticados — a condição vale —, mas o
	// máximo de 24h caiu abaixo do limiar e ela deixou de ser o assunto do
	// alarme. Anunciar isso como `condition_cleared` diria que ela voltou.
	qualified bool
}

// decision é o que uma passada decide para um par (alarme, grupo).
//
// A marca de restaurado viaja COM o evento, e não fica no tracker: ela descreve
// uma sustentação medida de um instante lido do disco, e isso vale para o evento
// que saiu daquela medida — não para o alarme enquanto ele durar. Guardada no
// estado, uma resolução observada de ponta a ponta por este processo sairia
// dizendo que veio de antes do reinício.
type decision struct {
	verdict  verdict
	restored bool
	// since é o instante em que a condição passou a valer, e viaja AQUI porque
	// o clear apaga o campo do tracker antes de o evento ser montado. Lido de
	// lá depois da decisão, o evento de resolução saía com o instante zero —
	// afirmando que a condição nunca valeu.
	since time.Time
}

// verdict é o que uma passada decide para um par (alarme, grupo).
type verdict int

const (
	// verdictNone: nada a emitir. É o caso dominante.
	verdictNone verdict = iota
	// verdictFiring: a condição se sustentou pelo prazo. Emitir o disparo.
	verdictFiring
	// verdictCleared: estava disparado e a condição deixou de valer.
	verdictCleared
	// verdictRetired: estava disparado e o grupo saiu do contador.
	verdictRetired
	// verdictDisqualified: estava disparado, a condição ainda vale e o grupo
	// deixou de ser elegível (o `qualifyWhen` passou a ser falso).
	verdictDisqualified
)

// phase é a fase de um alarme sobre um grupo.
type phase uint8

const (
	// phaseAbsent: a condição não vale. Nada a lembrar.
	phaseAbsent phase = iota
	// phasePending: vale, e o prazo ainda não se cumpriu.
	phasePending
	// phaseFiring: o disparo já foi emitido e não se repete.
	phaseFiring
)

// tracker guarda o estado de UM alarme sobre UM grupo entre duas passadas.
//
// # Por que o estado durável guarda só o `since`, e não a fase
//
// Um processo que cai com o alarme JÁ DISPARADO volta como phasePending, não
// como phaseFiring, e por isso reemite o disparo marcado como restaurado. É
// deliberado: o processo novo não tem como saber se alguém viu o evento
// anterior, e um alarme ativo que não é reanunciado depois de um reinício
// deixaria o operador sem sinal até a condição resolver. Reemitir também é o
// que mantém o par disparo/resolução coerente para quem consome — uma
// resolução cujo disparo ninguém viu abrir é pior que um disparo repetido.
//
// A consequência é que o arquivo de estado carrega um campo só por alarme
// ativo, e os três casos de reinício (prazo vencido, prazo em curso, condição
// que já passou) caem do mesmo `since` sem fase nenhuma a desempatar.
type tracker struct {
	phase phase
	// since é o instante em que a condição passou a valer. Zero em phaseAbsent.
	since time.Time
	// restored marca que o since veio do estado durável e não foi observado
	// por este processo. É consumido pelo primeiro evento que sai.
	restored bool
	// seen é a passada em que a varredura viu este grupo. Mora no tracker, e
	// não num mapa paralelo, porque escrever num mapa com string(chave) ALOCA
	// — a otimização do compilador que evita a cópia vale para leitura e
	// delete, nunca para atribuição. Com a marca aqui, só quem já tem estado é
	// marcado, e o grupo saudável não custa nada.
	seen uint64

	// firedAt, firingValue e hasFiringValue descrevem a transição para
	// phaseFiring — zero/false em phaseAbsent e em phasePending, porque
	// nenhuma delas ainda aconteceu. São escritos SÓ nessa transição (ver
	// Runner.decide) por dois motivos: custo — reler o payload a cada passada
	// para todo grupo pending ou já disparado do inventário seria trabalho
	// proporcional ao tamanho do contador, não ao problema, mesmo a leitura
	// em si não alocando — e CONGELAMENTO — os dois campos descrevem O
	// disparo, e um grupo disparado cuja condição oscila (continua valendo,
	// mas o número muda) não pode anunciar, na próxima passada, um valor que
	// não foi o que decidiu.
	//
	// firingValue é o valor da coluna declarada em `value:` (padrão `total`),
	// lido do MESMO payload que decidiu o disparo — nunca uma releitura.
	// hasFiringValue diz se a coluna estava presente nele: uma métrica de
	// janela sem amostra ainda sai com false, nunca um zero que se pareceria
	// com um valor observado.
	//
	// Não fazem parte do estado durável (alarms.state v2 não muda): um
	// tracker resolvido é APAGADO do mapa (Runner.decide), nunca reaproveitado
	// — e um alarme restaurado do disco volta a phasePending e passa de novo
	// por decide ao completar o prazo, então firedAt e firingValue descrevem
	// SEMPRE o disparo deste processo, nunca o que o processo anterior viu.
	firedAt        time.Time
	firingValue    float64
	hasFiringValue bool
}

// restore instala o estado lido do arquivo durável.
func (t *tracker) restore(since time.Time) {
	t.phase = phasePending
	t.since = since
	t.restored = true
}

// active diz se há estado a guardar para este par. É o que decide o que vai
// para o arquivo durável.
func (t *tracker) active() bool { return t.phase != phaseAbsent }

// observedSince é o instante em que a condição passou a valer. Vai para o
// arquivo durável e para o campo "desde quando" do evento.
func (t *tracker) observedSince() time.Time { return t.since }

// evaluate avança a máquina com uma observação e devolve o que emitir.
//
// A ordem das guardas é a ordem das perguntas: o grupo ainda existe? a condição
// vale? o grupo é elegível? já se sustentou? Ausência, condição falsa e
// desqualificação são tratadas ANTES da fase porque as três zeram o cronômetro
// — e zerá-lo é o que faz a próxima queda precisar se sustentar outra vez, em
// vez de herdar o prazo já cumprido.
//
// A mesma ordem é a PRECEDÊNCIA dos motivos quando mais de um vale na mesma
// passada: `group_retired` > `condition_cleared` > `disqualified`. Grupo que
// sumiu não tem condição a avaliar; e condição que passou é o fato mais forte
// sobre um grupo ainda presente — dizer "desqualificado" de uma OLT que voltou
// esconderia a recuperação atrás de uma mudança de elegibilidade.
//
// Grupo desqualificado não entra em observação e, se estava contando o prazo,
// volta a phaseAbsent sem evento: nunca houve disparo a resolver. Disparado,
// resolve com verdictDisqualified — e só volta a disparar depois de
// requalificar E sustentar o prazo inteiro outra vez.
func (t *tracker) evaluate(o observation, sustainedFor time.Duration, now time.Time) decision {
	if !o.present {
		return t.clear(verdictRetired)
	}
	if !o.conditionHolds {
		return t.clear(verdictCleared)
	}
	if !o.qualified {
		return t.clear(verdictDisqualified)
	}
	switch t.phase {
	case phaseAbsent:
		// A condição acabou de passar a valer: começa a contar e não emite.
		// Disparar aqui seria alarmar por rajada, que é exatamente o que o
		// prazo existe para evitar.
		//
		// É esta atribuição — e não o zeramento do clear — que faz a queda
		// seguinte precisar se sustentar OUTRA VEZ: o cronômetro de um grupo
		// que já disparou uma vez recomeça do instante em que a condição
		// voltou a valer, nunca do prazo já cumprido.
		t.phase = phasePending
		t.since = now
		return decision{verdict: verdictNone}
	case phasePending:
		if now.Sub(t.since) < sustainedFor {
			return decision{verdict: verdictNone}
		}
		t.phase = phaseFiring
		return decision{verdict: verdictFiring, restored: t.consumeRestored(), since: t.since}
	default:
		// phaseFiring: não repete. O pedido é avisar uma vez quando a condição
		// se firma, e de novo quando ela passa.
		return decision{verdict: verdictNone}
	}
}

// clear apaga o estado e devolve resolved só se havia disparo a resolver.
//
// O zeramento do since é higiene, e não a barreira da recontagem — essa está na
// transição de phaseAbsent, que reatribui o instante. Ele existe para que um
// tracker inativo não carregue um instante obsoleto que alguma leitura futura
// do estado pudesse serializar ou reportar.
func (t *tracker) clear(resolved verdict) decision {
	wasFiring := t.phase == phaseFiring
	restored := t.consumeRestored()
	since := t.since
	t.phase = phaseAbsent
	t.since = time.Time{}
	if !wasFiring {
		// Nunca houve disparo: não há o que resolver. Anunciar uma resolução
		// aqui inventaria um par disparo/resolução que o consumidor nunca viu
		// abrir — e é o que aconteceria em toda rajada curta e em todo estado
		// restaurado cuja condição já tinha passado.
		return decision{verdict: verdictNone}
	}
	return decision{verdict: resolved, restored: restored, since: since}
}

// consumeRestored devolve a marca e a apaga, de uma vez.
func (t *tracker) consumeRestored() bool {
	was := t.restored
	t.restored = false
	return was
}
