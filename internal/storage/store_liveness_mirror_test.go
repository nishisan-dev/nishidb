package storage

// O espelho de liveness que o Stats publica (liveBytesAll) é mantido no
// CAMINHO QUENTE, dentro do applyRecord, e recarregado em valor ABSOLUTO na
// reconciliação do checkpoint. Essa recarga é o que torna qualquer teste que
// leia Stats DEPOIS de um checkpoint incapaz de dizer coisa alguma sobre o
// applyRecord: o checkpoint reescreve o número certo por cima do errado antes
// de a asserção olhar, e um erro na soma do Put sai verde. A revisão
// adversarial mediu o buraco — três variações do applyRecord sobreviviam à
// suíte INTEIRA. Medido nos cenários deste arquivo: remover a soma do record
// que nasce publica LiveBytesUpperBound = 18446744073709550136 no /v1/stats
// logo após o primeiro update, porque num uint64 subtrair a mais não dá
// negativo, dá 1,8e19. É exatamente a "métrica que mente" que o M4 existe
// para evitar, e a suíte não a via.
//
// Daí a regra deste arquivo: todo teste aqui lê o Stats SEM checkpoint entre
// as escritas e a leitura, e com oráculo EXATO em bytes — nunca um "> 0", que
// é o defeito do débito 6 (medir sem oráculo). O oráculo é a mesma conta que
// o encoding grava no segmento: RecordHeaderSize + len(key) + len(payload)
// por record.

import (
	"fmt"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
)

// bytesOfRecord é o oráculo. O tamanho que o applyRecord soma ao espelho é o
// mesmo que o BuildRecord grava no segmento, e essa conta é fixa (§10.1).
func bytesOfRecord(key string, payload []byte) uint64 {
	return uint64(encoding.RecordHeaderSize + len(key) + len(payload))
}

// somaOfTallies devolve Σ secTally.liveBytes sob o mutex do writer — o mapa
// é writer-only e ler mapa Go sem sincronizar é fatal error do runtime, não
// corrida benigna.
func somaOfTallies(s *Store) uint64 {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	var total uint64
	for _, t := range s.secTally {
		total += t.liveBytes
	}
	return total
}

// exigeEspelhoIgualAosTallies fixa a INVARIANTE da qual todo o resto depende:
// o número que sai pelo /v1/stats é cópia da soma dos tallies, e as duas não
// podem discordar em instante nenhum. É a asserção que separa "o espelho está
// com valor errado" de "o espelho furou por baixo": num uint64, subtrair
// demais não dá negativo, dá 1,8e19 — e o garbage_bytes, que o servidor
// calcula como storage - live também em uint64, sai desse mesmo tamanho.
func exigeEspelhoIgualAosTallies(t *testing.T, s *Store, moment string) {
	t.Helper()
	espelho := s.Stats().LiveBytesUpperBound
	if soma := somaOfTallies(s); espelho != soma {
		t.Fatalf("%s: o Stats publica LiveBytesUpperBound=%d, mas a soma dos tallies vale %d — "+
			"o espelho deixou de ser cópia dos mapas e a métrica passou a mentir", moment, espelho, soma)
	}
}

// TestEspelhoDeLivenessSomaCadaPutSemCheckpoint é o oráculo que faltava para
// o nascimento do record: depois de n Puts, e SEM checkpoint nenhum entre
// eles e a leitura, o LiveBytesUpperBound tem de valer exatamente a soma dos
// records escritos. Sem esta asserção, remover a soma do applyRecord deixa a
// suíte inteira verde — o primeiro checkpoint conserta o número antes de
// qualquer teste olhar, e até lá o /v1/stats publica o wrap do uint64.
func TestEspelhoDeLivenessSomaCadaPutSemCheckpoint(t *testing.T) {
	s := openStore(t, Options{})
	const n = 20
	var expected uint64
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("esp-%03d", i)
		p := statusPayload(t, s, k, "Start", int64(i))
		mustPut(t, s, k, int64(i), p, nil)
		expected += bytesOfRecord(k, p)
	}
	if got := s.Stats().LiveBytesUpperBound; got != expected {
		t.Fatalf("LiveBytesUpperBound = %d após %d Puts; o certo é a soma exata dos records, %d",
			got, n, expected)
	}
	exigeEspelhoIgualAosTallies(t, s, "após os Puts")
}

// TestUpdateSemCheckpointTiraDoEspelhoORecordAnterior cobre a outra metade do
// applyRecord: o update torna o record ANTERIOR garbage no instante do
// publish, e o espelho tem de perder esses bytes junto com o tally. Os
// payloads novos têm comprimento DIFERENTE dos antigos de propósito — com
// records do mesmo tamanho, descontar o record novo no lugar do antigo daria
// o mesmo total e o erro passaria despercebido.
func TestUpdateSemCheckpointTiraDoEspelhoORecordAnterior(t *testing.T) {
	s := openStore(t, Options{})
	const n = 20
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("upd-%03d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	// Reescrita das MESMAS n rows: o dataset continua com n rows vivas, e o
	// espelho tem de descrever só os records novos.
	var expected uint64
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("upd-%03d", i)
		prev := mustGetAud(t, s, k)
		p := statusPayload(t, s, k, "Interrupted", int64(i))
		mustPut(t, s, k, int64(n+i), p, &prev)
		expected += bytesOfRecord(k, p)
	}
	if got := s.Stats().LiveBytesUpperBound; got != expected {
		t.Fatalf("LiveBytesUpperBound = %d após %d updates; o certo é só a soma dos records NOVOS, %d — "+
			"a diferença é o record anterior que não saiu do agregado", got, n, expected)
	}
	exigeEspelhoIgualAosTallies(t, s, "após os updates")
}

// zeraTallyOfSegment monta, à mão e de forma COERENTE, a precondição que o
// clamp do applyRecord existe para tratar: o tally do segmento abaixo do
// record que a location ainda nomeia. Não dá para chegar nela pelo contrato
// público — o godoc do Put exige que o prev SEJA o record corrente da key, e
// com prev honesto o oldLen nunca passa do tally —, então o teste a constrói.
//
// "Coerente" é o ponto: tira do espelho exatamente o que tira do tally, para
// que a asserção do fim meça o applyRecord e não a montagem.
func zeraTallyOfSegment(t *testing.T, s *Store, sec uint32) {
	t.Helper()
	s.wmu.Lock()
	defer s.wmu.Unlock()
	tally := s.secTally[sec]
	if tally == nil {
		t.Fatalf("segmento %d não tem tally: a montagem do teste não descreve o estado real", sec)
	}
	removido := tally.liveBytes
	tally.liveBytes = 0
	s.liveBytesAll.Add(^(removido - 1))
}

// TestEspelhoDescontaABaixaClampadaENaoOOldLenCru fixa a decisão que o
// comentário do applyRecord justifica em prosa: o que sai do espelho é a
// BAIXA — min(oldLen, tally) —, nunca o oldLen cru. O oldLen é estimativa
// (débitos 38 e 45), e o clamp é o que impede o tally de furar por baixo;
// descontar o valor não-clampado do agregado o faria divergir da soma dos
// tallies num erro que só cresce, e é o agregado que o Stats publica.
//
// A montagem separa os dois segmentos de propósito: o record antigo fica num
// segmento cujo tally está zerado (baixa = 0, nada a descontar) e o record
// novo cai em outro. Assim o total esperado é positivo e específico, e não o
// zero degenerado que passaria em qualquer implementação.
func TestEspelhoDescontaABaixaClampadaENaoOOldLenCru(t *testing.T) {
	// Segmento minúsculo força rotação, que é o que põe o record antigo e o
	// novo em segmentos diferentes.
	s := openStore(t, Options{SegmentSize: 400})
	const n = 12
	bySegmento := make(map[uint32]uint64)
	var total uint64
	var target string
	var targetSeg uint32
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("clm-%03d", i)
		p := statusPayload(t, s, k, "Start", int64(i))
		mustPut(t, s, k, int64(i), p, nil)
		// O segmento em que o record caiu é o corrente DEPOIS do Put: a
		// rotação acontece dentro do Append, antes da escrita.
		sec := s.Stats().CurrentSegmentID
		bySegmento[sec] += bytesOfRecord(k, p)
		total += bytesOfRecord(k, p)
		if i == 0 {
			target, targetSeg = k, sec
		}
	}
	if len(bySegmento) < 2 {
		t.Fatalf("o teste precisa de mais de um segmento; obtido %d", len(bySegmento))
	}
	zeraTallyOfSegment(t, s, targetSeg)

	prev := mustGetAud(t, s, target)
	nw := statusPayload(t, s, target, "Stop", 999)
	mustPut(t, s, target, 999, nw, &prev)
	if sec := s.Stats().CurrentSegmentID; sec == targetSeg {
		t.Fatalf("o record novo caiu no segmento %d do record antigo: a montagem não exercita o clamp", sec)
	}

	// A baixa vale min(oldLen, 0) = 0, então nada sai do espelho e o record
	// novo entra inteiro. Descontar o oldLen cru daria este total MENOS o
	// tamanho do record antigo — e, quando o tally de um segmento pequeno é
	// o único que resta, o wrap do uint64.
	expected := total - bySegmento[targetSeg] + bytesOfRecord(target, nw)
	if got := s.Stats().LiveBytesUpperBound; got != expected {
		t.Fatalf("LiveBytesUpperBound = %d após o update sobre tally zerado; o certo é %d — "+
			"a diferença é o oldLen cru descontado no lugar da baixa clampada", got, expected)
	}
	exigeEspelhoIgualAosTallies(t, s, "após o update com tally zerado")
}

// TestRecoverySemeiaOEspelhoQueOStatsPublica fecha a janela que sustentava o
// "ninguém observa o intervalo": a recovery semeia o secTally com os
// liveBytes do manifest e, se não semear também o espelho, o replay do tail
// desconta uma baixa CLAMPADA contra o tally semeado de um agregado que vale
// zero. Isso é underflow de uint64 no código de PRODUÇÃO — medido no fim do
// recoverFrom: espelho em 18446744073709551544 com os tallies somando 568.
//
// O Checkpoint incondicional do Open recarrega o absoluto e apaga o rastro,
// então nenhuma leitura de Stats pode distinguir as duas implementações
// depois que o Open retorna. Quem distingue é a INVARIANTE: no instante em
// que o checkpoint encontra o estado, os espelhos já têm de descrever os
// mapas. Se a recarga precisou CONSERTAR alguma coisa, ela deixou de ser
// reconciliação e virou remendo — e é isso que o contador afere.
//
// A montagem é a de um crash real: checkpoint, updates com payload MENOR que
// o anterior (é a diferença de tamanho que faz o agregado furar por baixo) e
// Close sem checkpoint. O overlay de locations morre com o processo, o mmap
// continua nomeando os records do ponto S e, por isso, o replay encontra a
// location ESTRITAMENTE atrás do cursor — que é a condição para ele
// decrementar em vez de deixar o resíduo do débito 38.
func TestRecoverySemeiaOEspelhoQueOStatsPublica(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir}
	s := audOpen(t, opts)
	const n = 8
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("rec-%03d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Interrupted", int64(i)), nil)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	var expected uint64
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("rec-%03d", i)
		prev := mustGetAud(t, s, k)
		p := statusPayload(t, s, k, "Ok", int64(i))
		mustPut(t, s, k, int64(n+i), p, &prev)
		expected += bytesOfRecord(k, p)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2 := audOpen(t, opts)
	defer func() { _ = s2.Close() }()
	if d := s2.EspelhosDivergentesForTest(); d != 0 {
		t.Fatalf("a recovery deixou os espelhos fora dos mapas em %d checkpoint(s): "+
			"o replay do tail descontou a baixa de um agregado sem a base do manifest, "+
			"e o que segurava o /v1/stats era só a recarga do Checkpoint do Open", d)
	}
	if got := s2.Stats().LiveBytesUpperBound; got != expected {
		t.Fatalf("LiveBytesUpperBound = %d após a recovery; o certo é a soma dos records do tail, %d", got, expected)
	}
	exigeEspelhoIgualAosTallies(t, s2, "após a recovery")
}

// TestConferenciaDeEspelhosAcusaCadaEspelhoDivergente prova que o detector
// DISPARA, e prova para os TRÊS espelhos, um subteste por espelho. Sem isto
// ele é decoração: a revisão adversarial da Task 1b derrubou três mutantes
// com a suíte inteira verde — remover a chamada de conferaEspelhos do
// checkpoint, cegar a guarda para o termo do disco, e mover a conferência
// para DEPOIS da recarga dos espelhos.
//
// O último é o que dá sentido aos outros dois: a conferência só acha alguma
// coisa porque roda ANTES de a reconciliação tocar nos espelhos. Depois da
// recarga ela compara o valor recém-consertado consigo mesmo e nunca acusa
// nada — e é exatamente por isso que a asserção é sobre o CONTADOR de
// divergências, não sobre o valor que o Stats publica no fim: o valor está
// certo nas duas implementações.
func TestConferenciaDeEspelhosAcusaCadaEspelhoDivergente(t *testing.T) {
	for _, c := range []struct {
		name    string
		corrupt func(*Store)
	}{
		{"secCount", func(s *Store) { s.secCount.Add(1) }},
		{"liveBytesAll", func(s *Store) { s.liveBytesAll.Add(4096) }},
		{"diskBytesAll", func(s *Store) { s.diskBytesAll.Add(4096) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := openStore(t, Options{})
			const n = 20
			for i := 0; i < n; i++ {
				k := fmt.Sprintf("cnf-%03d", i)
				mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
			}
			// Checkpoint de base: ele recarrega os três espelhos a partir dos
			// mapas, então o que vier depois parte de espelho e mapa em
			// acordo — e a única divergência possível é a que o teste planta.
			if err := s.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint de base: %v", err)
			}
			before := s.EspelhosDivergentesForTest()
			if before != 0 {
				t.Fatalf("o dataset já nasceu com %d divergência(s): a montagem não isola o que o teste planta", before)
			}
			s.CorruptEspelhoForTest(c.corrupt)
			if err := s.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}
			if got := s.EspelhosDivergentesForTest(); got != before+1 {
				t.Errorf("divergências = %d, queria %d: o checkpoint não acusou o espelho %s fora do mapa, "+
					"e um detector que ninguém prova disparando lê-se como proteção sem proteger", got, before+1, c.name)
			}
		})
	}
}

// TestEspelhosSaemDaRecoveryJaSemeados lê os espelhos ao FIM do recoverFrom,
// antes do Checkpoint incondicional que o Open dispara. É esse ponto de
// leitura que faz o defeito aparecer, e é ele que faltava: a recarga do
// checkpoint reescreve os três em valor absoluto, e depois dela nenhuma
// leitura de Stats distingue uma recovery que semeou os espelhos de uma que
// os deixou furados. Foi assim que o underflow de uint64 medido no fim do
// recoverFrom — liveBytesAll em 18446744073709551544 contra 568 nos tallies —
// sobreviveu à suíte inteira no mutante composto da revisão da Task 1b.
//
// O oráculo é a INVARIANTE (espelho é cópia do mapa), não uma faixa. Comparar
// o vivo contra o espelho de DISCO não serviria: neste instante o disco vale
// zero por decisão de produção documentada no próprio recoverFrom — o secSize
// não é semeado ali, porque o Checkpoint do Open o repovoa pelo ListSegments
// — e uma faixa (0, disco] reprovaria o código correto.
func TestEspelhosSaemDaRecoveryJaSemeados(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir}
	s := audOpen(t, opts)
	const n = 8
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("sem-%03d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Interrupted", int64(i)), nil)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	// O tail que o replay vai reaplicar. Payloads MENORES que os anteriores
	// de propósito: é a diferença de tamanho que faz o agregado furar por
	// baixo quando ele não parte da mesma base do tally.
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("sem-%03d", i)
		prev := mustGetAud(t, s, k)
		mustPut(t, s, k, int64(n+i), statusPayload(t, s, k, "Ok", int64(i)), &prev)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var (
		chamou            bool
		espSegs, mapaSegs uint32
		espVivo, mapaVivo uint64
		espDisk, mapaDisk int64
	)
	reabre := opts
	reabre.traceRecovery = func(s *Store) {
		chamou = true
		espSegs, espVivo, espDisk = s.secCount.Load(), s.liveBytesAll.Load(), s.diskBytesAll.Load()
		// Os mapas são writer-only e ler mapa Go sem sincronizar é fatal
		// error do runtime. Aqui não há writer concorrente — o Store nem saiu
		// do Open —, mas o seam respeita o contrato em vez de depender disso.
		s.wmu.Lock()
		mapaSegs, mapaVivo, mapaDisk = s.agregadosOfTally()
		s.wmu.Unlock()
	}
	s2 := audOpen(t, reabre)
	defer func() { _ = s2.Close() }()

	if !chamou {
		t.Fatal("o trace do fim da recovery não rodou: este Open não passou pelo recoverFrom e o teste não prova nada")
	}
	if mapaVivo == 0 || mapaSegs == 0 {
		t.Fatalf("os mapas saíram da recovery em %d segmento(s) e %d bytes vivos: sem estado não há o que conferir",
			mapaSegs, mapaVivo)
	}
	if espVivo != mapaVivo {
		t.Errorf("liveBytesAll = %d ao fim da recovery e os tallies somam %d: o espelho não partiu da base do manifest, "+
			"e a primeira baixa do replay o furou por baixo — num uint64 isso não dá negativo, dá 1,8e19 no /v1/stats",
			espVivo, mapaVivo)
	}
	if espSegs != mapaSegs {
		t.Errorf("secCount = %d ao fim da recovery e o tally tem %d segmento(s): o espelho não foi semeado", espSegs, mapaSegs)
	}
	if espDisk != mapaDisk {
		t.Errorf("diskBytesAll = %d ao fim da recovery e o secSize soma %d: os dois têm de sair daqui iguais, "+
			"cada espelho é cópia fiel do SEU mapa", espDisk, mapaDisk)
	}
}
