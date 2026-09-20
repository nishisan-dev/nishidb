package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/storage/rowindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// diretorioOfCounters é o subdiretório de indexes/ onde cada contador tem
// o seu, pelo nome declarado.
const diretorioOfCounters = "contadores"

// pendenciaOfCounter é um contador que a abertura decidiu RECONSTRUIR: a
// varredura roda depois do ramo de recovery, e o motivo vai para o log.
type pendenciaOfCounter struct {
	c      *counter.Counter
	motivo string
}

// openCounters compila as definições pelo callback, abre os arquivos de cada
// contador e decide, contador a contador, entre adotar e reconstruir.
//
// Roda DEPOIS de _writtenAt, _expiresIn e os campos indexados estarem no
// dicionário — a ordem de LookupOrCreate é contrato — e ANTES do ramo de
// recovery, porque o replay do tail passa pelo applyRecord e o applyRecord
// transiciona os contadores: eles têm de existir, dobrados, quando a primeira
// linha do tail for reaplicada.
//
// # Adotar ou reconstruir
//
// A decisão vem do manifest, que grava por contador os parâmetros que decidem
// o conteúdo do vetor (nome, colunas, where). Divergiu, ou não estava lá, ou o
// vetor não descreve as linhas do dataset: reconstrói — recriando os DOIS
// arquivos, porque chaves de outras colunas não podem poluir um dicionário
// cujos ids nunca são reemitidos. Mudar o teto não reconstrói: ele não muda o
// que já foi contado, e só é recusado quando menor que os grupos em uso.
//
// Um contador que o manifest declara e o YAML já não tem é APAGADO, com log:
// deixar os arquivos seria disco ocupado que nada no produto explica.
//
// # A recusa sem callback
//
// Um dataset cujo manifest declara contadores e cuja abertura não forneceu o
// callback é RECUSADO. Abrir sem as definições deixaria o vetor parado
// enquanto o Put anda, e a divergência só apareceria na auditoria — é a guarda
// que pega o chamador de storage.Open que esqueceu de ligar os contadores.
func (s *Store) openCounters(opts Options, m *checkpoint.Manifest, idxDir string, minRows uint64) error {
	var declarados []checkpoint.CounterInfo
	if m != nil {
		declarados = m.Counters
	}
	if opts.CompileCountersCallbackFunction == nil {
		if len(declarados) > 0 {
			nomes := make([]string, 0, len(declarados))
			for _, d := range declarados {
				nomes = append(nomes, d.Name)
			}
			return fmt.Errorf("%w: o dataset %s declara os contadores %v no manifest e esta abertura não forneceu CompilaContadores; "+
				"abrir sem as definições deixaria o contador parado enquanto o Put anda",
				ErrInvalidArgument, opts.Dir, nomes)
		}
		return nil
	}
	defs, err := opts.CompileCountersCallbackFunction(s.dict)
	if err != nil {
		return fmt.Errorf("storage: compilar os contadores de %s: %w", opts.Dir, err)
	}
	if len(defs) == 0 && len(declarados) == 0 {
		return nil
	}
	if len(defs) > 0 && opts.Mode == HistoryMode {
		// A retenção do histórico purga linhas por marca-d'água, sem passar
		// pelo applyRecord: o contador nunca decrementaria. E não há estado
		// corrente a contar num log append-only.
		return fmt.Errorf("%w: ModoHistorico não aceita counters: a tabela histórica não tem estado corrente a contar", ErrInvalidArgument)
	}
	dir := filepath.Join(idxDir, diretorioOfCounters)
	byNome := make(map[string]checkpoint.CounterInfo, len(declarados))
	for _, info := range declarados {
		byNome[info.Name] = info
	}
	vistos := make(map[string]struct{}, len(defs))
	for _, def := range defs {
		if _, dup := vistos[def.Name]; dup {
			return fmt.Errorf("%w: contador %q declarado duas vezes", ErrInvalidArgument, def.Name)
		}
		vistos[def.Name] = struct{}{}
		info, tinha := byNome[def.Name]
		delete(byNome, def.Name)

		recria, motivo := false, ""
		switch {
		case m == nil:
			// Dataset novo: não há o que reconstruir, o vetor nasce vazio.
		case !tinha:
			recria, motivo = true, "o dataset não tinha este contador"
		case !sameDefinition(info, def):
			recria, motivo = true, fmt.Sprintf("os parâmetros mudaram (groupBy %v → %v, where %q → %q)",
				info.Columns, columnsOf(def), info.Where, def.Where)
		}
		c, err := counter.Open(filepath.Join(dir, def.Name), def, minRows, recria)
		if err != nil {
			return fmt.Errorf("storage: abrir o contador %q de %s: %w", def.Name, opts.Dir, err)
		}
		s.counters = append(s.counters, c)
		var measureEpoch uint64
		var persistedMeasures bool
		if m != nil {
			measureEpoch = m.Epoch
			persistedMeasures = slices.Contains(m.MeasureCounters, def.Name)
		}
		if err := c.OpenMeasures(measureEpoch, persistedMeasures, recria); err != nil {
			if errors.Is(err, counter.ErrMeasureSnapshotCorrupt) {
				return fmt.Errorf("%w: %w", ErrDatasetCorrupt, err)
			}
			return err
		}
		for _, measure := range c.MeasureResetNames() {
			s.opts.Log.Info("counter_measure.reset", "measure starts a new window without backfill", "counter", def.Name, "measure", measure)
		}
		if !recria && m != nil && ((c.Born() && m.NextRowID > 0) || !c.Covers(types.RowID(m.NextRowID))) {
			// O manifest declara os parâmetros, não a existência dos arquivos —
			// a mesma pergunta que a agenda faz com NeedsRebuild.
			recria, motivo = true, "os arquivos do contador não descrevem as linhas do dataset"
			if err := c.Clear(); err != nil {
				return err
			}
		}
		if est := c.Stats(); est.Groups > def.MaxGroups {
			return fmt.Errorf("%w: contador %q tem %d grupos em uso e maxGroups %d; o teto só pode diminuir até os grupos em uso (ou reconstrua o contador)",
				ErrInvalidArgument, def.Name, est.Groups, def.MaxGroups)
		}
		name := def.Name
		c.OnRefuse(func() {
			s.opts.Log.Warn("counter.teto",
				"um grupo novo foi recusado pelo teto de maxGroups; a linha entrou, mas não é contada até o contador ser reconstruído com teto maior",
				"contador", name, "maxGroups", def.MaxGroups)
		})
		if recria {
			s.countersARecriar = append(s.countersARecriar, pendenciaOfCounter{c: c, motivo: motivo})
			continue
		}
		if err := c.Fold(); err != nil {
			return fmt.Errorf("storage: dobrar o contador %q de %s: %w", def.Name, opts.Dir, err)
		}
		if orfas := c.Stats().Orphans; orfas > 0 {
			s.opts.Log.Warn("counter.orfas",
				"entradas do vetor apontavam para grupos aposentados ou inexistentes e foram zeradas; não é esperado fora de uma janela de crash",
				"contador", name, "orfas", orfas)
		}
	}
	// Sobras do manifest: contadores que o YAML já não declara.
	sobras := make([]string, 0, len(byNome))
	for name := range byNome {
		sobras = append(sobras, name)
	}
	slices.Sort(sobras)
	for _, name := range sobras {
		if err := os.RemoveAll(filepath.Join(dir, name)); err != nil {
			return fmt.Errorf("storage: apagar o contador %q removido da configuração: %w", name, err)
		}
		s.opts.Log.Info("counter.removido",
			"o contador saiu da configuração do dataset e os arquivos dele foram apagados",
			"contador", name)
	}
	return nil
}

// sameDefinition compara os parâmetros que decidem o conteúdo: nome, colunas
// (na ordem) e where.
//
// O molde do groupKey NÃO entra, pelo mesmo critério do maxGroups: ele decide
// como o grupo é ESCRITO, não o que foi contado. O texto é derivado da chave em
// toda abertura, então trocá-lo re-renderiza e não custa uma varredura. Quem
// avisa que ele mudou é o log de subida, que sai com o molde efetivo.
func sameDefinition(info checkpoint.CounterInfo, def counter.Definition) bool {
	return info.Name == def.Name && info.Where == def.Where && slices.Equal(info.Columns, columnsOf(def))
}

func columnsOf(def counter.Definition) []uint16 {
	out := make([]uint16, len(def.Columns))
	for i, c := range def.Columns {
		out[i] = uint16(c)
	}
	return out
}

// infoOfCounters é a seção do manifest, na ordem em que os contadores
// foram declarados.
func (s *Store) infoOfCounters() []checkpoint.CounterInfo {
	if len(s.counters) == 0 {
		return nil
	}
	out := make([]checkpoint.CounterInfo, 0, len(s.counters))
	for _, c := range s.counters {
		def := c.Def()
		out = append(out, checkpoint.CounterInfo{Name: def.Name, Columns: columnsOf(def), Where: def.Where})
	}
	return out
}

// reconstroiCountersPendentes varre as linhas vivas para cada contador que
// a abertura decidiu reconstruir. Roda DEPOIS do ramo de recovery: as
// locations já estão publicadas, e o que o replay já transicionou no contador
// recém-nascido é idempotente com a varredura (o delta é contra a entrada
// corrente do vetor).
func (s *Store) reconstroiCountersPendentes() error {
	for _, p := range s.countersARecriar {
		start := time.Now()
		rows, err := s.varreRowsVivasInCounter(context.Background(), p.c, false)
		if err != nil {
			return fmt.Errorf("storage: construir o contador %q de %s: %w", p.c.Def().Name, s.opts.Dir, err)
		}
		if err := p.c.RemoveOld(); err != nil {
			return err
		}
		s.countersConstruidos++
		// O log nomeia o custo pela mesma razão do da agenda: é a única coisa
		// que o operador precisa decidir — segundos numa subida são aceitáveis,
		// minutos num dataset grande mudam a janela de manutenção.
		s.opts.Log.Info("counter.construido",
			"o contador foi construído varrendo as linhas vivas do dataset",
			"contador", p.c.Def().Name, "motivo", p.motivo, "linhas", rows,
			"grupos", p.c.Stats().Groups, "duracao", time.Since(start).String())
	}
	s.countersARecriar = nil
	return nil
}

// varreRowsVivasInCounter transiciona, no contador, toda linha com
// location e sem lápide, lendo o record de cada uma. sobAposMu diz se a
// leitura precisa da proteção contra a aposentadoria de segmento — na
// abertura não há concorrência; em operação (ReconstroiCounters) o writer
// já segura o wmu e a leitura do par (location, bytes) entra no aposMu, que é
// o lock mais interno.
func (s *Store) varreRowsVivasInCounter(ctx context.Context, c *counter.Counter, sobAposMu bool) (uint64, error) {
	limit := types.RowID(s.nextRowID.Load())
	var buf []byte
	var rows uint64
	for id := types.RowID(0); id < limit; id++ {
		if id&passoOfContexto == 0 {
			if err := ctx.Err(); err != nil {
				return rows, err
			}
		}
		packed, ok := s.locs.Get(id)
		if !ok {
			continue
		}
		sec, off, flags := rowindex.Unpack(packed)
		if flags&types.FlagTombstone != 0 {
			continue
		}
		if sobAposMu {
			s.aposMu.RLock()
		}
		rec, b, err := s.secR.ReadRecord(sec, off, buf)
		if sobAposMu {
			s.aposMu.RUnlock()
		}
		buf = b
		if err != nil {
			return rows, fmt.Errorf("ler a linha %d em (%d, %d): %w", id, sec, off, err)
		}
		if rec.RowID != id {
			return rows, fmt.Errorf("%w: a location da linha %d aponta para o record da linha %d", ErrDatasetCorrupt, id, rec.RowID)
		}
		if err := c.Transition(id, rec.Payload, false); err != nil {
			return rows, err
		}
		rows++
	}
	return rows, nil
}

// Counters devolve os contadores abertos, na ordem declarada. A slice é do
// Store: quem chama não a altera.
func (s *Store) Counters() []*counter.Counter { return s.counters }

// EstatisticasOfCounters é o retrato de todos os contadores, para as
// métricas e o /v1/stats — a MESMA chamada nos dois.
func (s *Store) EstatisticasOfCounters() []counter.Stats {
	out := make([]counter.Stats, 0, len(s.counters))
	for _, c := range s.counters {
		out = append(out, c.Stats())
	}
	return out
}

// passoOfContexto é a cadência (em linhas, potência de dois menos um usada
// como máscara) com que as varreduras longas olham o contexto e a validade do
// snapshot. Mil linhas são microssegundos; olhar por linha custaria mais que
// a própria leitura.
const passoOfContexto = 1023

// recalculoOfCounter é o que a varredura da auditoria acumula por contador.
type recalculoOfCounter struct {
	totais, refs  []int64
	naoInternadas uint64
	deriv         *counter.Deriver
}

// AuditaCounters recalcula cada contador por varredura e compara com o
// mantido, SEM segurar o writer pela varredura.
//
// # A definição
//
// "Viva" para o contador é a linha COM location e SEM lápide — o vetor
// representa exatamente isso, e o applyRecord só desconta quando a lápide é
// gravada. A visibilidade da consulta é outra coisa: ela filtra também por
// prazo vencido, decidido em tempo de leitura, e uma linha vencida que o
// coletor ainda não lapidou está invisível à consulta e CONTADA no contador
// (a proposta já assume isso: "o total cai quando o coletor chega"). Por isso
// a auditoria não usa ScanLive/GetByRowID: ela tem enumerador próprio, com a
// definição do contador, senão reportaria como divergência o que é desenho.
//
// # O instante
//
// Sob o wmu, por microssegundos: abre o snapshot e copia os totais de cada
// contador. Os dois no mesmo instante, com o writer parado, é o que torna a
// comparação exata — o snapshot dá a versão T0 de cada linha (o journal de
// versões), e a cópia dá os totais T0. Abrir o snapshot DENTRO do wmu também
// fecha a corrida entre ler a época e registrar o leitor (snapshot.go). Depois
// disso o writer segue; a varredura lê sob o aposMu por linha, como toda
// leitura do par (location, bytes).
//
// # Inconclusiva
//
// O journal tem teto (runtime.query.snapshotMaxBytes), e a varredura de um
// dataset grande segura o snapshot por segundos. Estourado, o snapshot mais
// antigo é invalidado — pode ser este — e a resposta é ErrExpiredSnapshot,
// nunca um relatório: uma divergência inventada por versão perdida mandaria o
// operador reconstruir um contador certo.
func (s *Store) AuditaCounters(ctx context.Context) ([]counter.Report, error) {
	if err := s.entra(); err != nil {
		return nil, err
	}
	defer s.sai()
	if len(s.counters) == 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	start := time.Now()

	s.wmu.Lock()
	snap := s.OpenSnapshot()
	if s.audBeforeOfCopy != nil {
		s.audBeforeOfCopy()
	}
	congelados := make([]counter.AuditSnapshot, len(s.counters))
	for i, c := range s.counters {
		congelados[i] = c.AuditSnapshot()
	}
	s.wmu.Unlock()
	defer s.CloseSnapshot(snap)

	recalc := make([]recalculoOfCounter, len(s.counters))
	for i, c := range s.counters {
		recalc[i] = recalculoOfCounter{
			totais: make([]int64, congelados[i].Limit),
			refs:   make([]int64, congelados[i].Limit),
			deriv:  c.NewDeriver(),
		}
	}

	var buf []byte
	var rows uint64
	for id := snap.BaseRowID; id < snap.LimitRowID; id++ {
		if id&passoOfContexto == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		// A validade vem ANTES de consultar o journal: um snapshot invalidado
		// já pode ter tido as versões dele podadas, e o journal responderia
		// "location corrente" para uma linha que mudou depois de T0.
		if !s.snaps.valid(snap.Epoch) {
			return nil, ErrExpiredSnapshot
		}
		var packed uint64
		var ok bool
		if !s.vers.IsEmpty() {
			packed, ok = s.vers.At(id, snap.Epoch)
		}
		if !ok {
			if packed, ok = s.locs.Get(id); !ok {
				continue
			}
		}
		sec, off, flags := rowindex.Unpack(packed)
		if flags&types.FlagTombstone != 0 {
			continue
		}
		s.aposMu.RLock()
		rec, b, err := s.secR.ReadRecord(sec, off, buf)
		s.aposMu.RUnlock()
		buf = b
		if err != nil {
			return nil, fmt.Errorf("storage: auditoria dos contadores: ler a linha %d em (%d, %d): %w", id, sec, off, err)
		}
		if rec.RowID != id {
			return nil, fmt.Errorf("%w: a location da linha %d aponta para o record da linha %d", ErrDatasetCorrupt, id, rec.RowID)
		}
		rows++
		for i, c := range s.counters {
			key, cond, has, err := recalc[i].deriv.Derive(rec.Payload)
			if err != nil {
				return nil, fmt.Errorf("storage: auditoria do contador %q na linha %d: %w", c.Def().Name, id, err)
			}
			if !has {
				continue
			}
			g, ok := c.ByKey(key)
			if !ok || g >= congelados[i].Limit {
				// Grupo internado DEPOIS de T0 (a linha era de outro grupo em
				// T0 e o journal já devolveu a versão T0, então isto só
				// acontece por deriva) ou nunca internado.
				recalc[i].naoInternadas++
				continue
			}
			recalc[i].refs[g]++
			if cond && !c.Def().DisableTotal {
				recalc[i].totais[g]++
			}
		}
		if s.audTrace != nil {
			s.audTrace(id)
		}
	}
	if !s.snaps.valid(snap.Epoch) {
		return nil, ErrExpiredSnapshot
	}

	duration := time.Since(start)
	out := make([]counter.Report, 0, len(s.counters))
	for i, c := range s.counters {
		cg, rc := congelados[i], recalc[i]
		r := counter.Report{
			TotalDisabled: c.Def().DisableTotal,
			Name:          c.Def().Name,
			Groups:        c.Stats().Groups,
			NotInterned:   rc.naoInternadas,
			RowsScanned:   rows,
			Duration:      duration,
		}
		for g := uint32(1); g < cg.Limit; g++ {
			m, mr := cg.Totals[g], cg.Refs[g]
			q, qr := rc.totais[g], rc.refs[g]
			r.Maintained += m
			r.Recomputed += q
			r.RowsMaintained += mr
			r.RowsRecomputed += qr
			if m == q && mr == qr {
				continue
			}
			r.DivergentGroups++
			if len(r.Sample) < counter.SampleCeiling {
				_, text, _, _, _ := c.Group(g)
				r.Sample = append(r.Sample, counter.Divergence{
					ID: g, Text: string(text),
					Maintained: m, Recomputed: q,
					RefsMaintained: mr, RefsRecomputed: qr,
				})
			}
		}
		// Os agregados do contador são a SEGUNDA fonte para o mesmo fato que
		// os vetores por grupo, e esta é a única coisa que os reconcilia. A
		// comparação é contra o RECALCULADO, não contra a soma mantida: é a
		// realidade que a auditoria mede, e comparar as duas fontes mantidas
		// entre si deixaria passar a deriva que atinge as duas.
		//
		// Contados à parte de Divergentes porque a pergunta é outra: uma linha
		// que trocou de grupo por deriva deixa os grupos errados e o agregado
		// CERTO, e somar as duas contagens esconderia essa distinção.
		r.AggregateRows, r.AggregateTotal = cg.Rows, cg.Total
		if r.AggregateRows != r.RowsRecomputed {
			r.DivergentAggregates++
		}
		if r.AggregateTotal != r.Recomputed {
			r.DivergentAggregates++
		}
		c.RecordAudit(r.DivergentGroups+int(r.NotInterned)+r.DivergentAggregates, s.now())
		out = append(out, r)
	}
	return out, nil
}

// ReconstroiCounters zera cada contador e o refaz varrendo as linhas vivas,
// SOB o wmu: a ingestão para pela varredura inteira. É o remédio para uma
// deriva que a auditoria apontou — explícito, caro e raro —, e o custo é
// dito no godoc e na documentação em vez de escondido atrás de um caminho
// "sem lock" que deixaria o contador inconsistente enquanto o Put anda.
func (s *Store) ReconstroiCounters(ctx context.Context) ([]counter.Rebuild, error) {
	if err := s.entra(); err != nil {
		return nil, err
	}
	defer s.sai()
	if len(s.counters) == 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.stickyErr != nil {
		return nil, fmt.Errorf("%w: %w", ErrWriterFailed, s.stickyErr)
	}
	out := make([]counter.Rebuild, 0, len(s.counters))
	for _, c := range s.counters {
		start := time.Now()
		if err := c.Clear(); err != nil {
			return nil, fmt.Errorf("storage: reconstruir o contador %q: %w", c.Def().Name, err)
		}
		rows, err := s.varreRowsVivasInCounter(ctx, c, true)
		if err != nil {
			return nil, fmt.Errorf("storage: reconstruir o contador %q: %w", c.Def().Name, err)
		}
		out = append(out, counter.Rebuild{
			Name: c.Def().Name, Rows: rows, Groups: c.Stats().Groups, Duration: time.Since(start),
		})
		s.opts.Log.Info("counter.reconstruido",
			"o contador foi reconstruído em operação, varrendo as linhas vivas sob o lock do writer",
			"contador", c.Def().Name, "linhas", rows, "grupos", c.Stats().Groups, "duracao", time.Since(start).String())
	}
	return out, nil
}

// CadenciaOfRetirement é o intervalo entre passadas do laço de
// aposentadoria de grupos. Um minuto, como o teto de espera do coletor de
// expiração: o prazo de visibilidade é de dias, e a passada é uma varredura
// linear de milhares de grupos — não de milhões de linhas. Exportada para o
// log de subida do servidor nomear a cadência.
const CadenciaOfRetirement = time.Minute

// RetirementOfGroupsActive reporta se o laço está de pé.
func (s *Store) RetirementOfGroupsActive() bool { return s.apGrupActive.Load() }

// hasCounterWithRetention diz se algum contador declara retainEmptyGroups.
func (s *Store) hasCounterWithRetention() bool {
	for _, c := range s.counters {
		if c.Def().RetainEmptyGroups > 0 {
			return true
		}
	}
	return false
}

// AposentaGroupsVazios roda UMA passada de aposentadoria em todos os
// contadores, sob o wmu. Devolve quantos grupos saíram.
func (s *Store) AposentaGroupsVazios() (int, error) {
	if err := s.entra(); err != nil {
		return 0, err
	}
	defer s.sai()
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.stickyErr != nil {
		return 0, fmt.Errorf("%w: %w", ErrWriterFailed, s.stickyErr)
	}
	total := 0
	for _, c := range s.counters {
		n, err := c.RetireEmptyGroups(s.now())
		total += n
		if err != nil {
			return total, fmt.Errorf("storage: aposentar grupos do contador %q: %w", c.Def().Name, err)
		}
	}
	return total, nil
}

// StartRetirementOfGroups sobe o laço que aposenta grupos vazios. Sem
// contador com retainEmptyGroups não há goroutine: o dataset não paga uma
// thread para não fazer nada.
func (s *Store) StartRetirementOfGroups(ctx context.Context) {
	if !s.hasCounterWithRetention() {
		return
	}
	s.apGrupMu.Lock()
	defer s.apGrupMu.Unlock()
	if s.apGrupCancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	s.apGrupCancel = cancel
	s.apGrupActive.Store(true)
	s.apGrupInVoo.Add(1)
	go func() {
		defer s.apGrupInVoo.Done()
		defer s.apGrupActive.Store(false)
		s.runRetirement(ctx)
	}()
}

// ForRetirementOfGroups cancela e AGUARDA a passada em voo, pela mesma
// razão do ForExpiration: a passada escreve no dicionário do contador, que o
// Close fecha.
func (s *Store) ForRetirementOfGroups() {
	s.apGrupMu.Lock()
	defer s.apGrupMu.Unlock()
	if s.apGrupCancel != nil {
		s.apGrupCancel()
		s.apGrupCancel = nil
	}
	s.apGrupInVoo.Wait()
}

func (s *Store) runRetirement(ctx context.Context) {
	t := time.NewTicker(CadenciaOfRetirement)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n, err := s.AposentaGroupsVazios()
		if err != nil {
			// Erro aqui é do writer (sticky) ou do store fechando; insistir
			// só produz log em laço.
			s.opts.Log.Warn("counter.aposentadoria_parou",
				"o laço de aposentadoria de grupos vazios encerrou; grupos vazios deixam de sair da tabela",
				"erro", err)
			return
		}
		if n > 0 {
			s.opts.Log.Info("counter.aposentadoria",
				"grupos vazios há mais que retainEmptyGroups saíram das tabelas de contadores",
				"aposentados", n)
		}
	}
}

// describeCounters rende as definições numa linha, para o log de subida.
func describeCounters(cs []*counter.Counter) string {
	var sb strings.Builder
	for i, c := range cs {
		if i > 0 {
			sb.WriteString("; ")
		}
		def := c.Def()
		fmt.Fprintf(&sb, "%s groupBy=%v keyFormat=%q", def.Name, def.Names, def.Format)
		if def.Where != "" {
			fmt.Fprintf(&sb, " where=%q", def.Where)
		}
		fmt.Fprintf(&sb, " maxGroups=%d", def.MaxGroups)
	}
	return sb.String()
}
