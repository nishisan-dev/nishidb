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

// pendingCounterRebuild é um contador que a abertura decidiu RECONSTRUIR: a
// varredura roda depois do ramo de recovery, e o motivo vai para o log.
type pendingCounterRebuild struct {
	c      *counter.Counter
	reason string
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
// o conteúdo do vetor (nome, colunas, e a identidade de where e scope — ver
// identityOf). Divergiu, ou não estava lá, ou o
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
	var declared []checkpoint.CounterInfo
	if m != nil {
		declared = m.Counters
	}
	if opts.CompileCountersCallbackFunction == nil {
		if len(declared) > 0 {
			names := make([]string, 0, len(declared))
			for _, d := range declared {
				names = append(names, d.Name)
			}
			return fmt.Errorf("%w: o dataset %s declara os contadores %v no manifest e esta abertura não forneceu CompilaContadores; "+
				"abrir sem as definições deixaria o contador parado enquanto o Put anda",
				ErrInvalidArgument, opts.Dir, names)
		}
		return nil
	}
	defs, err := opts.CompileCountersCallbackFunction(s.dict)
	if err != nil {
		return fmt.Errorf("storage: compilar os contadores de %s: %w", opts.Dir, err)
	}
	if len(defs) == 0 && len(declared) == 0 {
		return nil
	}
	if len(defs) > 0 && opts.Mode == HistoryMode {
		// A retenção do histórico purga linhas por marca-d'água, sem passar
		// pelo applyRecord: o contador nunca decrementaria. E não há estado
		// corrente a contar num log append-only.
		return fmt.Errorf("%w: ModoHistorico não aceita counters: a tabela histórica não tem estado corrente a contar", ErrInvalidArgument)
	}
	dir := filepath.Join(idxDir, diretorioOfCounters)
	byName := make(map[string]checkpoint.CounterInfo, len(declared))
	for _, info := range declared {
		byName[info.Name] = info
	}
	seen := make(map[string]struct{}, len(defs))
	for _, def := range defs {
		if _, dup := seen[def.Name]; dup {
			return fmt.Errorf("%w: contador %q declarado duas vezes", ErrInvalidArgument, def.Name)
		}
		seen[def.Name] = struct{}{}
		info, had := byName[def.Name]
		delete(byName, def.Name)

		counterDir := filepath.Join(dir, def.Name)
		building, err := counter.BuildingMarked(counterDir)
		if err != nil {
			return fmt.Errorf("storage: abrir o contador %q de %s: %w", def.Name, opts.Dir, err)
		}
		recreate, reason := false, ""
		switch {
		case building:
			// Vem ANTES de tudo: o marcador diz que o vetor em disco é de uma
			// construção que não chegou ao checkpoint que a adota — zerado ou
			// parcial —, e nem parâmetros iguais no manifest nem um vetor que
			// cobre as linhas dizem nada sobre o conteúdo dele.
			recreate, reason = true, "a construção anterior do contador não chegou ao checkpoint que a adota"
		case m == nil:
			// Dataset novo: não há o que reconstruir, o vetor nasce vazio.
		case !had:
			recreate, reason = true, "o dataset não tinha este contador"
		case !sameDefinition(info, def):
			oldWhere, oldScope := splitIdentity(info.Where)
			recreate, reason = true, fmt.Sprintf("os parâmetros mudaram (groupBy %v → %v, where %q → %q, scope %q → %q)",
				info.Columns, columnsOf(def), oldWhere, def.Where, oldScope, def.Scope)
		}
		// O piso de durabilidade do dicionário (seção 0x0002 do manifest) só
		// vale para o contador que vai ser ADOTADO: com recreate os arquivos
		// vão para o lado de qualquer jeito.
		var dictFloor int64
		if had && !recreate {
			dictFloor = info.DictValidBytes
		}
		c, err := counter.Open(counterDir, def, minRows, recreate, dictFloor)
		if errors.Is(err, counter.ErrDictBelowFloor) {
			// O dicionário perdeu dado que um checkpoint declarou durável.
			// Adotá-lo deixaria o vetor apontando para ids que ele não tem, e
			// a dobra os zeraria como órfãos: contagem baixa sem erro. A
			// resposta é reconstruir, e é decidida AQUI — antes do
			// OpenMeasures, que com reset não confere os ids do snapshot das
			// medidas contra um dicionário que encolheu.
			s.opts.Log.Warn("counter.dict_below_durable_floor",
				"the group dictionary lost bytes that a checkpoint had recorded as durable; the counter is rebuilt from the live rows",
				"counter", def.Name, "durable_floor_bytes", dictFloor, "error", err.Error())
			recreate, reason = true, "o dicionário de grupos perdeu dado abaixo do piso de durabilidade do último checkpoint"
			c, err = counter.Open(counterDir, def, minRows, true, 0)
		}
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
		if err := c.OpenMeasures(measureEpoch, persistedMeasures, recreate); err != nil {
			if errors.Is(err, counter.ErrMeasureSnapshotCorrupt) {
				return fmt.Errorf("%w: %w", ErrDatasetCorrupt, err)
			}
			return err
		}
		for _, measure := range c.MeasureResetNames() {
			s.opts.Log.Info("counter_measure.reset", "measure starts a new window without backfill", "counter", def.Name, "measure", measure)
		}
		if !recreate && m != nil && ((c.Born() && m.NextRowID > 0) || !c.Covers(types.RowID(m.NextRowID))) {
			// O manifest declara os parâmetros, não a existência dos arquivos —
			// a mesma pergunta que a agenda faz com NeedsRebuild.
			recreate, reason = true, "os arquivos do contador não descrevem as linhas do dataset"
			// O marcador vem ANTES do Clear: o vetor é zerado in place, e a
			// partir daqui o conteúdo dele só volta a valer depois do
			// checkpoint que adota a construção.
			if err := c.MarkBuilding(); err != nil {
				return err
			}
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
		if recreate {
			s.countersToRebuild = append(s.countersToRebuild, pendingCounterRebuild{c: c, reason: reason})
			continue
		}
		if err := c.Fold(); err != nil {
			return fmt.Errorf("storage: dobrar o contador %q de %s: %w", def.Name, opts.Dir, err)
		}
		if orphans := c.Stats().Orphans; orphans > 0 {
			s.opts.Log.Warn("counter.orfas",
				"entradas do vetor apontavam para grupos aposentados ou inexistentes e foram zeradas; não é esperado fora de uma janela de crash",
				"contador", name, "orfas", orphans)
		}
	}
	// Sobras do manifest: contadores que o YAML já não declara.
	leftovers := make([]string, 0, len(byName))
	for name := range byName {
		leftovers = append(leftovers, name)
	}
	slices.Sort(leftovers)
	for _, name := range leftovers {
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
// (na ordem) e a identidade de where e scope (identityOf).
//
// O molde do groupKey NÃO entra, pelo mesmo critério do maxGroups: ele decide
// como o grupo é ESCRITO, não o que foi contado. O texto é derivado da chave em
// toda abertura, então trocá-lo re-renderiza e não custa uma varredura. Quem
// avisa que ele mudou é o log de subida, que sai com o molde efetivo.
func sameDefinition(info checkpoint.CounterInfo, def counter.Definition) bool {
	return info.Name == def.Name && info.Where == identityOf(def) && slices.Equal(info.Columns, columnsOf(def))
}

// identitySeparator separa where e scope no texto de identidade gravado em
// checkpoint.CounterInfo.Where. É `\n` porque nenhum dos dois o contém: os dois
// chegam aqui normalizados por strings.Fields, que come todo espaço em branco,
// quebra de linha inclusive — a não-colisão sai da normalização, e não de um
// escape.
const identitySeparator = "\n"

// identityOf é a função ÚNICA do texto que decide adotar ou reconstruir o vetor
// a partir de where e scope, e é usada SÓ por infoOfCounters (o que vai ao
// manifest) e por sameDefinition (o que se compara com ele).
//
// Sem scope é def.Where byte a byte: um dataset gravado antes do scope existir
// abre sem reconstruir. Com scope é `<where>\n<scope>` — `\n<scope>` quando não
// há where, nunca "" —, um texto que nenhum where normalizado sozinho produz.
// É isso que faz o ROLLBACK reconstruir: o binário antigo compara o texto
// combinado com o seu where, não casa, e reconstrói em vez de adotar um vetor
// cujo conteúdo o scope decidiu.
//
// def.Where continua intocado, e é ele — não esta identidade — que alimenta a
// coluna `condition` de _meta.counters, o log de subida e a impressão digital
// de contexto do alarme.
func identityOf(def counter.Definition) string {
	if def.Scope == "" {
		return def.Where
	}
	return def.Where + identitySeparator + def.Scope
}

// splitIdentity é o inverso de identityOf, para a mensagem de reconstrução
// nomear o where e o scope que o manifest gravou.
func splitIdentity(identity string) (where, scope string) {
	where, scope, _ = strings.Cut(identity, identitySeparator)
	return where, scope
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
//
// DictValidBytes é o piso de durabilidade do dicionário de cada contador: o
// prefixo que um fsync tornou durável. Lido sob o wmu, DEPOIS do fsync que é a
// primeira coisa do ApplyOverlay de cada contador neste mesmo checkpoint — é o
// tamanho no instante desse fsync, e nada interna grupo entre os dois.
func (s *Store) infoOfCounters() []checkpoint.CounterInfo {
	if len(s.counters) == 0 {
		return nil
	}
	out := make([]checkpoint.CounterInfo, 0, len(s.counters))
	for _, c := range s.counters {
		def := c.Def()
		out = append(out, checkpoint.CounterInfo{
			Name: def.Name, Columns: columnsOf(def), Where: identityOf(def),
			DictValidBytes: c.DictSyncedBytes(),
		})
	}
	return out
}

// rebuildPendingCounters varre as linhas vivas para cada contador que
// a abertura decidiu reconstruir. Roda DEPOIS do ramo de recovery: as
// locations já estão publicadas, e o que o replay já transicionou no contador
// recém-nascido é idempotente com a varredura (o delta é contra a entrada
// corrente do vetor).
func (s *Store) rebuildPendingCounters() error {
	for _, p := range s.countersToRebuild {
		start := time.Now()
		rows, err := s.varreRowsVivasInCounter(context.Background(), p.c, false)
		if err != nil {
			return fmt.Errorf("storage: construir o contador %q de %s: %w", p.c.Def().Name, s.opts.Dir, err)
		}
		if err := p.c.RemoveOld(); err != nil {
			return err
		}
		// O marcador gravado pela abertura com recreate (ou antes do Clear)
		// só sai depois do checkpoint do fim do Open, que é o que leva o vetor
		// completo ao disco.
		s.awaitAdoption(p.c)
		s.countersBuilt++
		// O log nomeia o custo pela mesma razão do da agenda: é a única coisa
		// que o operador precisa decidir — segundos numa subida são aceitáveis,
		// minutos num dataset grande mudam a janela de manutenção.
		s.opts.Log.Info("counter.construido",
			"o contador foi construído varrendo as linhas vivas do dataset",
			"contador", p.c.Def().Name, "motivo", p.reason, "linhas", rows,
			"grupos", p.c.Stats().Groups, "duracao", time.Since(start).String())
	}
	s.countersToRebuild = nil
	return nil
}

// awaitAdoption põe um contador recém-construído na fila dos que esperam o
// checkpoint que os adota (countersAwaitingAdoption). Writer-only.
func (s *Store) awaitAdoption(c *counter.Counter) {
	if !slices.Contains(s.countersAwaitingAdoption, c) {
		s.countersAwaitingAdoption = append(s.countersAwaitingAdoption, c)
	}
}

// clearAdoptedBuildingMarks remove o marcador de construção de cada contador
// da fila. Roda no FIM de um checkpoint bem-sucedido, sob o wmu: o manifest e
// os msyncs que vieram antes levaram o vetor completo ao disco, e só a partir
// daí ele pode ser adotado por uma reabertura.
//
// Falha ao remover não derruba o checkpoint — os dados já estão duráveis — e
// o contador fica na fila para a próxima tentativa. O pior caso de um marcador
// que sobra é uma reconstrução desnecessária na próxima abertura, que é custo e
// não erro; por isso é WARN, com o nome do contador.
func (s *Store) clearAdoptedBuildingMarks() {
	if len(s.countersAwaitingAdoption) == 0 {
		return
	}
	kept := s.countersAwaitingAdoption[:0]
	for _, c := range s.countersAwaitingAdoption {
		if err := c.ClearBuildingMark(); err != nil {
			s.opts.Log.Warn("counter.building_mark_not_removed",
				"the counter was built and checkpointed, but its building marker could not be removed; the next open rebuilds it again",
				"counter", c.Def().Name, "error", err.Error())
			kept = append(kept, c)
		}
	}
	s.countersAwaitingAdoption = kept
}

// varreRowsVivasInCounter transiciona, no contador, toda linha com
// location e sem lápide, lendo o record de cada uma. sobAposMu diz se a
// leitura precisa da proteção contra a aposentadoria de segmento — na
// abertura não há concorrência; em operação (RebuildCounters) o writer
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

// RebuildCounters zera cada contador e o refaz varrendo as linhas vivas,
// SOB o wmu: a ingestão para pela varredura inteira. É o remédio para uma
// deriva que a auditoria apontou — explícito, caro e raro —, e o custo é
// dito no godoc e na documentação em vez de escondido atrás de um caminho
// "sem lock" que deixaria o contador inconsistente enquanto o Put anda.
//
// O Clear zera o vetor in place, num mmap compartilhado, e por isso o marcador
// de construção em curso é gravado ANTES dele e só sai no fim do primeiro
// checkpoint bem-sucedido DEPOIS da varredura (awaitAdoption). Um processo que
// morra nesse meio reabre reconstruindo, em vez de adotar o vetor zerado.
// Uma varredura que falha deixa o marcador no lugar e o contador fora da
// lista: a reabertura reconstrói.
func (s *Store) RebuildCounters(ctx context.Context) ([]counter.Rebuild, error) {
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
		if err := c.MarkBuilding(); err != nil {
			return nil, fmt.Errorf("storage: reconstruir o contador %q: %w", c.Def().Name, err)
		}
		if err := c.Clear(); err != nil {
			return nil, fmt.Errorf("storage: reconstruir o contador %q: %w", c.Def().Name, err)
		}
		rows, err := s.varreRowsVivasInCounter(ctx, c, true)
		if err != nil {
			return nil, fmt.Errorf("storage: reconstruir o contador %q: %w", c.Def().Name, err)
		}
		s.awaitAdoption(c)
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

// RetireEmptyGroups roda UMA passada de aposentadoria em todos os
// contadores, sob o wmu. Devolve quantos grupos saíram.
//
// O registro de aposentadoria é fsyncado na hora, e a falha desse fsync deixa
// o writer FALHO (ErrDictSync → failWriter), como a do checkpoint: depois de
// um EIO de writeback as páginas perdidas saem do cache como limpas, e o
// próximo fsync — o do checkpoint — "passaria" com os grupos pendentes fora do
// disco. Só parar o laço deixaria essa porta do fsyncgate aberta.
func (s *Store) RetireEmptyGroups() (int, error) {
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
		n, err := c.RetireEmptyGroups(s.now(), s.counterDictSyncStep(c, ""))
		total += n
		if errors.Is(err, counter.ErrDictSync) {
			return total, s.failWriter(fmt.Errorf("storage: aposentar grupos do contador %q: %w", c.Def().Name, err))
		}
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
		n, err := s.RetireEmptyGroups()
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
		if def.Scope != "" {
			fmt.Fprintf(&sb, " scope=%q", def.Scope)
		}
		if def.Where != "" {
			fmt.Fprintf(&sb, " where=%q", def.Where)
		}
		fmt.Fprintf(&sb, " maxGroups=%d", def.MaxGroups)
	}
	return sb.String()
}
