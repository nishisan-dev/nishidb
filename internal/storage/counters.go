package storage

import (
	"context"
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

// diretorioDosContadores é o subdiretório de indexes/ onde cada contador tem
// o seu, pelo nome declarado.
const diretorioDosContadores = "contadores"

// pendenciaDeContador é um contador que a abertura decidiu RECONSTRUIR: a
// varredura roda depois do ramo de recovery, e o motivo vai para o log.
type pendenciaDeContador struct {
	c      *counter.Contador
	motivo string
}

// abreContadores compila as definições pelo callback, abre os arquivos de cada
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
func (s *Store) abreContadores(opts Options, m *checkpoint.Manifest, idxDir string, minRows uint64) error {
	var declarados []checkpoint.CounterInfo
	if m != nil {
		declarados = m.Counters
	}
	if opts.CompilaContadores == nil {
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
	defs, err := opts.CompilaContadores(s.dict)
	if err != nil {
		return fmt.Errorf("storage: compilar os contadores de %s: %w", opts.Dir, err)
	}
	if len(defs) == 0 && len(declarados) == 0 {
		return nil
	}
	if len(defs) > 0 && opts.Modo == ModoHistorico {
		// A retenção do histórico purga linhas por marca-d'água, sem passar
		// pelo applyRecord: o contador nunca decrementaria. E não há estado
		// corrente a contar num log append-only.
		return fmt.Errorf("%w: ModoHistorico não aceita counters: a tabela histórica não tem estado corrente a contar", ErrInvalidArgument)
	}
	dir := filepath.Join(idxDir, diretorioDosContadores)
	porNome := make(map[string]checkpoint.CounterInfo, len(declarados))
	for _, info := range declarados {
		porNome[info.Name] = info
	}
	vistos := make(map[string]struct{}, len(defs))
	for _, def := range defs {
		if _, dup := vistos[def.Nome]; dup {
			return fmt.Errorf("%w: contador %q declarado duas vezes", ErrInvalidArgument, def.Nome)
		}
		vistos[def.Nome] = struct{}{}
		info, tinha := porNome[def.Nome]
		delete(porNome, def.Nome)

		recria, motivo := false, ""
		switch {
		case m == nil:
			// Dataset novo: não há o que reconstruir, o vetor nasce vazio.
		case !tinha:
			recria, motivo = true, "o dataset não tinha este contador"
		case !mesmaDefinicao(info, def):
			recria, motivo = true, fmt.Sprintf("os parâmetros mudaram (groupBy %v → %v, where %q → %q)",
				info.Columns, colunasDe(def), info.Where, def.Where)
		}
		c, err := counter.Abre(filepath.Join(dir, def.Nome), def, minRows, recria)
		if err != nil {
			return fmt.Errorf("storage: abrir o contador %q de %s: %w", def.Nome, opts.Dir, err)
		}
		s.contadores = append(s.contadores, c)
		if !recria && m != nil && ((c.Nasceu() && m.NextRowID > 0) || !c.Cobre(types.RowID(m.NextRowID))) {
			// O manifest declara os parâmetros, não a existência dos arquivos —
			// a mesma pergunta que a agenda faz com NeedsRebuild.
			recria, motivo = true, "os arquivos do contador não descrevem as linhas do dataset"
			if err := c.Zera(); err != nil {
				return err
			}
		}
		if est := c.Estatisticas(); est.Grupos > def.MaxGroups {
			return fmt.Errorf("%w: contador %q tem %d grupos em uso e maxGroups %d; o teto só pode diminuir até os grupos em uso (ou reconstrua o contador)",
				ErrInvalidArgument, def.Nome, est.Grupos, def.MaxGroups)
		}
		nome := def.Nome
		c.AoRecusar(func() {
			s.opts.Log.Warn("contador.teto",
				"um grupo novo foi recusado pelo teto de maxGroups; a linha entrou, mas não é contada até o contador ser reconstruído com teto maior",
				"contador", nome, "maxGroups", def.MaxGroups)
		})
		if recria {
			s.contadoresARecriar = append(s.contadoresARecriar, pendenciaDeContador{c: c, motivo: motivo})
			continue
		}
		if err := c.Dobra(); err != nil {
			return fmt.Errorf("storage: dobrar o contador %q de %s: %w", def.Nome, opts.Dir, err)
		}
		if orfas := c.Estatisticas().Orfas; orfas > 0 {
			s.opts.Log.Warn("contador.orfas",
				"entradas do vetor apontavam para grupos aposentados ou inexistentes e foram zeradas; não é esperado fora de uma janela de crash",
				"contador", nome, "orfas", orfas)
		}
	}
	// Sobras do manifest: contadores que o YAML já não declara.
	sobras := make([]string, 0, len(porNome))
	for nome := range porNome {
		sobras = append(sobras, nome)
	}
	slices.Sort(sobras)
	for _, nome := range sobras {
		if err := os.RemoveAll(filepath.Join(dir, nome)); err != nil {
			return fmt.Errorf("storage: apagar o contador %q removido da configuração: %w", nome, err)
		}
		s.opts.Log.Info("contador.removido",
			"o contador saiu da configuração do dataset e os arquivos dele foram apagados",
			"contador", nome)
	}
	return nil
}

// mesmaDefinicao compara os parâmetros que decidem o conteúdo: nome, colunas
// (na ordem) e where.
func mesmaDefinicao(info checkpoint.CounterInfo, def counter.Definicao) bool {
	return info.Name == def.Nome && info.Where == def.Where && slices.Equal(info.Columns, colunasDe(def))
}

func colunasDe(def counter.Definicao) []uint16 {
	out := make([]uint16, len(def.Colunas))
	for i, c := range def.Colunas {
		out[i] = uint16(c)
	}
	return out
}

// infoDosContadores é a seção do manifest, na ordem em que os contadores
// foram declarados.
func (s *Store) infoDosContadores() []checkpoint.CounterInfo {
	if len(s.contadores) == 0 {
		return nil
	}
	out := make([]checkpoint.CounterInfo, 0, len(s.contadores))
	for _, c := range s.contadores {
		def := c.Def()
		out = append(out, checkpoint.CounterInfo{Name: def.Nome, Columns: colunasDe(def), Where: def.Where})
	}
	return out
}

// reconstroiContadoresPendentes varre as linhas vivas para cada contador que
// a abertura decidiu reconstruir. Roda DEPOIS do ramo de recovery: as
// locations já estão publicadas, e o que o replay já transicionou no contador
// recém-nascido é idempotente com a varredura (o delta é contra a entrada
// corrente do vetor).
func (s *Store) reconstroiContadoresPendentes() error {
	for _, p := range s.contadoresARecriar {
		inicio := time.Now()
		linhas, err := s.varreLinhasVivasNoContador(context.Background(), p.c, false)
		if err != nil {
			return fmt.Errorf("storage: construir o contador %q de %s: %w", p.c.Def().Nome, s.opts.Dir, err)
		}
		if err := p.c.ApagaAntigos(); err != nil {
			return err
		}
		s.contadoresConstruidos++
		// O log nomeia o custo pela mesma razão do da agenda: é a única coisa
		// que o operador precisa decidir — segundos numa subida são aceitáveis,
		// minutos num dataset grande mudam a janela de manutenção.
		s.opts.Log.Info("contador.construido",
			"o contador foi construído varrendo as linhas vivas do dataset",
			"contador", p.c.Def().Nome, "motivo", p.motivo, "linhas", linhas,
			"grupos", p.c.Estatisticas().Grupos, "duracao", time.Since(inicio).String())
	}
	s.contadoresARecriar = nil
	return nil
}

// varreLinhasVivasNoContador transiciona, no contador, toda linha com
// location e sem lápide, lendo o record de cada uma. sobAposMu diz se a
// leitura precisa da proteção contra a aposentadoria de segmento — na
// abertura não há concorrência; em operação (ReconstroiContadores) o writer
// já segura o wmu e a leitura do par (location, bytes) entra no aposMu, que é
// o lock mais interno.
func (s *Store) varreLinhasVivasNoContador(ctx context.Context, c *counter.Contador, sobAposMu bool) (uint64, error) {
	limite := types.RowID(s.nextRowID.Load())
	var buf []byte
	var linhas uint64
	for id := types.RowID(0); id < limite; id++ {
		if id&passoDoContexto == 0 {
			if err := ctx.Err(); err != nil {
				return linhas, err
			}
		}
		packed, ok := s.locs.Get(id)
		if !ok {
			continue
		}
		seg, off, flags := rowindex.Unpack(packed)
		if flags&types.FlagTombstone != 0 {
			continue
		}
		if sobAposMu {
			s.aposMu.RLock()
		}
		rec, b, err := s.segR.ReadRecord(seg, off, buf)
		if sobAposMu {
			s.aposMu.RUnlock()
		}
		buf = b
		if err != nil {
			return linhas, fmt.Errorf("ler a linha %d em (%d, %d): %w", id, seg, off, err)
		}
		if rec.RowID != id {
			return linhas, fmt.Errorf("%w: a location da linha %d aponta para o record da linha %d", ErrDatasetCorrupt, id, rec.RowID)
		}
		if err := c.Transiciona(id, rec.Payload, false); err != nil {
			return linhas, err
		}
		linhas++
	}
	return linhas, nil
}

// Contadores devolve os contadores abertos, na ordem declarada. A slice é do
// Store: quem chama não a altera.
func (s *Store) Contadores() []*counter.Contador { return s.contadores }

// EstatisticasDeContadores é o retrato de todos os contadores, para as
// métricas e o /v1/stats — a MESMA chamada nos dois.
func (s *Store) EstatisticasDeContadores() []counter.Estatisticas {
	out := make([]counter.Estatisticas, 0, len(s.contadores))
	for _, c := range s.contadores {
		out = append(out, c.Estatisticas())
	}
	return out
}

// passoDoContexto é a cadência (em linhas, potência de dois menos um usada
// como máscara) com que as varreduras longas olham o contexto e a validade do
// snapshot. Mil linhas são microssegundos; olhar por linha custaria mais que
// a própria leitura.
const passoDoContexto = 1023

// recalculoDeContador é o que a varredura da auditoria acumula por contador.
type recalculoDeContador struct {
	totais, refs  []int64
	naoInternadas uint64
	deriv         *counter.Derivador
}

// AuditaContadores recalcula cada contador por varredura e compara com o
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
// antigo é invalidado — pode ser este — e a resposta é ErrSnapshotExpirado,
// nunca um relatório: uma divergência inventada por versão perdida mandaria o
// operador reconstruir um contador certo.
func (s *Store) AuditaContadores(ctx context.Context) ([]counter.Relatorio, error) {
	if err := s.entra(); err != nil {
		return nil, err
	}
	defer s.sai()
	if len(s.contadores) == 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	inicio := time.Now()

	s.wmu.Lock()
	snap := s.AbreSnapshot()
	if s.audAntesDaCopia != nil {
		s.audAntesDaCopia()
	}
	congelados := make([]counter.Congelado, len(s.contadores))
	for i, c := range s.contadores {
		congelados[i] = c.Congela()
	}
	s.wmu.Unlock()
	defer s.FechaSnapshot(snap)

	recalc := make([]recalculoDeContador, len(s.contadores))
	for i, c := range s.contadores {
		recalc[i] = recalculoDeContador{
			totais: make([]int64, congelados[i].Limite),
			refs:   make([]int64, congelados[i].Limite),
			deriv:  c.NovoDerivador(),
		}
	}

	var buf []byte
	var linhas uint64
	for id := snap.BaseRowID; id < snap.LimitRowID; id++ {
		if id&passoDoContexto == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		// A validade vem ANTES de consultar o journal: um snapshot invalidado
		// já pode ter tido as versões dele podadas, e o journal responderia
		// "location corrente" para uma linha que mudou depois de T0.
		if !s.snaps.valido(snap.Epoch) {
			return nil, ErrSnapshotExpirado
		}
		var packed uint64
		var ok bool
		if !s.vers.Vazio() {
			packed, ok = s.vers.Em(id, snap.Epoch)
		}
		if !ok {
			if packed, ok = s.locs.Get(id); !ok {
				continue
			}
		}
		seg, off, flags := rowindex.Unpack(packed)
		if flags&types.FlagTombstone != 0 {
			continue
		}
		s.aposMu.RLock()
		rec, b, err := s.segR.ReadRecord(seg, off, buf)
		s.aposMu.RUnlock()
		buf = b
		if err != nil {
			return nil, fmt.Errorf("storage: auditoria dos contadores: ler a linha %d em (%d, %d): %w", id, seg, off, err)
		}
		if rec.RowID != id {
			return nil, fmt.Errorf("%w: a location da linha %d aponta para o record da linha %d", ErrDatasetCorrupt, id, rec.RowID)
		}
		linhas++
		for i, c := range s.contadores {
			chave, cond, tem, err := recalc[i].deriv.Deriva(rec.Payload)
			if err != nil {
				return nil, fmt.Errorf("storage: auditoria do contador %q na linha %d: %w", c.Def().Nome, id, err)
			}
			if !tem {
				continue
			}
			g, ok := c.PorChave(chave)
			if !ok || g >= congelados[i].Limite {
				// Grupo internado DEPOIS de T0 (a linha era de outro grupo em
				// T0 e o journal já devolveu a versão T0, então isto só
				// acontece por deriva) ou nunca internado.
				recalc[i].naoInternadas++
				continue
			}
			recalc[i].refs[g]++
			if cond {
				recalc[i].totais[g]++
			}
		}
		if s.audTrace != nil {
			s.audTrace(id)
		}
	}
	if !s.snaps.valido(snap.Epoch) {
		return nil, ErrSnapshotExpirado
	}

	duracao := time.Since(inicio)
	out := make([]counter.Relatorio, 0, len(s.contadores))
	for i, c := range s.contadores {
		cg, rc := congelados[i], recalc[i]
		r := counter.Relatorio{
			Nome:           c.Def().Nome,
			Grupos:         c.Estatisticas().Grupos,
			NaoInternadas:  rc.naoInternadas,
			LinhasVarridas: linhas,
			Duracao:        duracao,
		}
		for g := uint32(1); g < cg.Limite; g++ {
			m, mr := cg.Totais[g], cg.Refs[g]
			q, qr := rc.totais[g], rc.refs[g]
			r.Mantido += m
			r.Recalculado += q
			r.LinhasMantidas += mr
			r.LinhasRecalculadas += qr
			if m == q && mr == qr {
				continue
			}
			r.Divergentes++
			if len(r.Amostra) < counter.TetoDaAmostra {
				_, texto, _, _, _ := c.Grupo(g)
				r.Amostra = append(r.Amostra, counter.Divergencia{
					ID: g, Texto: string(texto),
					Mantido: m, Recalculado: q,
					RefsMantido: mr, RefsRecalculado: qr,
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
		r.AgregadoLinhas, r.AgregadoTotal = cg.Linhas, cg.Total
		if r.AgregadoLinhas != r.LinhasRecalculadas {
			r.AgregadosDivergentes++
		}
		if r.AgregadoTotal != r.Recalculado {
			r.AgregadosDivergentes++
		}
		c.RegistraAuditoria(r.Divergentes+int(r.NaoInternadas)+r.AgregadosDivergentes, s.agora())
		out = append(out, r)
	}
	return out, nil
}

// ReconstroiContadores zera cada contador e o refaz varrendo as linhas vivas,
// SOB o wmu: a ingestão para pela varredura inteira. É o remédio para uma
// deriva que a auditoria apontou — explícito, caro e raro —, e o custo é
// dito no godoc e na documentação em vez de escondido atrás de um caminho
// "sem lock" que deixaria o contador inconsistente enquanto o Put anda.
func (s *Store) ReconstroiContadores(ctx context.Context) ([]counter.Reconstrucao, error) {
	if err := s.entra(); err != nil {
		return nil, err
	}
	defer s.sai()
	if len(s.contadores) == 0 {
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
	out := make([]counter.Reconstrucao, 0, len(s.contadores))
	for _, c := range s.contadores {
		inicio := time.Now()
		if err := c.Zera(); err != nil {
			return nil, fmt.Errorf("storage: reconstruir o contador %q: %w", c.Def().Nome, err)
		}
		linhas, err := s.varreLinhasVivasNoContador(ctx, c, true)
		if err != nil {
			return nil, fmt.Errorf("storage: reconstruir o contador %q: %w", c.Def().Nome, err)
		}
		out = append(out, counter.Reconstrucao{
			Nome: c.Def().Nome, Linhas: linhas, Grupos: c.Estatisticas().Grupos, Duracao: time.Since(inicio),
		})
		s.opts.Log.Info("contador.reconstruido",
			"o contador foi reconstruído em operação, varrendo as linhas vivas sob o lock do writer",
			"contador", c.Def().Nome, "linhas", linhas, "grupos", c.Estatisticas().Grupos, "duracao", time.Since(inicio).String())
	}
	return out, nil
}

// CadenciaDaAposentadoria é o intervalo entre passadas do laço de
// aposentadoria de grupos. Um minuto, como o teto de espera do coletor de
// expiração: o prazo de visibilidade é de dias, e a passada é uma varredura
// linear de milhares de grupos — não de milhões de linhas. Exportada para o
// log de subida do servidor nomear a cadência.
const CadenciaDaAposentadoria = time.Minute

// AposentadoriaDeGruposAtiva reporta se o laço está de pé.
func (s *Store) AposentadoriaDeGruposAtiva() bool { return s.apGrupAtiva.Load() }

// temContadorComRetencao diz se algum contador declara retainEmptyGroups.
func (s *Store) temContadorComRetencao() bool {
	for _, c := range s.contadores {
		if c.Def().RetainEmptyGroups > 0 {
			return true
		}
	}
	return false
}

// AposentaGruposVazios roda UMA passada de aposentadoria em todos os
// contadores, sob o wmu. Devolve quantos grupos saíram.
func (s *Store) AposentaGruposVazios() (int, error) {
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
	for _, c := range s.contadores {
		n, err := c.AposentaVazios(s.agora())
		total += n
		if err != nil {
			return total, fmt.Errorf("storage: aposentar grupos do contador %q: %w", c.Def().Nome, err)
		}
	}
	return total, nil
}

// IniciaAposentadoriaDeGrupos sobe o laço que aposenta grupos vazios. Sem
// contador com retainEmptyGroups não há goroutine: o dataset não paga uma
// thread para não fazer nada.
func (s *Store) IniciaAposentadoriaDeGrupos(ctx context.Context) {
	if !s.temContadorComRetencao() {
		return
	}
	s.apGrupMu.Lock()
	defer s.apGrupMu.Unlock()
	if s.apGrupCancela != nil {
		return
	}
	ctx, cancela := context.WithCancel(ctx)
	s.apGrupCancela = cancela
	s.apGrupAtiva.Store(true)
	s.apGrupEmVoo.Add(1)
	go func() {
		defer s.apGrupEmVoo.Done()
		defer s.apGrupAtiva.Store(false)
		s.rodaAposentadoria(ctx)
	}()
}

// ParaAposentadoriaDeGrupos cancela e AGUARDA a passada em voo, pela mesma
// razão do ParaExpiracao: a passada escreve no dicionário do contador, que o
// Close fecha.
func (s *Store) ParaAposentadoriaDeGrupos() {
	s.apGrupMu.Lock()
	defer s.apGrupMu.Unlock()
	if s.apGrupCancela != nil {
		s.apGrupCancela()
		s.apGrupCancela = nil
	}
	s.apGrupEmVoo.Wait()
}

func (s *Store) rodaAposentadoria(ctx context.Context) {
	t := time.NewTicker(CadenciaDaAposentadoria)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n, err := s.AposentaGruposVazios()
		if err != nil {
			// Erro aqui é do writer (sticky) ou do store fechando; insistir
			// só produz log em laço.
			s.opts.Log.Warn("contador.aposentadoria_parou",
				"o laço de aposentadoria de grupos vazios encerrou; grupos vazios deixam de sair da tabela",
				"erro", err)
			return
		}
		if n > 0 {
			s.opts.Log.Info("contador.aposentadoria",
				"grupos vazios há mais que retainEmptyGroups saíram das tabelas de contadores",
				"aposentados", n)
		}
	}
}

// descreveContadores rende as definições numa linha, para o log de subida.
func descreveContadores(cs []*counter.Contador) string {
	var sb strings.Builder
	for i, c := range cs {
		if i > 0 {
			sb.WriteString("; ")
		}
		def := c.Def()
		fmt.Fprintf(&sb, "%s groupBy=%v", def.Nome, def.Nomes)
		if def.Where != "" {
			fmt.Fprintf(&sb, " where=%q", def.Where)
		}
		fmt.Fprintf(&sb, " maxGroups=%d", def.MaxGroups)
	}
	return sb.String()
}
