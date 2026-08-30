package server

import (
	"context"
	"fmt"
	"sort"

	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocolo"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

// validaNomesDeHistorico recusa o nome derivado que colide com um dataset
// declarado.
//
// A colisão é decidida ANTES de abrir qualquer coisa, junto da unicidade de
// groupId e pela mesma razão: um conflito entre o primeiro arquivo e o último
// não pode ser descoberto com meio processo já de pé. E o desfecho aqui é
// pior que o de lá — o dataset declarado e a tabela irmã disputariam o mesmo
// diretório, com dois writers, dois contadores de rowId e dois manifests.
func validaNomesDeHistorico(specs map[string]*schema.DatasetSpec) error {
	derivados := map[string]string{}
	nomes := make([]string, 0, len(specs))
	for nome := range specs {
		nomes = append(nomes, nome)
	}
	// Ordem alfabética para que a mensagem de um conjunto com dois defeitos
	// seja sempre a mesma — o laço de um mapa não tem ordem.
	sort.Strings(nomes)
	for _, nome := range nomes {
		h := specs[nome].Historico
		if h == nil {
			continue
		}
		derivado := h.NomeDaTabela(nome)
		if _, declarado := specs[derivado]; declarado {
			return fmt.Errorf("o dataset %q declara history com sufixo %q, e a tabela irmã %q colide com um dataset declarado em datasets.directory: "+
				"os dois disputariam o mesmo diretório; mude o sufixo ou renomeie o dataset",
				nome, h.Sufixo, derivado)
		}
		if dono, repetido := derivados[derivado]; repetido {
			return fmt.Errorf("os datasets %q e %q derivam a mesma tabela irmã %q; mude o sufixo de um deles",
				dono, nome, derivado)
		}
		derivados[derivado] = nome
	}
	return nil
}

// abreHistorico abre e registra a tabela irmã do dataset base.
//
// A ORDEM aqui é o contrato inteiro da feature, e cada passo depende do
// anterior:
//
//  1. o Store da irmã recebe o dicionário do BASE — sem isso os FieldIDs
//     divergem e o payload decodifica colunas trocadas;
//  2. a spec derivada compila contra esse MESMO dicionário, o que faz cada
//     coluna sair com o id que o base já usa;
//  3. o Compiled do base passa a espelhar o da irmã, para que a coluna
//     descoberta em runtime apareça nas duas tabelas sem esperar o restart;
//  4. a irmã é registrada no motor sob o próprio nome, e a partir daí
//     `SELECT ... FROM events_history` funciona da mesma conexão — sem tocar
//     em lexer, parser, binder, protocolo ou driver;
//  5. o materializer do base ganha o escritor, e só então o primeiro evento
//     pode chegar.
func (s *Servidor) abreHistorico(base *dataset, log *logging.Logger) (*dataset, error) {
	h := base.spec.Historico
	if h == nil {
		return nil, nil
	}
	specIrma := base.spec.DerivaHistorico()
	segmento := s.cfg.SegmentSize()
	if h.SegmentSize > 0 {
		segmento = h.SegmentSize
	}
	// A conferência do teto contra o segmento EFETIVO: quem omitiu
	// `history.segmentSize` herdou o global, que a spec não conhecia no load.
	if err := h.ValidaContraSegmento(segmento); err != nil {
		return nil, fmt.Errorf("dataset %q: %w", base.nome, err)
	}

	st, err := storage.Open(storage.Options{
		Dir:                    s.cfg.CaminhoDoDataset(specIrma.Name),
		Dataset:                specIrma.Name,
		SegmentSize:            segmento,
		Modo:                   storage.ModoHistorico,
		DictCompartilhado:      base.st.FieldDict(),
		CamposIndexadosPorNome: schema.CamposComIndice(specIrma),
		ColunasDerivadas:       specIrma.NomesDasExtracoes(),
		CampoDeEscrita:         schema.CampoInstanteDeEscrita,
		Log:                    log,
		Metrics:                s.reg,
	})
	if err != nil {
		return nil, fmt.Errorf("tabela histórica de %q: %w", base.nome, err)
	}
	compilado, err := specIrma.Compile(base.st.FieldDict())
	if err != nil {
		return nil, fechandoStore(st, fmt.Errorf("tabela histórica de %q: %w", base.nome, err))
	}
	base.sch.Espelha(compilado)
	if err := s.eng.Register(specIrma.Name, compilado, st); err != nil {
		return nil, fechandoStore(st, fmt.Errorf("tabela histórica de %q: %w", base.nome, err))
	}

	var raw *schema.CompiledField
	if h.RawPayload {
		f, ok := compilado.FieldByName([]byte(schema.CampoRaw))
		if !ok {
			return nil, fechandoStore(st, fmt.Errorf("tabela histórica de %q: a coluna %q não compilou",
				base.nome, schema.CampoRaw))
		}
		raw = f
	}
	base.mat.LigaHistorico(st, raw)

	log.Info("historico.aberto",
		"a tabela histórica está de pé",
		"dataset", base.nome, "tabela", specIrma.Name,
		"config", schema.DescreveHistorico(specIrma.Name, h))
	// Janela maior que a retenção não é erro, é INÓCUA: a retenção aposenta o
	// segmento antes de a janela o alcançar. O WARN é o meio-termo que a
	// doutrina pede — a configuração nunca é ignorada em silêncio, mas só é
	// recusada quando estar errada tem consequência, e aqui não tem: recusar
	// transformaria redundância em queda de serviço.
	//
	// Sai daqui, e não do ligaRetencao, porque é aqui que os DOIS nomes existem:
	// quem lê o aviso precisa do dataset BASE para achar o YAML em que a chave
	// foi escrita, e lá só há a tabela irmã.
	if h.JanelaExcedeARetencao() {
		log.Warn("janela.maior_que_retencao",
			"history.index.window é maior que history.expireAfterWrite e não indexa nada a mais: "+
				"a retenção aposenta o segmento antes de a janela o alcançar",
			"dataset", base.nome, "tabela", specIrma.Name,
			"window", h.Janela.Duracao.String(),
			"expireAfterWrite", h.ExpireAfterWrite.String())
	}

	return &dataset{
		nome:        specIrma.Name,
		spec:        specIrma,
		sch:         compilado,
		st:          st,
		ehHistorico: true,
	}, nil
}

// ligaRetencao sobe o motor de retenção das tabelas históricas.
//
// Depois do HTTP e antes da ingestão, na ordem do §51 aplicada a mais um
// consumidor de disco — e pela mesma razão da compaction: é o consumer que
// faz a tabela crescer, e ligar a retenção depois dele abriria uma janela em
// que o teto de tamanho não vale.
func (s *Servidor) ligaRetencao(ctx context.Context) {
	var comRetencao []string
	for _, d := range s.datasets {
		if !d.ehHistorico || d.spec.Historico == nil {
			continue
		}
		h := d.spec.Historico
		d.st.IniciaRetencao(ctx, storage.LimitesDeRetencao{
			ExpireAfterWrite: h.ExpireAfterWrite,
			MaxBytes:         h.MaxBytes,
			Janela:           janelaDoDataset(d),
		})
		comRetencao = append(comRetencao, d.nome)
	}
	if len(comRetencao) == 0 {
		return
	}
	sort.Strings(comRetencao)
	s.log.Info("retencao.ligada",
		"o motor de retenção das tabelas históricas está de pé", "tabelas", comRetencao)
}

// ligaReconstrucao sobe o laço que consome a fila de reconstrução do índice por
// segmento (§2.4), e é ele que faz `index: true` num dataset já povoado
// significar alguma coisa: sem ele os segmentos entram na fila e ninguém a
// drena, então o campo continua enumerando indefinidamente.
//
// Em TODOS os datasets, e não só nos históricos, porque a reconstrução vale para
// os dois modos (decisão D1): o backfill de um campo acrescentado ao schema
// acontece no materializado, onde não há passada de retenção nenhuma. É a
// diferença entre ela e a expulsão da janela, que roda dentro da retenção
// justamente por ser exclusiva do histórico.
//
// Depois da retenção, na mesma ordem do §51 que a compaction e a retenção já
// seguem: primeiro quem DEVOLVE disco, depois quem o consome. Uma reconstrução
// que subisse antes gastaria varredura em segmentos que a primeira passada de
// retenção vai aposentar em seguida.
func (s *Servidor) ligaReconstrucao(ctx context.Context) {
	var comReconstrucao []string
	for _, d := range s.datasets {
		d.st.IniciaReconstrucao(ctx, janelaDoDataset(d))
		if d.st.ReconstrucaoAtiva() {
			comReconstrucao = append(comReconstrucao, d.nome)
		}
	}
	if len(comReconstrucao) == 0 {
		return
	}
	sort.Strings(comReconstrucao)
	s.log.Info("reconstrucao.ligada",
		"o laço de reconstrução do índice por segmento está de pé", "datasets", comReconstrucao)
}

// janelaDoDataset devolve a janela de indexação que vale para um dataset, e ela
// é UMA função porque os DOIS consumidores da janela — a expulsão, dentro da
// retenção, e o laço de reconstrução — têm de responder pela MESMA conta.
//
// Se eles discordarem, a que responde "sim" reconstrói o que a outra acabou de
// apagar, para sempre, a até 512 MiB de leitura por volta. É o moinho da §4, e a
// única defesa contra ele é não haver dois lugares onde a resposta é decidida.
//
// O dataset MATERIALIZADO recebe a janela INFINITA mesmo quando declara
// `history:`, e essa linha é a armadilha que a função existe para fechar: o bloco
// `history` de um base descreve a tabela IRMÃ, e traduzi-lo aqui daria uma janela
// finita para um store materializado — que o storage recusa, com razão, porque lá
// a linha vive no segmento corrente independente da idade dela.
func janelaDoDataset(d *dataset) storage.JanelaDeIndice {
	if !d.ehHistorico {
		return storage.JanelaDeIndice{}
	}
	return janelaDaSpec(d.spec.Historico)
}

// janelaDaSpec traduz o bloco `history.index` na política que o storage
// aplica. Bloco ausente vira a janela INFINITA — indexa tudo que a retenção
// mantém —, que é o default e o comportamento de sempre.
//
// A tradução mora aqui, e não no schema, porque o schema não pode importar o
// storage: quem lê o YAML é a camada de cima. É a mesma fronteira que faz os
// campos indexados viajarem por NOME até o Open.
func janelaDaSpec(h *schema.HistoricoSpec) storage.JanelaDeIndice {
	if h == nil || h.Janela == nil {
		return storage.JanelaDeIndice{}
	}
	return storage.JanelaDeIndice{
		Duracao:  h.Janela.Duracao,
		MaxBytes: h.Janela.MaxBytes,
	}
}

// historicoStats monta o bloco `history` do /v1/stats.
//
// Ele é preenchido no dataset BASE, e não na irmã: quem declarou `history:`
// num YAML quer saber o que aquele bloco está fazendo, e a irmã já aparece na
// lista com os próprios números de storage e query como qualquer outro
// dataset.
func historicoStats(base *dataset) *protocolo.HistoricoStats {
	if base.irma == nil || base.spec.Historico == nil {
		return nil
	}
	h := base.spec.Historico
	snap := base.mat.Metrics()
	e := base.irma.st.EstatisticasDeRetencao()
	out := &protocolo.HistoricoStats{
		Table:           base.irma.nome,
		Rows:            snap.HistoricoLinhas,
		Dropped:         snap.HistoricoDescartadas,
		Degraded:        snap.HistoricoDegradado,
		DataBytes:       e.DataBytes,
		IndexBytes:      e.IndexBytes,
		OldestWrittenAt: e.InstanteMaisAntigo,
		PurgedByTTL:     e.PurgadosPorTTL,
		PurgedBySize:    e.PurgadosPorTamanho,
		MaxBytes:        h.MaxBytes,
	}
	if h.ExpireAfterWrite > 0 {
		out.ExpireAfterWrite = h.ExpireAfterWrite.String()
	}
	// A cobertura efetiva sai SEMPRE que há índice, e o `window` declarado só
	// quando existe: sem a janela declarada a comparação é contra o infinito, e
	// ecoar um "0" ali faria o painel parecer dizer que a janela é nula.
	out.EffectiveIndexWindow = e.JanelaEfetiva.Seconds()
	if h.Janela != nil && h.Janela.Duracao > 0 {
		out.IndexWindow = h.Janela.Duracao.String()
	}
	return out
}

// amostrasDeHistorico publica as mesmas contagens no /metrics.
//
// Saem da MESMA leitura de snapshot que alimenta o /v1/stats, pela razão que
// o metricas_dataset.go já registra: dois contadores para o mesmo fato
// divergem no dia em que um caminho de erro esquece de somar, e em produção
// alguém lê o errado.
//
// O CAMPO Familia É OBRIGATÓRIO em cada amostra, e a omissão dele era um defeito
// silencioso: o WriteTo agrupa os cabeçalhos por Familia, então dez amostras com
// Familia vazia compartilhavam UM grupo — o `# HELP` e o `# TYPE` saíam uma vez
// só, com o nome VAZIO (`# HELP  Linhas gravadas...`), e as dez séries chegavam
// ao operador sem tipo e sem ajuda. É a mesma armadilha que o godoc da Amostra
// descreve para o histograma, com o sinal trocado: lá o perigo é emitir cabeçalho
// demais, aqui é emitir de menos.
//
// Isso importa além da estética: metade do contrato de
// nishidb_history_index_bytes viaja no TEXTO — é ali que se diz que a purga
// devolve estes bytes desde que o índice passou a ser por segmento —, e uma
// ajuda que nunca é emitida é prosa que ninguém lê.
func amostrasDeHistorico(base *dataset) []metrics.Amostra {
	if base.irma == nil || base.spec.Historico == nil {
		return nil
	}
	snap := base.mat.Metrics()
	e := base.irma.st.EstatisticasDeRetencao()
	rotulo := []metrics.Label{{Nome: metrics.LabelDataset, Valor: base.nome}}
	// O formato de exposição só tem número, e Go não converte bool para
	// float: os 0/1 se escrevem aqui, uma vez, em vez de dentro do literal.
	var degradado, rodando float64
	if snap.HistoricoDegradado {
		degradado = 1
	}
	if base.irma.st.RetencaoAtiva() {
		rodando = 1
	}
	return []metrics.Amostra{
		{Familia: metrics.MetHistoryRows, Nome: metrics.MetHistoryRows, Tipo: metrics.TipoContador, Valor: float64(snap.HistoricoLinhas),
			Ajuda: "Linhas gravadas na tabela histórica.", Labels: rotulo},
		{Familia: metrics.MetHistoryDropped, Nome: metrics.MetHistoryDropped, Tipo: metrics.TipoContador, Valor: float64(snap.HistoricoDescartadas),
			Ajuda: "Eventos que não viraram linha na tabela histórica.", Labels: rotulo},
		{Familia: metrics.MetHistoryDegraded, Nome: metrics.MetHistoryDegraded, Tipo: metrics.TipoMedidor, Valor: degradado,
			Ajuda: "1 quando a tabela histórica parou de receber (modo falho sticky).", Labels: rotulo},
		{Familia: metrics.MetHistoryDataBytes, Nome: metrics.MetHistoryDataBytes, Tipo: metrics.TipoMedidor, Valor: float64(e.DataBytes),
			Ajuda: "Bytes dos segmentos da tabela histórica — o que o maxBytes governa.", Labels: rotulo},
		{Familia: metrics.MetHistoryIndexBytes, Nome: metrics.MetHistoryIndexBytes, Tipo: metrics.TipoMedidor, Valor: float64(e.IndexBytes),
			Ajuda: "Blocos alocados pelos índices da tabela histórica; a purga também os devolve, porque o .sidx sai no mesmo passo que remove o .dat. O número é da tabela IRMÃ, publicado no rótulo do dataset BASE; a parcela por segmento dele é nishidb_index_bytes{dataset=\"X_history\"}, que sai em OUTRO rótulo — subtrair as duas no mesmo rótulo dá o número inteiro de volta.", Labels: rotulo},
		{Familia: metrics.MetHistoryOldestWrittenAt, Nome: metrics.MetHistoryOldestWrittenAt, Tipo: metrics.TipoMedidor, Valor: float64(e.InstanteMaisAntigo),
			Ajuda: "Instante da linha mais antiga ainda visível, em millis. Mostra o maxBytes engolindo o prazo declarado.", Labels: rotulo},
		{Familia: metrics.MetHistoryIndexWindow, Nome: metrics.MetHistoryIndexWindow, Tipo: metrics.TipoMedidor, Valor: e.JanelaEfetiva.Seconds(),
			Ajuda: "Intervalo realmente coberto pelos segmentos indexados. Compare com history.index.window: menor significa que o teto do índice assumiu o comando.", Labels: rotulo},
		{Familia: metrics.MetHistoryPurged, Nome: metrics.MetHistoryPurged, Tipo: metrics.TipoContador, Valor: float64(e.PurgadosPorTTL),
			Ajuda:  "Segmentos aposentados pela retenção da tabela histórica.",
			Labels: append(append([]metrics.Label{}, rotulo...), metrics.Label{Nome: "reason", Valor: "ttl"})},
		{Familia: metrics.MetHistoryPurged, Nome: metrics.MetHistoryPurged, Tipo: metrics.TipoContador, Valor: float64(e.PurgadosPorTamanho),
			Labels: append(append([]metrics.Label{}, rotulo...), metrics.Label{Nome: "reason", Valor: "size"})},
		{Familia: metrics.MetHistoryRetentionRunning, Nome: metrics.MetHistoryRetentionRunning, Tipo: metrics.TipoMedidor, Valor: rodando,
			Ajuda: "1 quando o motor de retenção está de pé. Ele para de vez na primeira falha.", Labels: rotulo},
	}
}
