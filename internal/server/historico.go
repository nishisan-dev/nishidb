package server

import (
	"context"
	"fmt"
	"sort"

	"github.com/nishisan-dev/nishidb/internal/logging"
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
		Dir:               s.cfg.CaminhoDoDataset(specIrma.Name),
		Dataset:           specIrma.Name,
		SegmentSize:       segmento,
		Modo:              storage.ModoHistorico,
		DictCompartilhado: base.st.FieldDict(),
		BitmapFieldNames:  schema.CamposComBitmap(specIrma),
		HashFieldNames:    schema.CamposComHash(specIrma),
		ColunasDerivadas:  specIrma.NomesDasExtracoes(),
		CampoDeEscrita:    schema.CampoInstanteDeEscrita,
		Log:               log,
		Metrics:           s.reg,
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
