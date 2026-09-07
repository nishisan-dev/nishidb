package server

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

// specComPoliticas descreve um dataset que usa as três políticas de escrita
// que o merge conhece, mais as regras de TTL — é o conjunto inteiro do que
// este bloco de métricas existe para publicar.
func specComPoliticas() *schema.DatasetSpec {
	return &schema.DatasetSpec{
		Name: "d",
		Materialization: schema.MaterializationSpec{
			RegrasTTL: []schema.RegraTTL{
				{When: "type = 2", Duracao: 3 * time.Minute},
				{Duracao: 7 * 24 * time.Hour},
			},
			DuracaoDeFundo: 7 * 24 * time.Hour,
		},
	}
}

func TestMaterializacaoStatsTraduzOSnapshot(t *testing.T) {
	snap := materializer.MetricsSnapshot{
		CongeladasCapturadas:  7,
		CongeladasPreservadas: 5,
		Acumuladas:            9,
		Saturadas:             1,
		RegrasCasadas:         []int64{4, 2},
		SemRegra:              3,
	}
	got := materializacaoStats(specComPoliticas(), snap)

	if got.FrozenCaptured != 7 || got.FrozenPreserved != 5 {
		t.Errorf("congeladas = %+v", got)
	}
	if got.Summed != 9 || got.SummedSaturated != 1 {
		t.Errorf("soma = %+v", got)
	}
	if got.TTLUnmatched != 3 {
		t.Errorf("semRegra = %d", got.TTLUnmatched)
	}
	if len(got.TTLRules) != 2 {
		t.Fatalf("%d regras publicadas, quero 2", len(got.TTLRules))
	}
	// A regra é publicada com o `when` ao lado da contagem: o índice sozinho
	// obrigaria o operador a abrir o YAML para saber o que casou.
	if got.TTLRules[0].When != "type = 2" || got.TTLRules[0].Matched != 4 {
		t.Errorf("regra 0 = %+v", got.TTLRules[0])
	}
	if got.TTLRules[0].ExpireAfterWrite != "3m0s" {
		t.Errorf("duração da regra 0 = %q", got.TTLRules[0].ExpireAfterWrite)
	}
	if got.TTLRules[1].When != "" || got.TTLRules[1].Matched != 2 {
		t.Errorf("regra de fundo = %+v", got.TTLRules[1])
	}
}

// A regra `never` publica o literal, e não "0s": zero e imortal não são a
// mesma coisa, e o painel que mostrasse 0s diria o oposto do que a regra faz.
func TestMaterializacaoStatsPublicaNeverComoLiteral(t *testing.T) {
	spec := &schema.DatasetSpec{Name: "d", Materialization: schema.MaterializationSpec{
		RegrasTTL: []schema.RegraTTL{{When: "type = 9", Duracao: 0}},
	}}
	got := materializacaoStats(spec, materializer.MetricsSnapshot{RegrasCasadas: []int64{1}})
	if got.TTLRules[0].ExpireAfterWrite != schema.DuracaoNunca {
		t.Errorf("duração = %q, quero %q", got.TTLRules[0].ExpireAfterWrite, schema.DuracaoNunca)
	}
}

// Dataset sem regra nenhuma não publica lista vazia nem contagem: o bloco sai
// com os contadores das políticas de escrita e nada de TTL.
func TestMaterializacaoStatsSemRegrasNaoPublicaTTL(t *testing.T) {
	got := materializacaoStats(&schema.DatasetSpec{Name: "d"}, materializer.MetricsSnapshot{Acumuladas: 2})
	if len(got.TTLRules) != 0 {
		t.Errorf("regras = %+v, quero nenhuma", got.TTLRules)
	}
	if got.Summed != 2 {
		t.Errorf("soma = %d", got.Summed)
	}
}

func TestAmostrasDeMaterializacaoPublicamAsSeries(t *testing.T) {
	snap := materializer.MetricsSnapshot{
		CongeladasCapturadas:  7,
		CongeladasPreservadas: 5,
		Acumuladas:            9,
		Saturadas:             1,
		RegrasCasadas:         []int64{4, 2},
		SemRegra:              3,
	}
	amostras := amostrasDeMaterializacao("d", specComPoliticas(), snap)

	valores := map[string]float64{}
	for _, a := range amostras {
		chave := a.Name
		for _, l := range a.Labels {
			if l.Name == "rule" {
				chave += "{rule=" + l.Value + "}"
			}
		}
		valores[chave] = a.Value
	}
	quer := map[string]float64{
		metrics.MetFrozenCaptured:              7,
		metrics.MetFrozenPreserved:             5,
		metrics.MetMergeSum:                    9,
		metrics.MetMergeSumSaturated:           1,
		metrics.MetTTLRuleMatched + "{rule=0}": 4,
		metrics.MetTTLRuleMatched + "{rule=1}": 2,
		metrics.MetTTLUnmatched:                3,
	}
	for nome, v := range quer {
		if got, ok := valores[nome]; !ok || got != v {
			t.Errorf("%s = %v (presente=%v), quero %v", nome, got, ok, v)
		}
	}
}

// O snapshot invalidado pelo teto tem de sair no wire com CÓDIGO PRÓPRIO, e é
// aqui que ele se separa do INTERNAL.
//
// A distinção não é cosmética: INTERNAL manda o operador procurar um bug do
// servidor, e não há bug nenhum — é um teto de memória sendo aplicado, com
// ação do cliente (repetir, que abre um snapshot novo) e do operador (subir
// runtime.query.snapshotMaxBytes). É a mesma razão pela qual o ROW_TOO_LARGE
// ganhou código próprio quando o orçamento de buffer passou a ser aplicado.
func TestSnapshotExpiradoSaiComCodigoProprioENaoComoInterno(t *testing.T) {
	// O erro chega embrulhado, como chega de verdade: o executor o propaga
	// pelo it.Err() depois de o storage o devolver na leitura de uma row.
	err := fmt.Errorf("executor: leitura da row 42: %w", storage.ErrSnapshotExpirado)

	if got := codigoDeErroDeQuery(err); got != protocol.CodeSnapshotExpired {
		t.Errorf("codigoDeErroDeQuery = %q, quero %q", got, protocol.CodeSnapshotExpired)
	}
	// A contraprova: um erro qualquer continua caindo em INTERNAL. Sem ela, um
	// mapeamento que devolvesse SNAPSHOT_TOO_OLD para tudo passaria acima.
	if got := codigoDeErroDeQuery(errors.New("qualquer outra coisa")); got != protocol.CodeInternal {
		t.Errorf("codigoDeErroDeQuery de um erro genérico = %q, quero %q", got, protocol.CodeInternal)
	}
}
