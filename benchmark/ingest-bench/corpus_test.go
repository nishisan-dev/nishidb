package main

import (
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/testutil/radiusgen"
)

// O corpus da rodada de aceite é a EVIDÊNCIA que o documento de aceite cita.
// A primeira rodada de 8M montou o gerador do LOAD com uma Config literal
// parcial: os ratios ficaram em zero, o corpus virou 8 milhões de eventos
// completos sem um único patch, e o critério de aceite continuou afirmando 85%
// de patch parcial. Verde, rápido, e provando outra coisa.
//
// Este teste é o que detecta essa erosão: o perfil registrado no artefato tem
// de ser o workload de referência, não o que sobrou de uma Config incompleta.
func TestCorpusDoLoadTemOsRatiosDeReferencia(t *testing.T) {
	rep, err := Run(baseConfig(t, 200))
	if err != nil {
		t.Fatal(err)
	}
	ref := radiusgen.DefaultConfig()

	got := rep.Config.LoadCorpus
	if got.PatchRatio != ref.PatchRatio {
		t.Errorf("PatchRatio do LOAD = %v, want %v (corpus sem patch não exercita merge)",
			got.PatchRatio, ref.PatchRatio)
	}
	if got.ExtendedJSONRatio != ref.ExtendedJSONRatio {
		t.Errorf("ExtendedJSONRatio = %v, want %v", got.ExtendedJSONRatio, ref.ExtendedJSONRatio)
	}
	if got.UnknownFieldRatio != ref.UnknownFieldRatio {
		t.Errorf("UnknownFieldRatio = %v, want %v", got.UnknownFieldRatio, ref.UnknownFieldRatio)
	}
	if got.NullFieldRatio != ref.NullFieldRatio {
		t.Errorf("NullFieldRatio = %v, want %v", got.NullFieldRatio, ref.NullFieldRatio)
	}
	// Sequential continua ligado: é o que faz N eventos criarem N entidades.
	if !got.Sequential {
		t.Error("o LOAD deixou de ser sequencial: N eventos não criam mais N rows")
	}
}

// O perfil precisa chegar ao ARTEFATO, não só à execução: é o JSON que a
// revisão audita quando confere se a evidência citada existe.
func TestPerfilDoCorpusVaiParaOArtefato(t *testing.T) {
	cfg := baseConfig(t, 100)
	cfg.SteadyDuration = 300 * time.Millisecond
	cfg.SteadyRate = 50
	rep, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Config.LoadCorpus.PatchRatio == 0 {
		t.Error("o artefato não registra o perfil do corpus do LOAD")
	}
	if rep.Config.SteadyCorpus.PatchRatio == 0 {
		t.Error("o artefato não registra o perfil do corpus do STEADY")
	}
	// O STEADY sorteia entidade (carga de regime); o LOAD percorre em ordem.
	if rep.Config.SteadyCorpus.Sequential {
		t.Error("o STEADY virou sequencial: a carga de regime perde a localidade irregular")
	}
}
