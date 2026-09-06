package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// Um gate DURO que não pôde ser AVALIADO não é um gate aprovado.
//
// O artefato dizia o contrário: sem -heap-baseline, o §110 saía com
// "skipped": true e "passed": true, e a rodada inteira saía "passed": true —
// exatamente a leitura que o gate existe para impedir. As rodadas de 1M
// arquivadas em benchmark/baselines mostram o efeito: heap_decoupled pulado e
// a rodada aprovada.
//
// O que a correção faz: o veredito de um gate passou a ter TRÊS estados, e a
// rodada só é aprovada quando todo gate duro foi avaliado E passou.
func TestGateDuroPuladoNaoEhGateAprovado(t *testing.T) {
	rep, err := Run(baseConfig(t, 100)) // sem HeapBaseline: o §110 não tem o que comparar
	if err != nil {
		t.Fatal(err)
	}
	g := gateByName(t, rep, "heap_decoupled")
	if !g.Hard {
		t.Fatal("o §110 precisa continuar sendo gate DURO para este teste significar algo")
	}
	if g.Status != GateNaoAvaliado {
		t.Errorf("Status = %q, want %q", g.Status, GateNaoAvaliado)
	}
	if g.Status == GatePassou {
		t.Error("gate duro NÃO AVALIADO reportado como aprovado")
	}
	if rep.Passed {
		t.Error("rodada aprovada com gate duro não avaliado: " +
			"\"não medi\" não pode ser lido como \"está bom\"")
	}
}

// O artefato é o que sobrevive à rodada, e é nele que a leitura errada se
// materializa: um leitor de JSON que procurasse "passed": true no gate
// encontraria aprovação onde não houve medição. O estado agora é um campo só,
// com três valores — não dá para representar "pulado e aprovado".
func TestArtefatoNaoRepresentaGatePuladoComoAprovado(t *testing.T) {
	rep, err := Run(baseConfig(t, 100))
	if err != nil {
		t.Fatal(err)
	}
	bruto, err := json.Marshal(rep)
	if err != nil {
		t.Fatal(err)
	}
	var lido struct {
		Passed bool `json:"passed"`
		Gates  []struct {
			Name   string `json:"name"`
			Hard   bool   `json:"hard"`
			Status string `json:"status"`
			Passed *bool  `json:"passed"`
		} `json:"gates"`
	}
	if err := json.Unmarshal(bruto, &lido); err != nil {
		t.Fatal(err)
	}
	if lido.Passed {
		t.Error(`artefato com "passed": true e gate duro não avaliado`)
	}
	var achou bool
	for _, g := range lido.Gates {
		if g.Passed != nil {
			t.Errorf("gate %q ainda tem o booleano \"passed\": ele é o campo que "+
				"permitia dizer pulado E aprovado ao mesmo tempo", g.Name)
		}
		if g.Name != "heap_decoupled" {
			continue
		}
		achou = true
		if g.Status != string(GateNaoAvaliado) {
			t.Errorf("status do heap_decoupled no artefato = %q, want %q", g.Status, GateNaoAvaliado)
		}
	}
	if !achou {
		t.Fatal("heap_decoupled ausente do artefato")
	}
}

// O relatório impresso precisa separar as duas reprovações, senão quem roda a
// rodada de referência (que reprova por construção, porque não tem baseline)
// vai procurar um gate violado que não existe.
func TestResumoDeGatesSeparaVioladoDeNaoAvaliado(t *testing.T) {
	gates := []GateResult{
		{Name: "duro_ok", Hard: true, Status: GatePassou},
		{Name: "duro_violado", Hard: true, Status: GateReprovou},
		{Name: "duro_nao_avaliado", Hard: true, Status: GateNaoAvaliado},
		{Name: "soft_violado", Hard: false, Status: GateReprovou},
		{Name: "soft_nao_avaliado", Hard: false, Status: GateNaoAvaliado},
	}
	violados, naoAvaliados := gatesDurosPendentes(gates)
	if strings.Join(violados, ",") != "duro_violado" {
		t.Errorf("violados = %v, want [duro_violado]", violados)
	}
	if strings.Join(naoAvaliados, ",") != "duro_nao_avaliado" {
		t.Errorf("naoAvaliados = %v, want [duro_nao_avaliado]", naoAvaliados)
	}
	// Gate SOFT não avaliado não reprova rodada nenhuma: ele já não reprovava
	// quando violado.
	if aprovaRodada(gates) {
		t.Error("rodada aprovada com gate duro violado e outro não avaliado")
	}
	if !aprovaRodada(gates[:1]) {
		t.Error("rodada reprovada com o único gate duro aprovado")
	}
	// Sozinho, sem nenhum gate violado por perto: o não avaliado precisa
	// reprovar por si, senão volta a ser aprovação por omissão.
	if aprovaRodada(gates[2:3]) {
		t.Error("rodada aprovada com o único gate duro NÃO AVALIADO")
	}
	if !aprovaRodada(gates[3:]) {
		t.Error("gate SOFT violado ou não avaliado não pode reprovar a rodada")
	}
	// Rótulo por estado: o hard-skip precisa de rótulo PRÓPRIO, senão volta a
	// se confundir com aprovação ou com violação.
	rotulos := map[string]string{}
	for _, g := range gates {
		rotulos[g.Name] = rotuloDoGate(g)
	}
	esperado := map[string]string{
		"duro_ok":           "PASS",
		"duro_violado":      "FAIL",
		"duro_nao_avaliado": "SEM-AVAL",
		"soft_violado":      "WARN",
		"soft_nao_avaliado": "SKIP",
	}
	for nome, want := range esperado {
		if rotulos[nome] != want {
			t.Errorf("rótulo de %s = %q, want %q", nome, rotulos[nome], want)
		}
	}
}
