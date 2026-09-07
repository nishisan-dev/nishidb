package versions

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage/rowindex"
)

// v0 e v1 são duas locations no MESMO segmento: o que muda entre as versões de
// uma linha é o offset, porque o record novo é appendado adiante.
var (
	v0 = rowindex.Pack(3, 100, 0)
	v1 = rowindex.Pack(3, 200, 0)
)

// A REGRA DE BUSCA é o coração do pacote, e é onde um erro passaria
// despercebido: ao publicar a versão nova de R na época P, o que se guarda é a
// location ANTERIOR. Quem lê com época E quer a MENOR época registrada maior
// que E — é ela que carrega o estado vigente em E.
//
// Pegar a maior, ou a primeira encontrada, devolve a versão errada em toda
// linha que mudou duas vezes durante a mesma consulta.
func TestBuscaDevolveAVersaoVigenteNaEpocaENaoAIntermediaria(t *testing.T) {
	c := New()
	// A linha 7 nasce como v0, vira v1 na época 10 e v2 na época 20.
	c.Retain(7, v0, 10) // ao publicar v1, a anterior era v0
	c.Retain(7, v1, 20) // ao publicar v2, a anterior era v1

	cases := []struct {
		epoch  uint64
		want   uint64
		reason string
	}{
		{5, v0, "quem abriu antes das duas atualizações tem de ver v0"},
		{10, v1, "quem abriu na época 10 já viu v1: só a atualização da 20 é futura"},
		{15, v1, "entre as duas atualizações vale v1"},
		{20, 0, "quem abriu na época 20 já viu v2, que é a location CORRENTE"},
		{99, 0, "depois de tudo, nada a restaurar"},
	}
	for _, k := range cases {
		got, found := c.At(7, k.epoch)
		if k.want == 0 {
			if found {
				t.Errorf("At(7, %d) = %#x, quero achou=false: %s", k.epoch, got, k.reason)
			}
			continue
		}
		if !found || got != k.want {
			t.Errorf("At(7, %d) = (%#x, %v), quero %#x: %s", k.epoch, got, found, k.want, k.reason)
		}
	}
}

// A poda só pode remover o que NENHUM leitor alcança: uma entrada de época P
// interessa a todo leitor com época < P.
func TestPodaRemoveSoOQueNenhumLeitorAlcanca(t *testing.T) {
	c := New()
	c.Retain(7, v0, 10)
	c.Retain(7, v1, 20)

	c.Prune(15) // ainda há leitor na época 15: a entrada da 20 tem de ficar
	if _, found := c.At(7, 15); !found {
		t.Error("a poda removeu a versão que o leitor da época 15 ainda precisa")
	}
	// O observável da entrada que SAIU é a contagem, e não uma busca: depois
	// da poda não existe mais leitor na época 5, então At(7, 5) é uma pergunta
	// que ninguém faz — e ela devolveria a entrada da 20, que continua certa
	// para quem quer que ainda esteja atrás dela.
	if got := c.Entries(); got != 1 {
		t.Errorf("Entries() = %d, quero 1: a entrada da época 10 sobreviveu a uma poda que nenhum leitor alcança", got)
	}

	c.Prune(NoReader)
	if got := c.Entries(); got != 0 {
		t.Errorf("Entries() = %d, quero 0: sem leitor nenhum o journal tem de esvaziar", got)
	}
}

// O pin existe para que a compaction não aposente o segmento onde uma versão
// retida MORA — e ele tem de cair quando a versão sai.
func TestPinSegueAVersaoRetidaEnaoOSegmentoCorrente(t *testing.T) {
	c := New()
	c.Retain(7, v0, 10)

	if !c.Pinned(3) {
		t.Error("o segmento 3 guarda uma versão retida e não está pinado")
	}
	if c.Pinned(4) {
		t.Error("o segmento 4 não guarda versão nenhuma e apareceu pinado")
	}
	c.Prune(NoReader)
	if c.Pinned(3) {
		t.Error("o pin sobreviveu à poda da última versão que o segurava")
	}
}
