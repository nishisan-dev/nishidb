package storage_test

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

func scrape(t *testing.T, r *metrics.Registry) string {
	t.Helper()
	var sb strings.Builder
	if _, err := r.WriteTo(&sb); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	return sb.String()
}

func rowWithPrefixo(s, prefixo string) (string, bool) {
	for _, l := range strings.Split(s, "\n") {
		if strings.HasPrefix(l, prefixo) {
			return l, true
		}
	}
	return "", false
}

// O §92 exige as duas séries de checkpoint, e elas não existiam: até o M2 não
// havia sequer tipo onde morar.
func TestCheckpointPublicaDuracaoEUltimoSucesso(t *testing.T) {
	reg := metrics.NewRegistry()
	st, err := storage.Open(storage.Options{
		Dir:     t.TempDir(),
		Dataset: "radius_sessions",
		Metrics: reg,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	// O Open já faz um checkpoint imediato, então há medição antes mesmo de
	// qualquer escrita do chamador.
	s := scrape(t, reg)
	for _, prefixo := range []string{
		`nishidb_checkpoint_duration_seconds_count{dataset="radius_sessions"}`,
		`nishidb_checkpoint_last_success_timestamp{dataset="radius_sessions"}`,
	} {
		if _, ok := rowWithPrefixo(s, prefixo); !ok {
			t.Errorf("série ausente: %s\n%s", prefixo, s)
		}
	}

	before, _ := rowWithPrefixo(s, `nishidb_checkpoint_duration_seconds_count{dataset="radius_sessions"}`)
	if before != `nishidb_checkpoint_duration_seconds_count{dataset="radius_sessions"} 1` {
		t.Errorf("contagem após o Open = %q, quer 1 (o checkpoint imediato)", before)
	}

	if err := st.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	depois, _ := rowWithPrefixo(scrape(t, reg),
		`nishidb_checkpoint_duration_seconds_count{dataset="radius_sessions"}`)
	if depois != `nishidb_checkpoint_duration_seconds_count{dataset="radius_sessions"} 2` {
		t.Errorf("contagem após o segundo checkpoint = %q, quer 2", depois)
	}
}

// O relógio parado é o sinal de writer em modo falho: se um checkpoint que
// falhou atualizasse o "último sucesso", o alerta que existe justamente para
// pegar "não checkpointa há meia hora" nunca dispararia.
func TestCheckpointFalhoNaoMoveOUltimoSucesso(t *testing.T) {
	reg := metrics.NewRegistry()
	st, err := storage.Open(storage.Options{
		Dir:     t.TempDir(),
		Dataset: "d",
		Metrics: reg,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	before, ok := rowWithPrefixo(scrape(t, reg), `nishidb_checkpoint_last_success_timestamp{dataset="d"}`)
	if !ok {
		t.Fatal("série de último sucesso ausente após o Open")
	}
	if strings.HasSuffix(before, " 0") {
		t.Fatal("o checkpoint do Open deveria ter marcado o último sucesso")
	}

	// Fechar o Store faz o próximo Checkpoint falhar no entra() com
	// ErrDatasetClosed — um caminho de erro real, sem seam de teste.
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if err := st.Checkpoint(); err == nil {
		t.Fatal("Checkpoint sobre Store fechado deveria falhar")
	}

	depois, _ := rowWithPrefixo(scrape(t, reg), `nishidb_checkpoint_last_success_timestamp{dataset="d"}`)
	if depois != before {
		t.Errorf("checkpoint falho moveu o último sucesso:\n  antes: %s\n  depois: %s", before, depois)
	}
	count, _ := rowWithPrefixo(scrape(t, reg), `nishidb_checkpoint_duration_seconds_count{dataset="d"}`)
	if count != `nishidb_checkpoint_duration_seconds_count{dataset="d"} 1` {
		t.Errorf("checkpoint falho entrou no histograma: %q", count)
	}
}

// Sem registry o storage funciona igual: a instrumentação não pode ser
// condição de funcionamento.
func TestStoreSemRegistryFunciona(t *testing.T) {
	st, err := storage.Open(storage.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint sem registry falhou: %v", err)
	}
}

// Sem Dataset explícito o label vem do diretório — um scrape com
// `dataset=""` não diria de quem é a série.
func TestDatasetVazioHerdaONomeDoDiretorio(t *testing.T) {
	reg := metrics.NewRegistry()
	dir := t.TempDir() + "/radius_sessions"
	st, err := storage.Open(storage.Options{Dir: dir, Metrics: reg})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, ok := rowWithPrefixo(scrape(t, reg),
		`nishidb_checkpoint_last_success_timestamp{dataset="radius_sessions"}`); !ok {
		t.Errorf("label dataset não herdou o nome do diretório:\n%s", scrape(t, reg))
	}
}
