package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// DefaultsDeQuery é a costura entre o nishidb.yaml e o schema: é por ela que a
// chave global chega ao dataset. O teto tem teste fim-a-fim pelo binário
// (o truncamento é observável no exit code); o TIMEOUT é medido aqui, na
// costura, e não pelo relógio.
//
// A razão é deliberada: um teste que dependesse de um deadline de nanossegundos
// vencer antes da primeira checagem do executor seria intermitente por
// construção. A cadeia já está fechada sem isso — daqui até spec.Query.Timeout
// é TestDatasetSemQueryHerdaOsDefaultsGlobais (internal/schema), e de
// spec.Query.Timeout até o cancelamento da consulta é TestTimeoutDoDataset
// (internal/query), que usa atraso injetado em vez de esperar relógio.

func escreveConfig(t *testing.T, corpo string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "nishidb.yaml")
	cabecalho := "version: 1\nstorage:\n  path: " + dir +
		"\ndatasets:\n  directory: " + dir + "\n"
	if err := os.WriteFile(path, []byte(cabecalho+corpo), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDefaultsDeQueryCarregaAsChavesGlobais(t *testing.T) {
	path := escreveConfig(t, "query:\n  defaultMaxRows: 4242\n  defaultTimeout: 7s\n")
	c, err := CarregaConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	d := c.DefaultsDeQuery()
	if d.MaxRows != 4242 {
		t.Errorf("MaxRows = %d, want 4242", d.MaxRows)
	}
	if d.Timeout != 7*time.Second {
		t.Errorf("Timeout = %s, want 7s", d.Timeout)
	}
}

func TestDefaultsDeQuerySemBlocoUsaOsPadroes(t *testing.T) {
	c, err := CarregaConfig(escreveConfig(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	d := c.DefaultsDeQuery()
	if d.MaxRows != maxRowsPadrao || d.Timeout != timeoutPadrao {
		t.Errorf("DefaultsDeQuery() = %+v, want {%d %s}", d, maxRowsPadrao, timeoutPadrao)
	}
}

// Valor não positivo é recusado com a chave nomeada, em vez de virar "ausente".
func TestDefaultMaxRowsNaoPositivoFalhaNoCarregamento(t *testing.T) {
	for _, valor := range []string{"0", "-1", "-100000"} {
		t.Run(valor, func(t *testing.T) {
			_, err := CarregaConfig(escreveConfig(t, "query:\n  defaultMaxRows: "+valor+"\n"))
			if err == nil {
				t.Fatalf("defaultMaxRows: %s foi aceito", valor)
			}
			if !strings.Contains(err.Error(), "query.defaultMaxRows") ||
				!strings.Contains(err.Error(), valor) {
				t.Errorf("erro = %v, want nomear a chave e o valor", err)
			}
		})
	}
}
