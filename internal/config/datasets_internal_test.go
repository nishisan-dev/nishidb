package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// datasetYAML é a spec mínima de um dataset, parametrizada só no que estes
// testes variam: o nome e o schema. Tudo o mais é o resto OBRIGATÓRIO — um
// caso de recusa que falhasse no meio do arquivo mentiria sobre a causa.
const datasetYAML = `version: 1
name: %s
schemaName: %s
materialization:
  key:
    field: id
schema:
  fields:
    id:
      type: string
`

// configWithDatasets escreve um nishidb.yaml cujo datasets.directory é um
// diretório À PARTE, e devolve a Config carregada.
//
// O diretório separado não é detalhe: o ListDatasets conta todo `*.yaml` do
// diretório como dataset, e apontar datasets.directory para a pasta do
// próprio nishidb.yaml faria a configuração entrar na conta como se fosse um
// dataset — o teste passaria a medir outra coisa.
func configWithDatasets(t *testing.T, specsBySchema map[string]string) *Config {
	t.Helper()
	root := t.TempDir()
	datasetsDir := filepath.Join(root, "datasets")
	if err := os.MkdirAll(datasetsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, schemaName := range specsBySchema {
		body := fmt.Sprintf(datasetYAML, name, schemaName)
		if err := os.WriteFile(filepath.Join(datasetsDir, name+".yaml"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(root, "nishidb.yaml")
	body := "version: 1\nstorage:\n  path: " + root + "\ndatasets:\n  directory: " + datasetsDir + "\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatalf("carregar a configuração: %v", err)
	}
	return c
}

// TestDoisDatasetsNoMesmoSchemaSaoRecusados: o schema é o que identifica o
// dataset num `FROM telecom.tabela`. Repetido, a mesma consulta responderia de
// um dataset ou de outro conforme a ordem de carga — e a ordem de carga não é
// contrato.
//
// O assert cobra os TRÊS nomes — o schema e os dois datasets — porque recusa
// que não ensina a correção só transfere o trabalho: quem lê o log tem de
// saber qual chave mudar e em qual dos dois arquivos.
func TestDoisDatasetsNoMesmoSchemaSaoRecusados(t *testing.T) {
	c := configWithDatasets(t, map[string]string{
		"servicos": "telecom",
		"eventos":  "telecom",
	})

	_, err := c.LoadDatasetSpecs()
	if err == nil {
		t.Fatal("schema repetido entre datasets tem de ser recusado")
	}
	for _, quero := range []string{"telecom", "servicos", "eventos"} {
		if !strings.Contains(err.Error(), quero) {
			t.Errorf("a mensagem precisa nomear o schema E os DOIS datasets, e não cita %q: %v", quero, err)
		}
	}
}

// TestDatasetsComSchemasProprioSobem é a contrapartida: a recusa tem de ser da
// REPETIÇÃO, e não de haver mais de um dataset no diretório. Sem este caso, uma
// implementação que recusasse o segundo dataset sempre passaria no teste acima.
func TestDatasetsComSchemasProprioSobem(t *testing.T) {
	c := configWithDatasets(t, map[string]string{
		"servicos": "telecom",
		"eventos":  "auditoria",
	})

	specs, err := c.LoadDatasetSpecs()
	if err != nil {
		t.Fatalf("dois datasets com schemas próprios têm de subir: %v", err)
	}
	if len(specs) != 2 {
		t.Fatalf("specs carregadas = %d, quer 2", len(specs))
	}
	if specs["servicos"].SchemaName != "telecom" || specs["eventos"].SchemaName != "auditoria" {
		t.Errorf("as specs voltaram com os schemas trocados: %q e %q",
			specs["servicos"].SchemaName, specs["eventos"].SchemaName)
	}
}

// TestATabelaIrmaDoHistoricoNaoContaComoSegundoDataset fecha a pergunta que a
// herança do schemaName levanta: a irmã do histórico recebe o MESMO schema do
// dataset base (schema.DeriveHistory), e se ela entrasse na conta da unicidade
// todo dataset com bloco `history` deixaria de subir.
//
// O que faz a irmã ficar de fora não é uma exceção escrita na validação: é o
// mapa vir do ListDatasets, que enumera ARQUIVOS, e a irmã não ter YAML
// próprio. O teste amarra essa consequência para que ela não se perca no dia
// em que alguém alimentar o mapa por outra via.
func TestATabelaIrmaDoHistoricoNaoContaComoSegundoDataset(t *testing.T) {
	root := t.TempDir()
	datasetsDir := filepath.Join(root, "datasets")
	if err := os.MkdirAll(datasetsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := fmt.Sprintf(datasetYAML, "eventos", "telecom") + "history:\n  expireAfterWrite: 7d\n"
	if err := os.WriteFile(filepath.Join(datasetsDir, "eventos.yaml"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "nishidb.yaml")
	cfgBody := "version: 1\nstorage:\n  path: " + root + "\ndatasets:\n  directory: " + datasetsDir + "\n"
	if err := os.WriteFile(path, []byte(cfgBody), 0o644); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatalf("carregar a configuração: %v", err)
	}

	specs, err := c.LoadDatasetSpecs()
	if err != nil {
		t.Fatalf("um dataset com tabela histórica tem de subir: %v", err)
	}
	base := specs["eventos"]
	if base == nil {
		t.Fatal("o dataset base não foi carregado")
	}
	if base.History == nil {
		t.Fatal("o fixture perdeu o bloco `history`: sem ele o teste não exerce a herança do schema")
	}
	sister := base.DeriveHistory()
	if sister.SchemaName != base.SchemaName {
		t.Fatalf("a irmã deixou de herdar o schema do base (%q vs %q) — o teste não prova mais nada",
			sister.SchemaName, base.SchemaName)
	}
	if _, listed := specs[sister.Name]; listed {
		t.Errorf("a tabela irmã %q entrou no mapa de datasets: ela herda o schema do base e viraria uma colisão",
			sister.Name)
	}
}
