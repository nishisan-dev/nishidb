package server_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/protocolo"
	"github.com/nishisan-dev/nishidb/internal/server"
)

const specComHistorico = `version: 1
name: eventos
materialization:
  key:
    field: id
history:
  expireAfterWrite: 7d
  maxBytes: 4GiB
  fields:
    id:
      index: hash
schema:
  include: ["*"]
  fields:
    id:
      type: string
    severity:
      type: int64
`

func servidorComHistorico(t *testing.T, spec string) *server.Servidor {
	t.Helper()
	a := novoAmbienteCom(t, blocoPadrao)
	a.escreveDataset("eventos", spec)
	srv, err := server.Novo(a.carrega(), server.Opcoes{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	roda(t, srv)
	return srv
}

// A tabela irmã é um dataset de pleno direito para o /v1/metadata, e é isso
// que a faz aparecer na árvore do DBeaver sem nenhuma mudança no driver.
func TestATabelaIrmaApareceNoMetadata(t *testing.T) {
	srv := servidorComHistorico(t, specComHistorico)
	meta := leJSON[protocolo.Metadados](t, srv, "/v1/metadata")
	nomes := map[string]bool{}
	for _, d := range meta.Datasets {
		nomes[d.Name] = true
	}
	if !nomes["eventos"] {
		t.Fatalf("o dataset base sumiu: %v", nomes)
	}
	if !nomes["eventos_history"] {
		t.Fatalf("a tabela irmã não aparece: %v", nomes)
	}
}

// A consulta à irmã sai de graça: ela é registrada no motor sob o próprio
// nome, e o binder confere o nome do FROM contra o Compiled dela.
func TestConsultaNaTabelaIrmaFunciona(t *testing.T) {
	srv := servidorComHistorico(t, specComHistorico)
	resp := consulta(t, srv, `{"dataset":"eventos_history","sql":"SELECT id FROM eventos_history"}`)
	defer fecha(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, quer 200", resp.StatusCode)
	}
}

// As duas tabelas têm as MESMAS colunas: é o dicionário compartilhado que
// garante isso, e é o requisito que a feature existe para cumprir.
func TestAsDuasTabelasTemAsMesmasColunas(t *testing.T) {
	srv := servidorComHistorico(t, specComHistorico)
	meta := leJSON[protocolo.Metadados](t, srv, "/v1/metadata")
	colunas := map[string][]string{}
	for _, d := range meta.Datasets {
		for _, c := range d.Columns {
			colunas[d.Name] = append(colunas[d.Name], c.Name+":"+c.Type)
		}
	}
	base, irma := colunas["eventos"], colunas["eventos_history"]
	if len(base) == 0 {
		t.Fatal("o base não publicou coluna nenhuma")
	}
	if strings.Join(base, ",") != strings.Join(irma, ",") {
		t.Fatalf("colunas divergem:\n base = %v\n irmã = %v", base, irma)
	}
}

// A irmã não tem key: sem primary não há dedup, que é a definição da tabela.
// É essa ausência que impede o planner de eleger o point lookup sobre um
// Store que não tem contra o que resolvê-lo.
func TestATabelaIrmaNaoDeclaraKey(t *testing.T) {
	srv := servidorComHistorico(t, specComHistorico)
	meta := leJSON[protocolo.Metadados](t, srv, "/v1/metadata")
	for _, d := range meta.Datasets {
		if d.Name != "eventos_history" {
			continue
		}
		if d.Key != "" {
			t.Fatalf("a irmã declara key %q", d.Key)
		}
		return
	}
	t.Fatal("a irmã não aparece no metadata")
}

// Os dois disputariam o mesmo diretório, com dois writers, dois contadores de
// rowId e dois manifests. A recusa vem ANTES de abrir qualquer coisa.
func TestNomeDaIrmaQueColideComDatasetDeclaradoRecusaASubida(t *testing.T) {
	a := novoAmbienteCom(t, blocoPadrao)
	a.escreveDataset("eventos", specComHistorico)
	a.escreveDataset("eventos_history", strings.ReplaceAll(specSemKafka, "%s", "eventos_history"))
	if _, err := server.Novo(a.carrega(), server.Opcoes{}); err == nil {
		t.Fatal("a subida foi aceita com a colisão de nome")
	} else if !strings.Contains(err.Error(), "eventos_history") {
		t.Fatalf("a mensagem precisa nomear a tabela: %v", err)
	}
}

// Sem o bloco `history` nada muda: o dataset sobe sozinho, como sempre.
func TestSemBlocoHistoryNaoHaTabelaIrma(t *testing.T) {
	srv := servidorComHistorico(t, strings.ReplaceAll(specSemKafka, "%s", "eventos"))
	meta := leJSON[protocolo.Metadados](t, srv, "/v1/metadata")
	if len(meta.Datasets) != 1 {
		t.Fatalf("%d datasets, quer 1: %+v", len(meta.Datasets), meta.Datasets)
	}
}
