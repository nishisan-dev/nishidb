package server_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/server"
)

// serverWithTwoSchemas sobe o retrato mínimo em que a conexão por schema pode
// ser PROVADA: dois datasets em schemas distintos, e um deles com a irmã do
// histórico — o segundo dataset do mesmo schema, que o recorte tem de trazer.
// Com um schema só, "recortou pelo schema" e "devolveu tudo" dão a mesma
// resposta.
func serverWithTwoSchemas(t *testing.T) *server.Server {
	t.Helper()
	a := newEnvWith(t, blockPadrao)
	a.writeDataset("eventos", specWithHistory)
	a.writeDataset("servicos", fmt.Sprintf(specWithoutKafka, "servicos", "servicos"))
	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	run(t, srv)
	return srv
}

// connectionSchemas são os dois schemas do serverWithTwoSchemas.
var connectionSchemas = []string{"schema_eventos", "schema_servicos"}

func queryBody(schemaName, sql string) string {
	b, _ := json.Marshal(map[string]string{"schema": schemaName, "sql": sql})
	return string(b)
}

// drain consome a resposta e devolve o código de erro, vazio quando a consulta
// abriu (200 com a linha de fecho).
func drain(t *testing.T, resp *http.Response) string {
	t.Helper()
	if resp.StatusCode != http.StatusOK {
		return failOf(t, resp).Code
	}
	_, reader := result(t, resp)
	if reader.End() == nil {
		t.Fatal("o stream não trouxe a linha de fecho")
	}
	return ""
}

// A árvore da conexão e o SQL da conexão dizem a MESMA coisa. É a promessa do
// Catalog na escala da conexão por schema, cobrada pelos dois lados:
//
//   - tudo o que o /v1/metadata?schema=S publica no schema S abre pela conexão
//     a S, pelo endereço qualificado E pelo nome simples — e as linhas do
//     `_meta` abrem pelo atalho (a árvore que mostra o que a consulta recusa);
//   - o recorte só traz o schema S, e toda tabela de OUTRO schema é recusada
//     pela conexão a S, pelos dois endereços (a consulta que abre o que a
//     árvore esconde — o falso negativo da issue #57).
//
// As tabelas dos outros schemas saem do /v1/metadata SEM filtro, e não de uma
// lista escrita aqui: um nome digitado que não existe seria recusado com o
// motor aberto a qualquer schema, e o teste passaria sem provar nada.
func TestTheConnectionTreeAndTheConnectionSqlAgree(t *testing.T) {
	srv := serverWithTwoSchemas(t)
	all := readJSON[protocol.Metadata](t, srv, "/v1/metadata")

	for _, s := range connectionSchemas {
		tree := readJSON[protocol.Metadata](t, srv, "/v1/metadata?schema="+s)
		opened := 0
		for _, d := range tree.Datasets {
			if d.Schema != s {
				t.Errorf("conexão %s: a árvore traz o dataset %s do schema %s", s, d.Name, d.Schema)
			}
			for _, tab := range d.Tables {
				if strings.HasPrefix(tab.Schema, "_") {
					sql := "SELECT * FROM " + tab.Namespace + "." + tab.Name + " LIMIT 1"
					if code := drain(t, consulta(t, srv, queryBody(s, sql))); code != "" && code != protocol.CodeForbidden {
						t.Errorf("conexão %s: a árvore lista %s.%s e o atalho recusa com %s", s, tab.Namespace, tab.Name, code)
					}
					continue
				}
				if tab.Schema != s {
					t.Errorf("conexão %s: a árvore traz a tabela %s.%s", s, tab.Schema, tab.Name)
					continue
				}
				for _, sql := range []string{
					"SELECT * FROM " + s + "." + tab.Name + " LIMIT 1",
					"SELECT * FROM " + tab.Name + " LIMIT 1",
				} {
					if code := drain(t, consulta(t, srv, queryBody(s, sql))); code != "" {
						t.Errorf("conexão %s: a árvore lista %s e %q recusa com %s", s, tab.Name, sql, code)
					}
				}
				opened++
			}
		}
		if opened == 0 {
			t.Fatalf("conexão %s: a árvore não trouxe tabela nenhuma do schema; o oráculo não distingue nada", s)
		}

		refused := 0
		for _, d := range all.Datasets {
			if d.Schema == s {
				continue
			}
			for _, tab := range d.Tables {
				if strings.HasPrefix(tab.Schema, "_") {
					continue
				}
				for _, sql := range []string{
					"SELECT * FROM " + tab.Schema + "." + tab.Name + " LIMIT 1",
					"SELECT * FROM " + tab.Name + " LIMIT 1",
				} {
					if code := drain(t, consulta(t, srv, queryBody(s, sql))); code != protocol.CodeBind {
						t.Errorf("conexão %s: %q fora da árvore respondeu %q, quer %s", s, sql, code, protocol.CodeBind)
					}
				}
				refused++
			}
		}
		if refused == 0 {
			t.Fatalf("conexão %s: nenhuma tabela de outro schema; o lado da recusa não foi exercitado", s)
		}
	}
}

// O recorte traz o base e a irmã, nessa ordem — o driver lê os limites da
// conexão do PRIMEIRO elemento, e ele tem de ser o base.
func TestMetadataBySchemaListsTheBaseBeforeTheSister(t *testing.T) {
	srv := serverWithTwoSchemas(t)
	m := readJSON[protocol.Metadata](t, srv, "/v1/metadata?schema=schema_eventos")
	var names []string
	for _, d := range m.Datasets {
		names = append(names, d.Name)
	}
	if got := strings.Join(names, ","); got != "eventos,eventos_history" {
		t.Fatalf("?schema=schema_eventos trouxe %s, quer eventos,eventos_history", got)
	}
}

// A URL que nomeava o DATASET antes da conexão por schema cai no UNKNOWN_SCHEMA
// com a mensagem que ensina o schema — nas duas rotas, e também pelo nome da
// irmã, que herda o schema do base. O nome que não é nada lista os schemas.
func TestUnknownSchemaTeachesTheSchemaOfADatasetName(t *testing.T) {
	srv := serverWithTwoSchemas(t)
	cases := []struct {
		name, want string
	}{
		{"eventos", `use "schema_eventos"`},
		{"eventos_history", `use "schema_eventos"`},
		{"fantasma", "schema_eventos, schema_servicos"},
	}
	for _, c := range cases {
		resp, err := http.Get("http://" + srv.Endereco() + "/v1/metadata?schema=" + c.name)
		if err != nil {
			t.Fatal(err)
		}
		checkUnknownSchema(t, "/v1/metadata?schema="+c.name, resp, c.want)

		checkUnknownSchema(t, "/v1/query schema="+c.name,
			consulta(t, srv, queryBody(c.name, "SELECT id FROM "+c.name)), c.want)
	}
}

func checkUnknownSchema(t *testing.T, route string, resp *http.Response, want string) {
	t.Helper()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("%s: status %d, quer 404", route, resp.StatusCode)
	}
	fail := failOf(t, resp)
	if fail.Code != protocol.CodeUnknownSchema {
		t.Errorf("%s: código %s, quer %s", route, fail.Code, protocol.CodeUnknownSchema)
	}
	if !strings.Contains(fail.Message, want) {
		t.Errorf("%s: a mensagem não contém %q: %s", route, want, fail.Message)
	}
}

// schema e dataset juntos são recusados nas duas rotas: o servidor não escolhe
// em silêncio qual dos dois endereços o cliente quis.
func TestSchemaAndDatasetAreExclusive(t *testing.T) {
	srv := serverWithTwoSchemas(t)

	resp := consulta(t, srv, `{"schema":"schema_eventos","dataset":"eventos","sql":"SELECT id FROM eventos"}`)
	if resp.StatusCode != http.StatusBadRequest || failOf(t, resp).Code != protocol.CodeBadRequest {
		t.Errorf("/v1/query com schema e dataset: status %d", resp.StatusCode)
	}

	resp = consulta(t, srv, `{"sql":"SELECT id FROM eventos"}`)
	if resp.StatusCode != http.StatusBadRequest || failOf(t, resp).Code != protocol.CodeBadRequest {
		t.Errorf("/v1/query sem schema nem dataset: status %d", resp.StatusCode)
	}

	resp, err := http.Get("http://" + srv.Endereco() + "/v1/metadata?schema=schema_eventos&dataset=eventos")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusBadRequest || failOf(t, resp).Code != protocol.CodeBadRequest {
		t.Errorf("/v1/metadata com ?schema= e ?dataset=: status %d", resp.StatusCode)
	}
}
