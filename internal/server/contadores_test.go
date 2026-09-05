package server_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/protocolo"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

// specServicos é o dataset da proposta: serviços por OLT, com dois contadores
// — o denominador (todos) e o numerador (autenticados).
const specServicos = `version: 1
name: %s
materialization:
  key:
    field: customerId
schema:
  fields:
    customerId:
      type: string
    olt:
      type: string
    slot:
      type: int64
    port:
      type: int64
    authenticated:
      type: boolean
counters:
  - name: servicos_por_olt
    groupBy: [olt]
  - name: autenticados_por_olt
    groupBy: [olt]
    where: authenticated = true
  - name: autenticados_por_pon
    groupBy: [olt, slot, port]
    where: authenticated = true
`

// semeiaServicos materializa n serviços ANTES de o servidor subir, pelo
// pipeline real — SEM os contadores: é o cenário de um dataset que ganhou
// `counters:` depois de povoado, e o servidor os constrói na abertura.
func semeiaServicos(t *testing.T, a *ambiente, nome string, n int) {
	t.Helper()
	cfg := a.carrega()
	spec, err := schema.Load(cfg.SpecDoDataset(nome), cfg.DefaultsDeQuery())
	if err != nil {
		t.Fatal(err)
	}
	st, err := storage.Open(storage.Options{
		Dir:                    cfg.DirDoDataset(nome),
		Dataset:                nome,
		SegmentSize:            cfg.SegmentSize(),
		CamposIndexadosPorNome: schema.CamposComIndice(spec),
	})
	if err != nil {
		t.Fatal(err)
	}
	compilado, err := spec.Compile(st.FieldDict())
	if err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	mat, err := materializer.New(compilado, st)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		olt := "PRCTA_G1M10"
		if i%4 == 0 {
			olt = "BR_SPOMO_OLT14"
		}
		ev := fmt.Sprintf(`{"customerId":"C-%04d","olt":%q,"slot":%d,"port":%d,"authenticated":%v}`,
			i, olt, 1+i%2, 1+i%3, i%3 != 0)
		if r := mat.Apply([]byte(ev)); r.Err != nil {
			_ = st.Close()
			t.Fatalf("evento %d: %v", i, r.Err)
		}
	}
	if err := st.Checkpoint(); err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
}

func servidorComServicos(t *testing.T, n int) *server.Servidor {
	t.Helper()
	a := novoAmbienteComStorage(t, blocoPadrao, "")
	a.escreveDataset("servicos", fmt.Sprintf(specServicos, "servicos"))
	semeiaServicos(t, a, "servicos", n)
	srv, err := server.Novo(a.carrega(), server.Opcoes{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	roda(t, srv)
	return srv
}

// TestCountersRespondePelaRotaDeQuery: a tabela do contador é servida pela
// MESMA rota de consulta — sem suporte em cliente nenhum — e o dataset que
// ganhou `counters:` depois de povoado sobe com os contadores construídos.
func TestCountersRespondePelaRotaDeQuery(t *testing.T) {
	srv := servidorComServicos(t, 24)
	// 24 serviços: 6 na OLT14 (i%4==0), 18 na G1M10; autenticados são os
	// i%3 != 0 — 16 no total: OLT14 tem i ∈ {4, 8, 16, 20} = 4, G1M10 tem 12.
	resp := consulta(t, srv, `{"dataset":"servicos","sql":"SELECT olt, total FROM _counters.autenticados_por_olt ORDER BY total ASC"}`)
	linhas, fim := resultado(t, resp)
	if fim.Fim() == nil {
		t.Fatal("o stream não trouxe a linha de fecho")
	}
	if len(linhas) != 2 {
		t.Fatalf("%d linhas, quero 2: %v", len(linhas), linhas)
	}
	if !strings.Contains(linhas[0], `"olt":"BR_SPOMO_OLT14"`) || !strings.Contains(linhas[0], `"total":4`) {
		t.Fatalf("primeira linha = %s", linhas[0])
	}
	if !strings.Contains(linhas[1], `"olt":"PRCTA_G1M10"`) || !strings.Contains(linhas[1], `"total":12`) {
		t.Fatalf("segunda linha = %s", linhas[1])
	}

	resp = consulta(t, srv, `{"dataset":"servicos","sql":"SELECT groupKey, total FROM _counters.servicos_por_olt WHERE groupKey = 'PRCTA_G1M10'"}`)
	linhas, _ = resultado(t, resp)
	if len(linhas) != 1 || !strings.Contains(linhas[0], `"total":18`) {
		t.Fatalf("denominador = %v", linhas)
	}

	resp = consulta(t, srv, `{"dataset":"servicos","sql":"SELECT olt, slot, port, total FROM _counters.autenticados_por_pon WHERE olt = 'BR_SPOMO_OLT14' ORDER BY total ASC LIMIT 3"}`)
	linhas, _ = resultado(t, resp)
	if len(linhas) == 0 {
		t.Fatal("nenhuma PON da OLT14")
	}
	for _, l := range linhas {
		if !strings.Contains(l, `"slot":`) || !strings.Contains(l, `"port":`) {
			t.Fatalf("linha sem as colunas tipadas: %s", l)
		}
	}
}

// TestMetaCountersListaPelaRotaDeQuery: a lista dos contadores sai por SQL,
// pela mesma rota, com a definição e os números vivos.
func TestMetaCountersListaPelaRotaDeQuery(t *testing.T) {
	srv := servidorComServicos(t, 24)
	resp := consulta(t, srv, `{"dataset":"servicos","sql":"SELECT name, groupBy, condition, groups, auditAt FROM _meta.counters ORDER BY name"}`)
	linhas, fim := resultado(t, resp)
	if fim.Fim() == nil {
		t.Fatal("o stream não trouxe a linha de fecho")
	}
	if len(linhas) != 3 {
		t.Fatalf("%d linhas, quero 3: %v", len(linhas), linhas)
	}
	for _, quer := range []string{
		`"name":"autenticados_por_olt","groupBy":"olt","condition":"authenticated = true","groups":2,"auditAt":null`,
		`"name":"autenticados_por_pon","groupBy":"olt|slot|port"`,
		`"name":"servicos_por_olt","groupBy":"olt","condition":null`,
	} {
		if !strings.Contains(strings.Join(linhas, "\n"), quer) {
			t.Fatalf("faltou %s em %v", quer, linhas)
		}
	}
}

// TestCountersTabelaDesconhecidaNomeiaAsQueExistem cobre a mensagem E o
// CÓDIGO. A asserção de código nasceu depois, com a issue #35: enquanto o teste
// só exigia `status != 200`, ele passava verde com a resposta errada — 500 e
// INTERNAL —, que foi o que o piloto viu em 05/09/2026.
func TestCountersTabelaDesconhecidaNomeiaAsQueExistem(t *testing.T) {
	srv := servidorComServicos(t, 4)
	resp := consulta(t, srv, `{"dataset":"servicos","sql":"SELECT a FROM _counters.nao_existe"}`)
	defer fecha(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d, quero 400: tabela desconhecida é erro do cliente", resp.StatusCode)
	}
	f := falhaDe(t, resp)
	if f.Code != protocolo.CodBind {
		t.Fatalf("código %q, quero %q: a série de erro interno não pode subir por um nome digitado errado", f.Code, protocolo.CodBind)
	}
	if !strings.Contains(f.Message, "servicos_por_olt") {
		t.Fatalf("a recusa tem de listar as tabelas que existem: %s", f.Message)
	}
}

// TestTabelaVirtualDesconhecidaSaiComoErroDoCliente cobre os outros dois
// caminhos de recusa de roteamento pela rota de consulta: o `_meta.nada` que o
// piloto consultou e o namespace que não existe. Os três — com o
// `_counters.nao_existe` do teste acima — são a mesma família.
func TestTabelaVirtualDesconhecidaSaiComoErroDoCliente(t *testing.T) {
	srv := servidorComServicos(t, 4)
	casos := []struct {
		nome  string
		sql   string
		naMsg string
	}{
		{"tabela do _meta", "SELECT a FROM _meta.nada", "existem: counters, fields"},
		{"namespace", "SELECT a FROM _outro.coisa", "_counters"},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			resp := consulta(t, srv, `{"dataset":"servicos","sql":"`+c.sql+`"}`)
			defer fecha(t, resp)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status %d, quero 400", resp.StatusCode)
			}
			f := falhaDe(t, resp)
			if f.Code != protocolo.CodBind {
				t.Fatalf("código %q, quero %q", f.Code, protocolo.CodBind)
			}
			if !strings.Contains(f.Message, c.naMsg) {
				t.Fatalf("a recusa tem de nomear o que existe (%q): %s", c.naMsg, f.Message)
			}
			// A posição vem do DatasetPos e é o que o cliente destaca.
			if f.Line != 1 || f.Column <= 0 {
				t.Fatalf("a recusa tem de trazer linha e coluna do trecho ofensor: linha %d, coluna %d", f.Line, f.Column)
			}
		})
	}
}
