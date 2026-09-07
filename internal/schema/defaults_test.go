package schema

import (
	"strings"
	"testing"
	"time"
)

// Os defaults globais do nishidb.yaml (§78 query.defaultMaxRows/defaultTimeout)
// só existem se o dataset que OMITE a chave herdar deles. Enquanto o default
// morava numa constante dentro do validate, editar a chave global não mudava
// nada e não avisava nada — era o débito 37.

// specWithoutQuery é o YAML mínimo sem a seção `query`: é exatamente o caso em que
// o global tem de valer.
const specWithoutQuery = `
version: 1
name: sem_query
materialization:
  key:
    field: id
schema:
  fields:
    id:
      type: string
`

// specWithQuery declara as duas chaves: o dataset vence o global.
const specWithQuery = specWithoutQuery + `
query:
  maxRows: 7
  timeout: 3s
`

func TestDatasetSemQueryHerdaOsDefaultsGlobais(t *testing.T) {
	d := Defaults{MaxRows: 42, Timeout: 5 * time.Second}
	s, err := Parse([]byte(specWithoutQuery), d)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if s.Query.MaxRows != 42 {
		t.Errorf("MaxRows = %d, want 42 (o global do nishidb.yaml)", s.Query.MaxRows)
	}
	if s.Query.Timeout != 5*time.Second {
		t.Errorf("Timeout = %s, want 5s (o global do nishidb.yaml)", s.Query.Timeout)
	}
}

func TestDatasetQueDeclaraVenceODefaultGlobal(t *testing.T) {
	d := Defaults{MaxRows: 42, Timeout: 5 * time.Second}
	s, err := Parse([]byte(specWithQuery), d)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if s.Query.MaxRows != 7 {
		t.Errorf("MaxRows = %d, want 7 (o do dataset)", s.Query.MaxRows)
	}
	if s.Query.Timeout != 3*time.Second {
		t.Errorf("Timeout = %s, want 3s (o do dataset)", s.Query.Timeout)
	}
}

// Default global inválido é erro do chamador e sai como erro. Cair no padrão
// da spec quando o global veio zerado esconderia o defeito de quem passou o
// zero — e é assim que uma chave volta a mentir em silêncio.
func TestDefaultsGlobaisInvalidosSaoRecusados(t *testing.T) {
	cases := []struct {
		name    string
		d       Defaults
		snippet string
		yamlSrc string
	}{
		{"maxRows zero", Defaults{MaxRows: 0, Timeout: time.Second}, "defaultMaxRows", specWithoutQuery},
		{"maxRows negativo", Defaults{MaxRows: -1, Timeout: time.Second}, "defaultMaxRows", specWithoutQuery},
		{"timeout zero", Defaults{MaxRows: 1, Timeout: 0}, "defaultTimeout", specWithoutQuery},
		{"timeout negativo", Defaults{MaxRows: 1, Timeout: -time.Second}, "defaultTimeout", specWithoutQuery},
		// Recusa mesmo quando o dataset declara as duas chaves: o default
		// inválido não fica escondido pelo dataset que não precisaria dele.
		{"inválido com dataset que declara", Defaults{}, "defaultMaxRows", specWithQuery},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yamlSrc), tc.d)
			if err == nil {
				t.Fatalf("esperava erro contendo %q", tc.snippet)
			}
			if !strings.Contains(err.Error(), tc.snippet) {
				t.Errorf("erro = %v, want conter %q", err, tc.snippet)
			}
		})
	}
}

// BuiltinDefaults é o que a spec §78 documenta, e é o que vale para quem carrega
// uma spec sem nishidb.yaml nenhum (testes, benchmarks). Se mudar, o YAML de
// exemplo do README e da spec mudam junto.
func TestDefaultsPadraoSaoOsDaSpec(t *testing.T) {
	if BuiltinDefaults.MaxRows != 100000 {
		t.Errorf("DefaultsPadrao.MaxRows = %d, want 100000", BuiltinDefaults.MaxRows)
	}
	if BuiltinDefaults.Timeout != 30*time.Second {
		t.Errorf("DefaultsPadrao.Timeout = %s, want 30s", BuiltinDefaults.Timeout)
	}
}
