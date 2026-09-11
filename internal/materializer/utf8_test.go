package materializer

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/jsonp"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// invalidUTF8 é um byte de continuação solto: sequência que nenhum
// codificador legítimo produz e que json.Valid aceita.
const invalidUTF8 = "\x80\x81"

// A ingestão é a porta: um valor com UTF-8 malformado que passe daqui vira
// linha NDJSON inválida no encoder e quebra o consumidor em RUNTIME, por um
// dado que já estava errado na entrada. json.Valid não valida UTF-8, então a
// checagem precisa ser explícita.
func TestConvertRecusaUTF8Invalido(t *testing.T) {
	casos := []struct {
		name   string
		field  jsonp.Field
		target types.FieldType
	}{
		{
			name:   "string",
			field:  jsonp.Field{Kind: jsonp.KindString, Raw: []byte("ok" + invalidUTF8)},
			target: types.TypeString,
		},
		{
			name:   "json objeto",
			field:  jsonp.Field{Kind: jsonp.KindObject, Raw: []byte(`{"a":"` + invalidUTF8 + `"}`)},
			target: types.TypeJSON,
		},
		{
			name:   "json array",
			field:  jsonp.Field{Kind: jsonp.KindArray, Raw: []byte(`["` + invalidUTF8 + `"]`)},
			target: types.TypeJSON,
		},
	}
	for _, tc := range casos {
		t.Run(tc.name, func(t *testing.T) {
			// Pré-condição do teste: o JSON é sintaticamente VÁLIDO. Sem isso
			// o caso provaria só que json.Valid funciona.
			if tc.target == types.TypeJSON && !json.Valid(tc.field.Raw) {
				t.Fatalf("fixture inválida para o ponto do teste: %q não é JSON válido", tc.field.Raw)
			}

			_, _, err := Convert(tc.field, tc.target, nil, nil)
			if err == nil {
				t.Fatal("UTF-8 inválido foi aceito")
			}
			if !errors.Is(err, ErrTypeConversion) {
				t.Errorf("erro = %v, want ErrTypeConversion", err)
			}
			if !strings.Contains(err.Error(), "utf-8") {
				t.Errorf("mensagem = %q, want identificar a guarda", err)
			}
		})
	}
}

// O arena não pode ficar com resíduo do valor recusado: o próximo campo
// escreveria depois dele e o payload sairia com bytes órfãos.
func TestUTF8InvalidoNaoSujaOArena(t *testing.T) {
	arena := make([]byte, 0, 64)
	arena = append(arena, "prefixo"...)
	tamanho := len(arena)

	f := jsonp.Field{Kind: jsonp.KindString, Raw: []byte(invalidUTF8)}
	_, out, err := Convert(f, types.TypeString, nil, arena)
	if err == nil {
		t.Fatal("esperava erro")
	}
	if len(out) != tamanho {
		t.Errorf("arena com %d bytes após recusa, want %d", len(out), tamanho)
	}
	if string(out) != "prefixo" {
		t.Errorf("arena = %q, want %q", out, "prefixo")
	}
}

// UTF-8 legítimo continua passando — inclusive o que a amostra real traz
// (acentos) e emoji fora do BMP.
func TestConvertAceitaUTF8Valido(t *testing.T) {
	casos := []struct {
		name   string
		raw    string
		kind   jsonp.Kind
		target types.FieldType
	}{
		{"acentos", "joão@isp.net.br", jsonp.KindString, types.TypeString},
		{"emoji", "sessão 😀", jsonp.KindString, types.TypeString},
		{"vazio", "", jsonp.KindString, types.TypeString},
		{"json com acento", `{"nome":"José"}`, jsonp.KindObject, types.TypeJSON},
	}
	for _, tc := range casos {
		t.Run(tc.name, func(t *testing.T) {
			v, _, err := Convert(jsonp.Field{Kind: tc.kind, Raw: []byte(tc.raw)}, tc.target, nil, nil)
			if err != nil {
				t.Fatalf("Convert(%q): %v", tc.raw, err)
			}
			if string(v.Bytes) != tc.raw {
				t.Errorf("Bytes = %q, want %q", v.Bytes, tc.raw)
			}
		})
	}
}

// Fim a fim: o evento com UTF-8 malformado é evento RUIM (pulado), não falha
// de ingestão — e não chega ao store.
func TestApplyPulaEventoComUTF8Invalido(t *testing.T) {
	h := newHarness(t, patchYAML)
	r := h.m.Apply([]byte("{\"id\":\"A\",\"_SEQ\":1,\"Acct-Status-Type\":\"" + invalidUTF8 + "\"}"))

	if r.Action != ActionSkipped {
		t.Fatalf("Action = %v (%v), want Skipped", r.Action, r.Err)
	}
	if r.Category != CategoryTypeConversion {
		t.Errorf("Category = %v, want TYPE_CONVERSION", r.Category)
	}
	if len(h.st.PutCalls) != 0 {
		t.Errorf("valor com UTF-8 inválido chegou ao store: %+v", h.st.PutCalls)
	}
	// A ingestão continua.
	h.apply(`{"id":"B","_SEQ":1,"Acct-Status-Type":"Start"}`)
}
