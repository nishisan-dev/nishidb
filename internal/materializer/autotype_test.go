package materializer

import (
	"errors"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/fielddict"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

const autoFixYAML = `
version: 1
name: auto_fix
materialization:
  key:
    field: id
schema:
  include:
    - "*"
  unknownFields: include
  fields:
    id:
      type: string
`

// R11: a primeira observação decide o tipo, e a decisão é persistida. Sem
// isso o binder não teria contra o que coerir o literal de uma query sobre o
// campo — e o dataset real usa wildcard com unknownFields include, então
// quase todo campo filtrável cai aqui.
func TestAutoFixaTipoNaPrimeiraObservacao(t *testing.T) {
	h := newHarness(t, autoFixYAML)
	h.apply(`{"id":"A","X-Attr":5}`)

	f, ok := h.c.FieldByName([]byte("X-Attr"))
	if !ok {
		t.Fatal("X-Attr não foi resolvido")
	}
	if got := f.EffectiveType(); got != types.TypeInt64 {
		t.Errorf("EffectiveType = %v, want int64", got)
	}
	h.wantInt(h.state("A"), "X-Attr", 5)
}

// A regra na prática, e o que ela custa: depois de fixado, o tipo manda.
func TestAutoSegundaObservacaoDivergenteCoerceOuFalha(t *testing.T) {
	casos := []struct {
		name     string
		primeiro string
		segundo  string
		// querSkip: o segundo evento é descartado como TYPE_CONVERSION.
		querSkip bool
		// verifica o estado após o segundo evento.
		verifica func(t *testing.T, h *harness)
	}{
		{
			name:     "string numérica coerida para o int64 fixado",
			primeiro: `{"id":"A","X":5}`,
			// Mesma regra da coluna declarada int64, que já aceita string
			// numérica por causa do Connect-Info da amostra real.
			segundo: `{"id":"A","X":"1000"}`,
			verifica: func(t *testing.T, h *harness) {
				h.wantInt(h.state("A"), "X", 1000)
			},
		},
		{
			name:     "double num campo fixado como int64",
			primeiro: `{"id":"A","X":5}`,
			segundo:  `{"id":"A","X":5.5}`,
			querSkip: true,
			verifica: func(t *testing.T, h *harness) {
				h.wantInt(h.state("A"), "X", 5) // estado anterior intacto
			},
		},
		{
			// A OUTRA metade da simetria: assim como o int64 fixado aceita
			// string numérica (caso acima), o string fixado aceita número. O
			// Raw do parser é o texto-fonte, então gravar `7` como "7" não
			// altera um byte do que a origem escreveu — e antes disto o evento
			// inteiro era DESCARTADO por causa de uma grafia.
			name:     "número num campo fixado como string é coergido para o texto-fonte",
			primeiro: `{"id":"A","X":"texto"}`,
			segundo:  `{"id":"A","X":7}`,
			verifica: func(t *testing.T, h *harness) {
				h.wantString(h.state("A"), "X", "7")
				// E a coerção NÃO refixa o tipo. É a parte que o R11 exige e
				// que uma conversão bem-sucedida poderia fazer parecer
				// dispensável: se o número redefinisse a coluna para int64, o
				// mesmo campo passaria a ter significado diferente conforme a
				// ordem de chegada dos eventos, e nenhuma query sobre ele seria
				// confiável — que é exatamente o defeito que fixar o tipo na
				// primeira observação existe para impedir.
				f, ok := h.c.FieldByName([]byte("X"))
				if !ok {
					t.Fatal("campo X não compilado")
				}
				if got := f.EffectiveType(); got != types.TypeString {
					t.Errorf("EffectiveType após a coerção = %v, quer string", got)
				}
			},
		},
		{
			// O que a coerção NÃO alcança, e é o que mantém o "ou falha" do
			// nome deste teste vivo para o alvo `string`: um documento inteiro
			// não vira texto. A coluna existe para ser comparável.
			name:     "objeto num campo fixado como string",
			primeiro: `{"id":"A","X":"texto"}`,
			segundo:  `{"id":"A","X":{"a":1}}`,
			querSkip: true,
			verifica: func(t *testing.T, h *harness) {
				h.wantString(h.state("A"), "X", "texto")
			},
		},
		{
			name:     "null não conflita e não desfixa",
			primeiro: `{"id":"A","X":5}`,
			segundo:  `{"id":"A","X":null}`,
			verifica: func(t *testing.T, h *harness) {
				// D2: o campo some do estado...
				h.wantAbsent(h.state("A"), "X")
				// ...mas o tipo continua fixado.
				f, _ := h.c.FieldByName([]byte("X"))
				if got := f.EffectiveType(); got != types.TypeInt64 {
					t.Errorf("EffectiveType após null = %v, want int64", got)
				}
			},
		},
	}

	for _, tc := range casos {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, autoFixYAML)
			h.apply(tc.primeiro)

			r := h.m.Apply([]byte(tc.segundo))
			if tc.querSkip {
				// Evento ruim: a ingestão PULA e continua. Failed aqui
				// significaria que o conflito não foi embrulhado em
				// ErrTypeConversion e a ingestão inteira pararia.
				if r.Action != ActionSkipped {
					t.Fatalf("Action = %v (%v), want Skipped", r.Action, r.Err)
				}
				if r.Category != CategoryTypeConversion {
					t.Errorf("Category = %v, want TYPE_CONVERSION", r.Category)
				}
				if !errors.Is(r.Err, ErrTypeConversion) {
					t.Errorf("erro = %v, want ErrTypeConversion", r.Err)
				}
				if !strings.Contains(r.Err.Error(), "X") {
					t.Errorf("mensagem = %q, want nomear o campo", r.Err)
				}
			} else if r.Err != nil {
				t.Fatalf("Apply(%s): %v", tc.segundo, r.Err)
			}
			tc.verifica(t, h)

			// Em nenhum caso a ingestão pode ter parado.
			if r.Action == ActionFailed {
				t.Fatalf("Action = Failed (%v): conflito de tipo é evento ruim, não falha de ingestão", r.Err)
			}
			if h.m.Metrics().FailedTotal != 0 {
				t.Errorf("FailedTotal = %d, want 0", h.m.Metrics().FailedTotal)
			}
		})
	}
}

// null JSON não fixa tipo: fixar TypeNull gravaria "sem tipo" e travaria o
// campo para sempre.
func TestAutoNaoFixaComNull(t *testing.T) {
	h := newHarness(t, autoFixYAML)
	h.apply(`{"id":"A","X-Attr":null}`)

	f, ok := h.c.FieldByName([]byte("X-Attr"))
	if !ok {
		t.Fatal("X-Attr não foi resolvido")
	}
	if got := f.EffectiveType(); got != types.TypeNull {
		t.Errorf("EffectiveType = %v, want null (null não fixa)", got)
	}
	h.wantAbsent(h.state("A"), "X-Attr")

	// E o próximo evento com valor de verdade fixa normalmente.
	h.apply(`{"id":"A","X-Attr":"texto"}`)
	if got := f.EffectiveType(); got != types.TypeString {
		t.Errorf("EffectiveType após valor real = %v, want string", got)
	}
}

// O conflito do DICIONÁRIO (diferente do conflito de conversão) chega quando
// o tipo foi fixado por outro caminho — o writer e as queries compartilham o
// mesmo dicionário, então o hint pode aparecer entre o Compile e a primeira
// observação deste processo.
//
// Ele precisa carregar os DOIS sentinelas: o do storage identifica a causa no
// log, e ErrTypeConversion decide o balde. Sem o segundo, o erro cai no
// default do classify, vira CategoryStorage e **para a ingestão inteira** por
// causa de um campo que mudou de formato na origem.
func TestConflitoDeTipoNoDicionarioEhEventoRuim(t *testing.T) {
	spec, err := schema.Parse([]byte(autoFixYAML), schema.BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	dict := fakestorage.NewDict()
	c, err := spec.Compile(dict)
	if err != nil {
		t.Fatal(err)
	}
	m := mustNew(t, c, fakestorage.New(fakestorage.Indexes{}))

	// Segundo Compiled sobre o MESMO dicionário — o dicionário é do dataset,
	// não do processo.
	c2, err := spec.Compile(dict)
	if err != nil {
		t.Fatal(err)
	}
	m2 := mustNew(t, c2, fakestorage.New(fakestorage.Indexes{}))

	// (1) O primeiro compilado resolve X sem fixar tipo (null não fixa).
	if r := m.Apply([]byte(`{"id":"A","X":null}`)); r.Err != nil {
		t.Fatal(r.Err)
	}
	// (2) O segundo fixa string no dicionário compartilhado.
	if r := m2.Apply([]byte(`{"id":"B","X":"texto"}`)); r.Err != nil {
		t.Fatal(r.Err)
	}
	// (3) O primeiro ainda acha o campo virgem e tenta fixar int64: é aqui
	// que o conflito do dicionário aparece.
	r := m.Apply([]byte(`{"id":"C","X":5}`))

	if r.Action != ActionSkipped {
		t.Fatalf("Action = %v (%v), want Skipped: conflito de tipo é evento ruim, "+
			"não motivo para parar a ingestão", r.Action, r.Err)
	}
	if r.Category != CategoryTypeConversion {
		t.Errorf("Category = %v, want TYPE_CONVERSION", r.Category)
	}
	if !errors.Is(r.Err, ErrTypeConversion) {
		t.Errorf("erro = %v, want ErrTypeConversion (decide o balde)", r.Err)
	}
	if !errors.Is(r.Err, fielddict.ErrTypeHintConflict) {
		t.Errorf("erro = %v, want fielddict.ErrTypeHintConflict (identifica a causa)", r.Err)
	}
	if !strings.Contains(r.Err.Error(), "X") {
		t.Errorf("mensagem = %q, want nomear o campo", r.Err)
	}
	if mt := m.Metrics(); mt.FailedTotal != 0 || mt.InvalidTotal != 1 {
		t.Errorf("métricas = %+v, want InvalidTotal=1 FailedTotal=0", mt)
	}
	// E a ingestão continua utilizável, agora convertendo contra o tipo que
	// venceu.
	if r := m.Apply([]byte(`{"id":"D","X":"texto"}`)); r.Err != nil {
		t.Fatalf("evento seguinte falhou: %v", r.Err)
	}
}

// Conversão de VALOR (string não numérica num campo int64) é outro caminho:
// nem chega ao dicionário.
func TestConversaoDeValorNaoAcusaConflitoDeDicionario(t *testing.T) {
	h := newHarness(t, autoFixYAML)
	h.apply(`{"id":"A","X":5}`)

	r := h.m.Apply([]byte(`{"id":"A","X":"não numérico"}`))
	if r.Action != ActionSkipped {
		t.Fatalf("Action = %v, want Skipped", r.Action)
	}
	if !errors.Is(r.Err, ErrTypeConversion) {
		t.Errorf("erro = %v, want ErrTypeConversion", r.Err)
	}
	if errors.Is(r.Err, fielddict.ErrTypeHintConflict) {
		t.Error("conversão de valor não deveria acusar conflito de dicionário")
	}
}

// dictFalhaAoFixar persiste nomes normalmente, mas falha ao gravar o TIPO — o
// que o fielddict real faz quando o WriteAt/fsync do registro de hint falha
// (disco cheio, EIO, EFBIG).
type dictFalhaAoFixar struct {
	inner *fakestorage.Dict
	err   error
}

func (d dictFalhaAoFixar) LookupOrCreate(name []byte) (types.FieldID, error) {
	return d.inner.LookupOrCreate(name)
}

func (d dictFalhaAoFixar) TypeHint(id types.FieldID) (types.FieldType, bool) {
	return d.inner.TypeHint(id)
}

func (d dictFalhaAoFixar) SetTypeHint(types.FieldID, types.FieldType) error { return d.err }

func (d dictFalhaAoFixar) Len() int { return d.inner.Len() }

func (d dictFalhaAoFixar) Name(id types.FieldID) (string, bool) { return d.inner.Name(id) }

// Falha de I/O ao PERSISTIR o tipo fixado vem do dataset, não do evento.
// Contabilizá-la como TYPE_CONVERSION faria o pipeline pular, em silêncio e
// para sempre, todo evento que tocasse um campo auto ainda sem tipo — com
// Failed=0 no painel, que é a leitura oposta da realidade. A política de três
// baldes existe exatamente para impedir que bug/infra vire "dado ruim".
func TestFalhaDeIODoDicionarioAoFixarTipoEhFailed(t *testing.T) {
	s, err := schema.Parse([]byte(autoFixYAML), schema.BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	eio := errors.New(`fielddict: fsync do tipo de "X": input/output error`)
	c, err := s.Compile(dictFalhaAoFixar{inner: fakestorage.NewDict(), err: eio})
	if err != nil {
		t.Fatal(err)
	}
	m := mustNew(t, c, fakestorage.New(fakestorage.Indexes{}))

	r := m.Apply([]byte(`{"id":"A","X":5}`))

	if r.Action != ActionFailed {
		t.Fatalf("Action = %v (%v), want Failed: falha do dicionário não é evento ruim", r.Action, r.Err)
	}
	if r.Category != CategoryStorage {
		t.Errorf("Category = %v, want STORAGE_ERROR", r.Category)
	}
	if errors.Is(r.Err, ErrTypeConversion) {
		t.Errorf("erro = %v, want SEM ErrTypeConversion: é falha de infra, não conversão", r.Err)
	}
	if !errors.Is(r.Err, eio) {
		t.Errorf("erro = %v, want preservar o erro do dicionário", r.Err)
	}
	if mt := m.Metrics(); mt.InvalidTotal != 0 || mt.FailedTotal != 1 {
		t.Errorf("métricas = %+v, want InvalidTotal=0 FailedTotal=1", mt)
	}
}
