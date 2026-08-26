package schema

import (
	"strings"
	"testing"
	"time"
)

// specComQuery monta uma spec mínima com o bloco `query` que o teste quiser.
func specComBlocoDeQuery(bloco string) []byte {
	return []byte(`version: 1
name: d
materialization:
  key:
    field: id
schema:
  fields:
    id:
      type: string
` + bloco)
}

// O teste de COMPATIBILIDADE, e o mais importante desta frente: um YAML
// escrito antes de `query.maxTimeout` existir tem de produzir exatamente o
// comportamento de antes.
//
// O que "exatamente" quer dizer aqui é um número só: o teto efetivo continua
// sendo o `query.timeout` resolvido daquele dataset — nunca o default global,
// nunca 30 segundos fixos, nunca ausência de teto. As três alternativas
// passariam num teste frouxo e quebrariam produção de maneiras diferentes: o
// default global recusaria a spec de quem declara `timeout: 5m` (que hoje sobe),
// e a ausência de teto entregaria prazo infinito a quem nunca pediu.
func TestDatasetSemMaxTimeoutSeComportaExatamenteComoHoje(t *testing.T) {
	casos := []struct {
		nome     string
		bloco    string
		padroes  Defaults
		querTeto time.Duration
	}{
		{
			nome:     "sem bloco query nenhum: herda o global e o teto o acompanha",
			bloco:    "",
			padroes:  Defaults{MaxRows: 100, Timeout: 9 * time.Second},
			querTeto: 9 * time.Second,
		},
		{
			// O caso que uma implementação preguiçosa quebra: o dataset declara
			// prazo MAIOR que o default global. Se o teto viesse do global, esta
			// spec passaria a ser recusada na abertura — um arquivo que sobe hoje
			// deixaria o processo morrer na troca de binário.
			nome:     "dataset declara timeout acima do default global",
			bloco:    "query:\n  timeout: 5m\n",
			padroes:  Defaults{MaxRows: 100, Timeout: 30 * time.Second},
			querTeto: 5 * time.Minute,
		},
		{
			nome:     "dataset declara timeout abaixo do default global",
			bloco:    "query:\n  timeout: 250ms\n",
			padroes:  Defaults{MaxRows: 100, Timeout: 30 * time.Second},
			querTeto: 250 * time.Millisecond,
		},
		{
			nome:     "DefaultsPadrao, que é o que ferramenta e teste carregam",
			bloco:    "",
			padroes:  DefaultsPadrao,
			querTeto: 30 * time.Second,
		},
	}

	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			s, err := Parse(specComBlocoDeQuery(c.bloco), c.padroes)
			if err != nil {
				t.Fatal(err)
			}
			if s.Query.MaxTimeout != c.querTeto {
				t.Errorf("MaxTimeout = %s, quer %s", s.Query.MaxTimeout, c.querTeto)
			}
			// A igualdade com o Timeout é a invariante, não uma coincidência dos
			// números escolhidos: enquanto ninguém declara maxTimeout, teto e
			// default são o MESMO valor, que é o que fazia o campo único
			// funcionar.
			if s.Query.MaxTimeout != s.Query.Timeout {
				t.Errorf("MaxTimeout = %s e Timeout = %s: sem a chave declarada os dois têm de ser o mesmo número",
					s.Query.MaxTimeout, s.Query.Timeout)
			}
		})
	}
}

// A chave DECLARADA vence, e é o único jeito de alongar.
func TestMaxTimeoutDeclaradoAlongaOTetoSemMexerNoDefault(t *testing.T) {
	s, err := Parse(specComBlocoDeQuery("query:\n  timeout: 2s\n  maxTimeout: 10m\n"), DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	if s.Query.Timeout != 2*time.Second {
		t.Errorf("Timeout = %s, quer 2s: declarar o teto não pode mexer no prazo padrão", s.Query.Timeout)
	}
	if s.Query.MaxTimeout != 10*time.Minute {
		t.Errorf("MaxTimeout = %s, quer 10m", s.Query.MaxTimeout)
	}
}

// O default GLOBAL do teto (query.defaultMaxTimeout) alcança o dataset que não
// declara a chave — e perde para o que declara.
func TestDefaultGlobalDeMaxTimeoutAlcancaODatasetQueNaoDeclara(t *testing.T) {
	padroes := Defaults{MaxRows: 100, Timeout: 30 * time.Second, MaxTimeout: 8 * time.Minute}

	s, err := Parse(specComBlocoDeQuery(""), padroes)
	if err != nil {
		t.Fatal(err)
	}
	if s.Query.MaxTimeout != 8*time.Minute {
		t.Errorf("MaxTimeout = %s, quer 8m (o global)", s.Query.MaxTimeout)
	}
	if s.Query.Timeout != 30*time.Second {
		t.Errorf("Timeout = %s, quer 30s: o teto global não pode virar prazo padrão", s.Query.Timeout)
	}

	s, err = Parse(specComBlocoDeQuery("query:\n  maxTimeout: 1m\n"), padroes)
	if err != nil {
		t.Fatal(err)
	}
	if s.Query.MaxTimeout != time.Minute {
		t.Errorf("MaxTimeout = %s, quer 1m: o dataset que declara vence o global", s.Query.MaxTimeout)
	}
}

// Um teto abaixo do prazo padrão é contradição, não configuração: o servidor
// recusaria o prazo que ele mesmo aplica quando o request não pede nada.
//
// A recusa NOMEIA AS DUAS CHAVES com os dois valores. Sem isso o operador lê
// "teto menor que o default" e não sabe qual dos números veio do YAML do
// dataset e qual veio do global — e o par inválido pode ter se formado de
// quatro maneiras diferentes.
func TestMaxTimeoutMenorQueTimeoutEhRecusadoNomeandoAsDuasChaves(t *testing.T) {
	casos := []struct {
		nome    string
		bloco   string
		padroes Defaults
	}{
		{
			nome:    "as duas declaradas no dataset",
			bloco:   "query:\n  timeout: 10s\n  maxTimeout: 5s\n",
			padroes: DefaultsPadrao,
		},
		{
			// O par se forma CRUZADO: o teto vem do global e o prazo do dataset.
			// É o caso que mais engana, porque nenhum dos dois arquivos parece
			// errado sozinho.
			nome:    "teto global contra timeout do dataset",
			bloco:   "query:\n  timeout: 10m\n",
			padroes: Defaults{MaxRows: 100, Timeout: 30 * time.Second, MaxTimeout: time.Minute},
		},
	}

	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			_, err := Parse(specComBlocoDeQuery(c.bloco), c.padroes)
			if err == nil {
				t.Fatal("quer recusa: um teto abaixo do prazo padrão recusaria o próprio default")
			}
			for _, chave := range []string{"query.maxTimeout", "query.timeout"} {
				if !strings.Contains(err.Error(), chave) {
					t.Errorf("a mensagem não nomeia %s: %v", chave, err)
				}
			}
		})
	}
}

// Teto igual ao prazo padrão é o caso NORMAL — é o que todo dataset sem a
// chave produz —, e não pode cair na recusa por um `<=` no lugar de um `<`.
func TestMaxTimeoutIgualAoTimeoutEhAceito(t *testing.T) {
	s, err := Parse(specComBlocoDeQuery("query:\n  timeout: 7s\n  maxTimeout: 7s\n"), DefaultsPadrao)
	if err != nil {
		t.Fatalf("teto igual ao prazo padrão tem de ser aceito: %v", err)
	}
	if s.Query.MaxTimeout != 7*time.Second {
		t.Errorf("MaxTimeout = %s, quer 7s", s.Query.MaxTimeout)
	}
}

// Valor inválido é recusado NOMEANDO a chave, no mesmo molde do query.timeout.
func TestMaxTimeoutInvalidoEhRecusadoPelaChave(t *testing.T) {
	for _, v := range []string{"treze", "0s", "-1s"} {
		t.Run(v, func(t *testing.T) {
			_, err := Parse(specComBlocoDeQuery("query:\n  maxTimeout: "+v+"\n"), DefaultsPadrao)
			if err == nil {
				t.Fatalf("maxTimeout %q deveria ser recusado", v)
			}
			if !strings.Contains(err.Error(), "query.maxTimeout") {
				t.Errorf("a mensagem não nomeia a chave: %v", err)
			}
		})
	}
}

// O compilado é o que o servidor e o binder leem; um teto que fica na spec e
// não chega ao Compiled não vale para ninguém.
func TestMaxTimeoutChegaAoCompilado(t *testing.T) {
	s, err := Parse(specComBlocoDeQuery("query:\n  timeout: 3s\n  maxTimeout: 4m\n"), DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.Compile(newFakeDict())
	if err != nil {
		t.Fatal(err)
	}
	if c.Timeout != 3*time.Second {
		t.Errorf("Compiled.Timeout = %s, quer 3s", c.Timeout)
	}
	if c.MaxTimeout != 4*time.Minute {
		t.Errorf("Compiled.MaxTimeout = %s, quer 4m", c.MaxTimeout)
	}
}
