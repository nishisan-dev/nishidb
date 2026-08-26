package server

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"sync"
	"time"

	"github.com/nishisan-dev/nishidb/internal/protocolo"
)

// emVoo é o registro das consultas que estão correndo NESTE instante.
//
// Existe porque o medidor nishidb_query_active responde "quantas", e com o
// prazo longo virando cidadão de primeira classe a pergunta do operador passou
// a ser outra: QUAL consulta está segurando a vaga, em que dataset, e HÁ QUANTO
// TEMPO. "3 ativas" é a mesma frase para três point lookups de 2 ms e para um
// full scan de oito minutos, e as duas situações pedem ações opostas.
//
// O mapa é indexado por um serial monotônico, e não pelo hash do SQL: duas
// execuções da MESMA consulta são duas ocupações de vaga, e chaveá-las pelo
// texto faria a segunda apagar a primeira do painel — justamente no cenário em
// que o operador está tentando entender por que o pool encheu.
type emVoo struct {
	mu       sync.Mutex
	proximo  uint64
	consulta map[uint64]consultaEmVoo
}

type consultaEmVoo struct {
	dataset string
	hash    string
	longa   bool
	inicio  time.Time
}

func novoEmVoo() *emVoo {
	return &emVoo{consulta: make(map[uint64]consultaEmVoo)}
}

// registraEmVoo anota a consulta e devolve a função que a retira.
//
// A forma é a mesma de metricasDeQuery.admite, e pelo mesmo motivo: com o par
// registra/retira numa closure, um `defer` no chamador cobre TODOS os retornos
// — inclusive os que ainda não existem. Uma entrada que sobrevive à consulta
// vaza memória e, pior, mente no painel: o operador veria uma consulta de
// horas que terminou há muito tempo.
func (s *Servidor) registraEmVoo(dataset, sql string, longa bool, inicio time.Time) func() {
	return s.emVoo.registra(dataset, sql, longa, inicio)
}

func (e *emVoo) registra(dataset, sql string, longa bool, inicio time.Time) func() {
	c := consultaEmVoo{dataset: dataset, hash: hashDoSQL(sql), longa: longa, inicio: inicio}
	e.mu.Lock()
	id := e.proximo
	e.proximo++
	e.consulta[id] = c
	e.mu.Unlock()
	return func() {
		e.mu.Lock()
		delete(e.consulta, id)
		e.mu.Unlock()
	}
}

// instantaneo copia o registro para uma lista ordenada da mais ANTIGA para a
// mais nova.
//
// A ordem é do interesse: quem lê a lista está procurando a consulta que está
// lá há mais tempo, e ela tem de ser a primeira linha, não uma que se procura.
// A cópia é feita sob lock e a formatação fora dele — segurar o mutex do
// registro enquanto se serializa JSON faria o /v1/stats atrasar a admissão de
// TODA consulta nova.
func (e *emVoo) instantaneo(agora time.Time) []protocolo.ConsultaEmVoo {
	e.mu.Lock()
	out := make([]protocolo.ConsultaEmVoo, 0, len(e.consulta))
	for _, c := range e.consulta {
		out = append(out, protocolo.ConsultaEmVoo{
			Dataset:   c.dataset,
			SQLHash:   c.hash,
			Long:      c.longa,
			RunningMs: agora.Sub(c.inicio).Milliseconds(),
		})
	}
	e.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].RunningMs != out[j].RunningMs {
			return out[i].RunningMs > out[j].RunningMs
		}
		// Desempate ESTÁVEL por hash: duas consultas admitidas no mesmo
		// milissegundo sairiam em ordem de iteração de mapa, que o Go
		// randomiza — e um painel que troca de ordem a cada scrape é ilegível.
		return out[i].SQLHash < out[j].SQLHash
	})
	return out
}

// maisAntiga é a idade, em segundos, da consulta em voo há mais tempo. Zero
// quando não há nenhuma.
//
// É a fonte de nishidb_query_longest_running_seconds, e é lida no SCRAPE em
// vez de mantida num medidor atualizado a cada admissão: um medidor só mudaria
// quando alguma consulta começasse ou terminasse, e o número que interessa
// cresce SOZINHO enquanto nada acontece — que é exatamente o estado de um
// servidor travado numa consulta longa.
func (e *emVoo) maisAntiga(agora time.Time) float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	var pior time.Duration
	for _, c := range e.consulta {
		if d := agora.Sub(c.inicio); d > pior {
			pior = d
		}
	}
	return pior.Seconds()
}

// hashDoSQL identifica a consulta sem publicar o texto dela.
//
// O texto NÃO vai para o registro porque ele pode carregar literais com dado do
// cliente, e o §95 proíbe registrar payload por default — é a mesma razão de o
// SQL só sair no log em nível DEBUG (ver transmite, em query.go). O hash
// truncado responde a pergunta que o operador de fato faz: "as quatro consultas
// presas são a mesma ou quatro diferentes?".
//
// Doze caracteres hexadecimais (48 bits) são folga de sobra para o punhado de
// consultas que cabe no pool, e cabem numa coluna de terminal.
func hashDoSQL(sql string) string {
	soma := sha256.Sum256([]byte(sql))
	return hex.EncodeToString(soma[:6])
}
