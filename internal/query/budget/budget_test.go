package budget_test

import (
	"sync"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/budget"
)

// O caminho quente NÃO aloca.
//
// É a razão de este pacote ser dois `atomic.Int64` e não um mutex com um mapa:
// o consumidor real é o StreamWriter, chamado UMA VEZ POR ROW, e o gate de
// alocação do executor exige IGUALDADE EXATA de allocs entre 1.000 e 8.000
// linhas. Qualquer alocação aqui — um erro embrulhado, um slice, um boxing em
// interface — apareceria lá como alocação POR ROW, e o orçamento teria custado
// exatamente aquilo que ele existe para proteger.
//
// Reserva e Devolve são medidas JUNTAS e em par: medir só a Reserva deixaria
// passar uma Devolve que alocasse, e ela roda com a mesma frequência.
func TestOrcamentoNaoAlocaNoCaminhoQuente(t *testing.T) {
	g := budget.NewGlobal(1 << 30)
	p := g.Query(1 << 20)

	cases := []struct {
		name string
		f    func()
	}{
		{"Global.Reserva/Devolve", func() {
			g.Reserve(64)
			g.Return(64)
		}},
		{"Global ilimitado", func() {
			// O ramo do teto <= 0 é OUTRO caminho de código (Add direto, sem
			// CAS) e precisa da mesma prova: é por ele que passam o CLI e os
			// benchmarks.
			unlimited := budget.NewGlobal(0)
			unlimited.Reserve(64)
			unlimited.Return(64)
		}},
		{"PorQuery.Admite/Libera", func() {
			// Não é caminho por LINHA — é uma vez por consulta —, mas está no
			// caminho de ADMISSÃO, que roda antes de qualquer trabalho útil: uma
			// alocação aqui apareceria como custo fixo em toda requisição.
			p.Admit()
			p.Release()
		}},
		{"PorQuery.Teto", func() { _ = p.Ceiling() }},
		{"PorQuery.Usado", func() { _ = p.Used() }},
		{"Global.Usado", func() { _ = g.Used() }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if n := testing.AllocsPerRun(200, c.f); n != 0 {
				t.Errorf("%s alocou %.0f vezes por chamada; o caminho quente tem de ser zero", c.name, n)
			}
		})
	}
}

// A recusa é EXATA no limite, nas duas pontas.
//
// Um `>=` no lugar do `>` recusaria o último byte que cabe, e o operador que
// dimensionou o orçamento pelo tamanho da linha veria a linha ser recusada pelo
// número que ele mesmo escreveu.
func TestReservaRecusaExatamenteNoLimite(t *testing.T) {
	g := budget.NewGlobal(100)
	if !g.Reserve(100) {
		t.Fatal("100 não coube num teto de 100: o limite é inclusive")
	}
	if g.Reserve(1) {
		t.Fatal("101 coube num teto de 100")
	}
	if g.Used() != 100 {
		t.Fatalf("Usado() = %d depois de uma recusa, quer 100: a recusa não pode contabilizar", g.Used())
	}
	g.Return(100)
	if g.Used() != 0 {
		t.Fatalf("Usado() = %d, quer 0", g.Used())
	}
}

// Admite reserva o TETO INTEIRO, de uma vez.
//
// É a decisão de admissão: reservar conforme o buffer cresce deixaria o
// orçamento do processo acabar no MEIO de um stream, e a consulta morreria
// depois de o cliente já ter recebido quarenta mil linhas — por causa da memória
// de outra consulta. O número reservado é o teto, não o uso corrente, e é isso
// que o nishidb_query_buffer_bytes publica.
func TestAdmiteReservaOTetoInteiroDeUmaVez(t *testing.T) {
	g := budget.NewGlobal(1 << 20)
	p := g.Query(64 << 10)

	if !p.Admit() {
		t.Fatal("64 KiB não coube num processo de 1 MiB")
	}
	if g.Used() != 64<<10 {
		t.Errorf("Usado() = %d, quer 64KiB: a reserva é do teto inteiro", g.Used())
	}
	if p.Used() != 64<<10 {
		t.Errorf("PorQuery.Usado() = %d, quer 64KiB", p.Used())
	}
}

// A segunda consulta que não cabe é recusada NA ADMISSÃO, e não no meio.
//
// Sem esta asserção, um Global que apenas contasse sem nunca recusar passaria —
// e o `memoryTarget` continuaria sendo a chave sem efeito que o débito 15 nomeia,
// só que agora com um número no /metrics.
func TestAdmiteRecusaQuandoOProcessoNaoComporta(t *testing.T) {
	// Comporta UM teto de 64 KiB, não dois.
	g := budget.NewGlobal(80 << 10)

	first := g.Query(64 << 10)
	if !first.Admit() {
		t.Fatal("a primeira consulta não coube num processo que a comporta")
	}
	second := g.Query(64 << 10)
	if second.Admit() {
		t.Fatal("a segunda consulta passou: o Global não recusou")
	}
	if g.Used() != 64<<10 {
		t.Errorf("Usado() = %d depois da recusa, quer 64KiB: a recusa não pode contabilizar", g.Used())
	}

	// E a vaga de memória VOLTA quando a primeira termina — sem isso o
	// memoryTarget se esgotaria monotonicamente e o processo pararia de aceitar
	// consulta sem estar usando memória nenhuma.
	first.Release()
	if !second.Admit() {
		t.Fatal("depois do Libera a segunda continuou recusada: o orçamento não foi devolvido")
	}
}

// Libera é idempotente.
//
// O chamador a põe num defer e pode tê-la chamado no caminho de erro também.
// Uma devolução dupla faria o `usado` do processo andar para BAIXO do real, e a
// partir dali o teto deixaria de valer para todo mundo — em silêncio, que é o
// pior modo de um limite falhar.
func TestLiberaEhIdempotente(t *testing.T) {
	g := budget.NewGlobal(1 << 20)
	p := g.Query(1 << 10)
	if !p.Admit() {
		t.Fatal("1 KiB não coube")
	}
	p.Release()
	p.Release()
	p.Release()
	if g.Used() != 0 {
		t.Fatalf("Usado() = %d depois de três Libera, quer 0", g.Used())
	}
}

// Teto <= 0 é ILIMITADO nos dois níveis.
//
// O Global ilimitado continua CONTANDO — é o que permite a um processo sem
// configuração ainda publicar quanto está usando —, e o PorQuery ilimitado
// admite sem reservar, porque não há número a reservar: inventar um faria o
// nishidb_query_buffer_bytes publicar uma grandeza que não descreve nada.
func TestTetoNaoPositivoEhIlimitado(t *testing.T) {
	g := budget.NewGlobal(0)
	if !g.Reserve(1 << 40) {
		t.Fatal("um terabyte foi recusado por um Global ilimitado")
	}
	if g.Used() != 1<<40 {
		t.Fatalf("Usado() = %d, quer 1TiB: ilimitado não é o mesmo que não contar", g.Used())
	}

	p := budget.NewGlobal(1 << 10).Query(0)
	if !p.Admit() {
		t.Fatal("uma consulta sem teto foi recusada")
	}
	if p.Used() != 0 {
		t.Errorf("PorQuery.Usado() = %d sem teto declarado, quer 0", p.Used())
	}
}

// Sob concorrência, o teto VALE.
//
// É o teste do laço de CAS. Com `Load` seguido de `Add`, duas goroutines leem o
// mesmo "cabe" e passam juntas do teto — e o orçamento seria excedido
// exatamente sob a concorrência que ele existe para conter, que é a de N
// consultas simultâneas do §80. Com N goroutines pedindo uma unidade cada num
// teto de N/2, exatamente N/2 têm de passar.
func TestOTetoValeSobConcorrencia(t *testing.T) {
	const goroutines = 200
	const ceiling = goroutines / 2

	g := budget.NewGlobal(ceiling)
	var wg sync.WaitGroup
	accepted := make([]bool, goroutines)
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			accepted[i] = g.Reserve(1)
		}(i)
	}
	close(start)
	wg.Wait()

	n := 0
	for _, ok := range accepted {
		if ok {
			n++
		}
	}
	if n != ceiling {
		t.Errorf("%d reservas aceitas num teto de %d: o CAS não está segurando", n, ceiling)
	}
	if g.Used() != ceiling {
		t.Errorf("Usado() = %d, quer %d", g.Used(), ceiling)
	}
}
