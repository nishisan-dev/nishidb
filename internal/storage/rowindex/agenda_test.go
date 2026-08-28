package rowindex

import (
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func abreAgenda(t *testing.T, minRows uint64) *Agenda {
	t.Helper()
	a, err := OpenAgenda(t.TempDir(), minRows)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a
}

// A folha guarda o que foi gravado, e a linha nunca tocada é ∞.
func TestAgendaGuardaOVencimentoDaFolha(t *testing.T) {
	a := abreAgenda(t, 512)
	if v := a.Get(7); v != Nunca {
		t.Fatalf("linha nunca gravada devolveu %d, quer %d (∞)", v, Nunca)
	}
	if err := a.Atualiza(7, 1_000); err != nil {
		t.Fatal(err)
	}
	if v := a.Get(7); v != 1_000 {
		t.Fatalf("folha devolveu %d, quer 1000", v)
	}
}

// O mínimo do topo é o que o coletor usa para decidir até quando dormir. Ele
// é um limite INFERIOR do vencimento real — nunca pode ficar acima dele.
func TestAgendaProximoVencimentoEhOMinimoDeTodos(t *testing.T) {
	a := abreAgenda(t, 4096)
	for _, c := range []struct {
		row  types.RowID
		venc uint64
	}{{10, 5_000}, {2_000, 300}, {3_500, 900}} {
		if err := a.Atualiza(c.row, c.venc); err != nil {
			t.Fatal(err)
		}
	}
	v, ok := a.ProximoVencimento()
	if !ok || v != 300 {
		t.Fatalf("ProximoVencimento=%d,%v; quer 300,true", v, ok)
	}
}

// Sem nenhuma linha agendada não há próximo vencimento: o coletor não tem
// prazo para esperar, e dormir até "zero" o faria girar em vão.
func TestAgendaVaziaNaoTemProximoVencimento(t *testing.T) {
	a := abreAgenda(t, 512)
	if v, ok := a.ProximoVencimento(); ok {
		t.Fatalf("agenda vazia devolveu vencimento %d", v)
	}
}

// A descida entrega TODAS as linhas vencidas e NENHUMA que ainda não venceu.
// Um falso negativo aqui é uma linha que nunca mais é recuperada.
func TestAgendaEntregaExatamenteOsVencidos(t *testing.T) {
	a := abreAgenda(t, 1<<14)
	querem := map[types.RowID]bool{}
	for row := types.RowID(0); row < 5_000; row++ {
		venc := uint64(1_000 + row) // 1000..5999
		if err := a.Atualiza(row, venc); err != nil {
			t.Fatal(err)
		}
		if venc <= 3_000 {
			querem[row] = true
		}
	}
	achados := map[types.RowID]bool{}
	a.Vencidos(3_000, func(row types.RowID) bool {
		achados[row] = true
		return true
	})
	for row := range querem {
		if !achados[row] {
			t.Fatalf("linha %d venceu e não foi entregue", row)
		}
	}
	for row := range achados {
		if !querem[row] {
			t.Fatalf("linha %d não venceu e foi entregue", row)
		}
	}
}

// A visita que devolve false para a descida no meio: é o freio de
// MaxPorPassada, e ele não pode deixar a estrutura inconsistente.
func TestAgendaParaADescidaQuandoAVisitaPede(t *testing.T) {
	a := abreAgenda(t, 4096)
	for row := types.RowID(0); row < 500; row++ {
		if err := a.Atualiza(row, 10); err != nil {
			t.Fatal(err)
		}
	}
	visitas := 0
	a.Vencidos(100, func(types.RowID) bool {
		visitas++
		return visitas < 3
	})
	if visitas != 3 {
		t.Fatalf("visitou %d linhas depois de pedir parada na terceira", visitas)
	}
	if err := a.Confere(); err != nil {
		t.Fatal(err)
	}
}

// A invariante inteira, sob carga aleatória de abaixamentos e subidas: o nó
// é limite INFERIOR do mínimo dos filhos, em todo instante.
func TestAgendaMantemAInvarianteSobCargaAleatoria(t *testing.T) {
	a := abreAgenda(t, 1<<13)
	r := rand.New(rand.NewPCG(7, 11))
	espelho := map[types.RowID]uint64{}
	for i := 0; i < 20_000; i++ {
		row := types.RowID(r.UintN(4_000))
		venc := uint64(r.UintN(1_000) + 1)
		if r.UintN(10) == 0 {
			venc = Nunca // tombstone: sai da agenda
		}
		if err := a.Atualiza(row, venc); err != nil {
			t.Fatal(err)
		}
		espelho[row] = venc
		// A conferência varre a árvore inteira, então ela é amostrada: a
		// invariante que interessa é a que sobrevive a uma sequência longa de
		// operações, e conferi-la a cada uma faria o teste custar minutos
		// para provar a mesma coisa.
		if i%100 == 0 {
			if err := a.Confere(); err != nil {
				t.Fatalf("iteração %d (row=%d venc=%d): %v", i, row, venc, err)
			}
		}
	}
	if err := a.Confere(); err != nil {
		t.Fatal(err)
	}
	for row, venc := range espelho {
		if got := a.Get(row); got != venc {
			t.Fatalf("folha da row %d é %d, quer %d", row, got, venc)
		}
	}
	// E o teste que a invariante existe para permitir: a descida acha tudo.
	for corte := uint64(1); corte <= 1_000; corte += 137 {
		achados := map[types.RowID]bool{}
		a.Vencidos(corte, func(row types.RowID) bool {
			achados[row] = true
			return true
		})
		for row, venc := range espelho {
			if venc != Nunca && venc <= corte && !achados[row] {
				t.Fatalf("corte %d: row %d (venc %d) não foi entregue", corte, row, venc)
			}
		}
	}
}

// Crescer a agenda além do arquivo inicial não recomputa nada: o bloco novo
// nasce zerado, que é lido como ∞ — o valor certo para linhas que ainda não
// existem.
func TestAgendaCresceSemRecomputarOSumario(t *testing.T) {
	a := abreAgenda(t, 512)
	if err := a.Atualiza(3, 100); err != nil {
		t.Fatal(err)
	}
	// Muito além do arquivo inicial de 512 entradas.
	if err := a.Atualiza(300_000, 50); err != nil {
		t.Fatal(err)
	}
	if err := a.Confere(); err != nil {
		t.Fatal(err)
	}
	v, ok := a.ProximoVencimento()
	if !ok || v != 50 {
		t.Fatalf("ProximoVencimento=%d,%v; quer 50,true", v, ok)
	}
	achados := 0
	a.Vencidos(100, func(types.RowID) bool { achados++; return true })
	if achados != 2 {
		t.Fatalf("descida achou %d linhas, quer 2", achados)
	}
}

// Reabrir tem de encontrar a agenda como ela ficou: ela é persistente, e é
// disso que depende não varrer o dataset inteiro a cada subida.
func TestAgendaSobreviveAoFechamento(t *testing.T) {
	dir := t.TempDir()
	a, err := OpenAgenda(dir, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Atualiza(1_500, 777); err != nil {
		t.Fatal(err)
	}
	if err := a.Msync(); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	b, err := OpenAgenda(dir, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	if v := b.Get(1_500); v != 777 {
		t.Fatalf("depois de reabrir, a folha é %d, quer 777", v)
	}
	v, ok := b.ProximoVencimento()
	if !ok || v != 777 {
		t.Fatalf("depois de reabrir, ProximoVencimento=%d,%v; quer 777,true", v, ok)
	}
}

// Confere reprova o sumário que ficou ACIMA do mínimo dos filhos — que é a
// direção fatal: o coletor não desceria naquele ramo e a linha nunca seria
// recuperada. É o mutante que os testes de ordem de escrita simulam.
func TestConfereReprovaOSumarioAcimaDoMinimo(t *testing.T) {
	a := abreAgenda(t, 4096)
	if err := a.Atualiza(100, 10); err != nil {
		t.Fatal(err)
	}
	if err := a.Confere(); err != nil {
		t.Fatalf("agenda íntegra reprovada: %v", err)
	}
	// Adultera o sumário de nível 1 do bloco que contém a row 100.
	if err := a.niveis[1].store(types.RowID(100>>agendaShift), 999); err != nil {
		t.Fatal(err)
	}
	if err := a.Confere(); err == nil {
		t.Fatal("Confere aceitou sumário acima do mínimo dos filhos")
	}
}

// Sumário ABAIXO do mínimo real é frouxo, não quebrado: custa uma descida
// desperdiçada, e é exatamente o que uma subida interrompida antes do
// recompute deixa para trás.
func TestConfereAceitaOSumarioFrouxo(t *testing.T) {
	a := abreAgenda(t, 4096)
	if err := a.Atualiza(100, 10); err != nil {
		t.Fatal(err)
	}
	// A folha sobe sem que nenhum sumário recompute: todos os ancestrais
	// ficam em 10, abaixo do mínimo real, que agora é 500.
	if err := a.niveis[0].store(100, 500); err != nil {
		t.Fatal(err)
	}
	if err := a.Confere(); err != nil {
		t.Fatalf("sumário frouxo reprovado: %v", err)
	}
	// E o que a frouxidão custa é só isso: uma descida que não acha nada.
	achados := 0
	a.Vencidos(10, func(types.RowID) bool { achados++; return true })
	if achados != 0 {
		t.Fatalf("a descida entregou %d linhas não vencidas", achados)
	}
}

// O arquivo de cada nível é próprio, e o nome carrega o nível: trocar dois
// deles seria trocar folha por sumário sem nenhum sintoma imediato.
func TestAgendaTemUmArquivoPorNivel(t *testing.T) {
	dir := t.TempDir()
	a, err := OpenAgenda(dir, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	if len(a.niveis) != agendaNiveis {
		t.Fatalf("a agenda tem %d níveis, quer %d", len(a.niveis), agendaNiveis)
	}
	for _, nome := range []string{"vencimentos.idx", "vencimentos.s1.idx", "vencimentos.s2.idx", "vencimentos.s3.idx"} {
		if _, err := filepath.Glob(filepath.Join(dir, nome)); err != nil {
			t.Fatalf("nível ausente: %s", nome)
		}
	}
}

// A ORDEM DE ESCRITA do caminho de abaixamento, que é a invariante mais cara
// desta estrutura e a única cuja violação é invisível depois do fato.
//
// Abaixar a folha antes dos ancestrais deixa, ENTRE as duas escritas, um
// ancestral acima do mínimo dos filhos — e um leitor sem lock que descesse
// nessa janela pularia o ramo e nunca mais recuperaria a linha. A janela some
// quando o Atualiza retorna, então conferir a invariante só entre operações
// aprovaria a ordem invertida.
//
// MUTAÇÃO QUE ESTE TESTE PEGA: gravar a folha antes dos ancestrais no ramo de
// abaixamento do Atualiza.
func TestAbaixamentoMantemAInvarianteAPOSCADAEscrita(t *testing.T) {
	a := abreAgenda(t, 4096)
	// Semeia o bloco com um vencimento distante, para que a linha nova de
	// fato abaixe os sumários em vez de nascer neles.
	if err := a.Atualiza(200, 9_000); err != nil {
		t.Fatal(err)
	}
	escritas := 0
	a.aposEscrita = func() {
		escritas++
		if err := a.Confere(); err != nil {
			t.Fatalf("invariante quebrada depois da escrita %d de um abaixamento: %v", escritas, err)
		}
	}
	if err := a.Atualiza(201, 5); err != nil {
		t.Fatal(err)
	}
	if escritas < 2 {
		t.Fatalf("o abaixamento gravou %d entradas; sem sumário e folha não há ordem a provar", escritas)
	}
}

// A subida também não pode quebrar a invariante em nenhum instante — ali a
// ordem é a oposta (folha primeiro, recompute depois), e ela é segura porque
// aumentar um filho só torna o pai mais frouxo.
func TestSubidaMantemAInvarianteAPOSCADAEscrita(t *testing.T) {
	a := abreAgenda(t, 4096)
	if err := a.Atualiza(300, 10); err != nil {
		t.Fatal(err)
	}
	a.aposEscrita = func() {
		if err := a.Confere(); err != nil {
			t.Fatalf("invariante quebrada durante uma subida: %v", err)
		}
	}
	if err := a.Atualiza(300, 8_000); err != nil {
		t.Fatal(err)
	}
}

// O CUSTO da renovação, que é o caso de todo evento em regime. A parada da
// subida no primeiro nó cujo mínimo vinha de outro filho é o que mantém a
// atualização em O(1) esperado: sem ela, cada evento recomputaria um nó por
// nível, lendo agendaB filhos em cada um.
//
// MUTAÇÃO QUE ESTE TESTE PEGA: remover a condição de parada do recompute
// (`ordem(atual) < ordem(anterior)`) ou o corte por `novo == atual`.
func TestRenovacaoNaoRecomputaAArvoreInteira(t *testing.T) {
	a := abreAgenda(t, 1<<13)
	r := rand.New(rand.NewPCG(3, 5))
	// Vencimentos ESPALHADOS e renovação em ordem ALEATÓRIA, que é o regime:
	// a linha que o evento renova é a que o tráfego escolheu, não a que está
	// mais perto de vencer. Semear em ordem crescente e renovar na mesma
	// ordem faria de toda linha o mínimo do seu bloco, e o teste mediria o
	// pior caso da estrutura em vez do caso que ela paga a cada evento.
	const linhas = 4_000
	for row := types.RowID(0); row < linhas; row++ {
		if err := a.Atualiza(row, uint64(r.UintN(100_000)+1)); err != nil {
			t.Fatal(err)
		}
	}
	escritas, recomputes := 0, 0
	a.aposEscrita = func() { escritas++ }
	a.aposRecompute = func() { recomputes++ }
	const renovacoes = 4_000
	for i := 0; i < renovacoes; i++ {
		row := types.RowID(r.UintN(linhas))
		// Empurra o vencimento para frente, que é o que uma escrita nova faz.
		if err := a.Atualiza(row, uint64(200_000+i)); err != nil {
			t.Fatal(err)
		}
	}
	// Uma escrita é a folha, obrigatória. O teto de 1,3 dá folga para os
	// recomputes legítimos (a linha renovada ERA o mínimo do bloco) sem
	// deixar passar uma implementação que suba a árvore toda: sem a parada
	// seriam agendaNiveis escritas por evento.
	if media := float64(escritas) / renovacoes; media > 1.3 {
		t.Fatalf("renovação custou %.2f escritas em média (%d em %d); teto 1,30 — a subida não está parando cedo",
			media, escritas, renovacoes)
	}
	// E os RECOMPUTES, que são o custo de verdade: cada um lê agendaB
	// entradas. A parada da subida os mantém raros — só a linha que ERA o
	// mínimo do bloco força um. Sem ela, toda renovação recomputa um nó por
	// nível de sumário, e o número de escritas nem se mexe.
	// Medido nesta carga: 0,0165 recomputes por renovação. O teto de 0,10 dá
	// seis vezes de folga para a variação da amostra e ainda reprova por uma
	// ordem de grandeza o mutante que remove a parada, que mede 1,02.
	if media := float64(recomputes) / renovacoes; media > 0.10 {
		t.Fatalf("renovação custou %.2f recomputes em média (%d em %d); teto 0,10 — a subida está recomputando sem precisar",
			media, recomputes, renovacoes)
	}
}
