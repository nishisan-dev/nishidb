package segindex_test

import (
	"errors"
	"os"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// coleta esgota a fonte e devolve os rowIds emitidos, conferindo de passagem
// que Err() ficou nil — uma fonte que parasse cedo por erro e fosse comparada
// só pelo conteúdo passaria como "acertou os primeiros dois".
func coleta(t *testing.T, f segindex.Fonte) []types.RowID {
	t.Helper()
	var rows []types.RowID
	for f.Next() {
		rows = append(rows, f.RowID())
	}
	if err := f.Err(); err != nil {
		t.Fatalf("Err() = %v, quero nil", err)
	}
	return rows
}

// exigeRows compara o que a fonte emitiu com o esperado.
func exigeRows(t *testing.T, got, want []types.RowID, contexto string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: rows = %v, quero %v", contexto, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: rows = %v, quero %v", contexto, got, want)
		}
	}
}

// escreveSeladoDeTeste emite um selado mínimo — um campo, um valor, três rows
// — e devolve o caminho. É o corpus dos testes que só precisam de um arquivo
// bem-formado para depois estragá-lo.
func escreveSeladoDeTeste(t *testing.T) string {
	t.Helper()
	e := segindex.NovoEscritor()
	e.Adiciona(7, 100, 10)
	e.Adiciona(7, 100, 20)
	e.Adiciona(7, 100, 30)
	caminho := segindex.CaminhoSelado(t.TempDir(), 1)
	if err := e.Escreve(caminho, 1); err != nil {
		t.Fatalf("Escreve: %v", err)
	}
	return caminho
}

// TestOSeladoDevolveOQueOEscritorRecebeu é o round-trip do formato, e usa de
// propósito o MESMO corpus de TestOEscritorAgrupaOrdenaEDeduplicaAsPostings:
// rowIds fora de ordem, uma duplicata, dois campos, o hash menor adicionado
// depois e o fieldID menor por último. Aquele teste prova que os BYTES saíram
// certos, lendo-os com um decodificador local; este prova que o leitor os
// interpreta do mesmo jeito, e a interseção dos dois é o que impede "os dois
// erram igual" de passar por "os dois estão certos".
func TestOSeladoDevolveOQueOEscritorRecebeu(t *testing.T) {
	dir := t.TempDir()
	e := segindex.NovoEscritor()
	// campo 7, valor A (hash 100): rows 30, 10, 30 (duplicata), 20
	e.Adiciona(7, 100, 30)
	e.Adiciona(7, 100, 10)
	e.Adiciona(7, 100, 30)
	e.Adiciona(7, 100, 20)
	// campo 7, valor B (hash 50): row 15  -- hash MENOR, adicionado depois
	e.Adiciona(7, 50, 15)
	// campo 3 (fieldID MENOR, adicionado por último)
	e.Adiciona(3, 999, 12)

	caminho := segindex.CaminhoSelado(dir, 42)
	if err := e.Escreve(caminho, 42); err != nil {
		t.Fatalf("Escreve: %v", err)
	}
	s, err := segindex.AbreSelado(caminho)
	if err != nil {
		t.Fatalf("AbreSelado: %v", err)
	}
	defer func() {
		if err := s.Fecha(); err != nil {
			t.Errorf("Fecha: %v", err)
		}
	}()

	if got, want := s.SegmentID(), uint32(42); got != want {
		t.Errorf("SegmentID() = %d, quero %d", got, want)
	}
	if got, want := s.MinRowID(), types.RowID(10); got != want {
		t.Errorf("MinRowID() = %d, quero %d", got, want)
	}
	if got, want := s.MaxRowID(), types.RowID(30); got != want {
		t.Errorf("MaxRowID() = %d, quero %d", got, want)
	}

	campos := s.Campos()
	if len(campos) != 2 || campos[0] != 3 || campos[1] != 7 {
		t.Fatalf("Campos() = %v, quero [3 7] (ordenado por fieldID)", campos)
	}
	for _, c := range []types.FieldID{3, 7} {
		if !s.TemCampo(c) {
			t.Errorf("TemCampo(%d) = false, quero true", c)
		}
	}
	if s.TemCampo(5) {
		t.Error("TemCampo(5) = true, quero false — o campo 5 não tem seção neste segmento")
	}

	// O valor com muitas postings: agrupado, ascendente e sem a duplicata.
	//
	// É este Lookup, e não o do hash 50, que prova que postIdx é índice de
	// ELEMENTO e não offset em bytes. O hash 50 tem postIdx 0, onde as duas
	// leituras coincidem e um leitor errado passaria; o hash 100 tem postIdx 1,
	// e lê-lo como offset começaria a decodificar no BYTE 1 do array de deltas
	// — desalinhado, dentro do arquivo, sem estourar limite nenhum e sem erro.
	f, ok := s.Lookup(7, 100)
	if !ok {
		t.Fatal("Lookup(7, 100) devolveu ok=false")
	}
	if got, want := f.MinRowID(), types.RowID(10); got != want {
		t.Errorf("fonte(7,100).MinRowID() = %d, quero %d", got, want)
	}
	if got, want := f.MaxRowID(), types.RowID(30); got != want {
		t.Errorf("fonte(7,100).MaxRowID() = %d, quero %d", got, want)
	}
	exigeRows(t, coleta(t, f), []types.RowID{10, 20, 30}, "campo 7, hash 100")
	if err := f.Close(); err != nil {
		t.Errorf("Close da fonte: %v", err)
	}

	// O valor de posting única no MESMO campo: a busca binária no diretório
	// tem de achar as duas entradas, e o hash MENOR foi adicionado DEPOIS.
	f, ok = s.Lookup(7, 50)
	if !ok {
		t.Fatal("Lookup(7, 50) devolveu ok=false")
	}
	exigeRows(t, coleta(t, f), []types.RowID{15}, "campo 7, hash 50")

	// O outro campo, para provar que a busca binária na tabela de seções acha
	// o fieldID menor que foi adicionado por último.
	f, ok = s.Lookup(3, 999)
	if !ok {
		t.Fatal("Lookup(3, 999) devolveu ok=false")
	}
	exigeRows(t, coleta(t, f), []types.RowID{12}, "campo 3, hash 999")
}

// TestValorAusenteEhFonteVaziaCampoAusenteEhOkFalso trava a distinção que
// decide o plano do chamador. Campo sem seção significa "este segmento não
// responde por este campo" e obriga a enumerar o segmento inteiro (§3.2 da
// spec); valor sem entrada no diretório significa "respondo, e a resposta é
// zero linhas". Confundir os dois nas duas direções é grave: um faz enumerar
// 224 mil linhas por nada, o outro faz PERDER linhas que existem.
func TestValorAusenteEhFonteVaziaCampoAusenteEhOkFalso(t *testing.T) {
	s, err := segindex.AbreSelado(escreveSeladoDeTeste(t))
	if err != nil {
		t.Fatalf("AbreSelado: %v", err)
	}
	defer func() {
		if err := s.Fecha(); err != nil {
			t.Errorf("Fecha: %v", err)
		}
	}()

	if _, ok := s.Lookup(9, 100); ok {
		t.Error("Lookup de campo SEM seção devolveu ok=true — o chamador deixaria de enumerar o segmento")
	}
	f, ok := s.Lookup(7, 12345)
	if !ok {
		t.Fatal("Lookup de valor ausente em campo indexado devolveu ok=false — o chamador enumeraria o segmento por nada")
	}
	if rows := coleta(t, f); len(rows) != 0 {
		t.Errorf("fonte de valor ausente emitiu %v, quero nada", rows)
	}
	// O contrato do Une (Tarefa 5): fonte que não vai emitir nada reporta
	// Min > Max, e é isso que permite descartá-la antes de decidir sobre
	// disjunção, sem consumir nenhuma.
	if f.MinRowID() <= f.MaxRowID() {
		t.Errorf("fonte vazia reportou Min=%d Max=%d — quero Min > Max", f.MinRowID(), f.MaxRowID())
	}
	if err := f.Close(); err != nil {
		t.Errorf("Close da fonte vazia: %v", err)
	}
}

// TestCRCDivergenteNaoDerrubaAConsulta: índice é dado derivado, e a doutrina do
// planner ("toda dúvida degrada") vale aqui. O Lookup devolve ok=false, que o
// chamador lê como "este segmento não responde por este campo" e resolve por
// enumeração — resposta correta, só lenta. Derrubar a consulta trocaria
// corrupção de um arquivo reconstruível por indisponibilidade.
func TestCRCDivergenteNaoDerrubaAConsulta(t *testing.T) {
	caminho := escreveSeladoDeTeste(t) // helper: 1 campo, 1 valor, 3 rows
	b, err := os.ReadFile(caminho)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 0xFF // corrompe o crc
	if err := os.WriteFile(caminho, b, 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := segindex.AbreSelado(caminho)
	if err != nil {
		t.Fatalf("AbreSelado não deve falhar por crc: %v", err)
	}
	defer func() {
		if err := s.Fecha(); err != nil {
			t.Errorf("Fecha: %v", err)
		}
	}()
	if _, ok := s.Lookup(7, 100); ok {
		t.Error("Lookup sobre arquivo com crc divergente devolveu ok=true")
	}
	if !errors.Is(s.ErroDeIntegridade(), segindex.ErrCRC) {
		t.Errorf("ErroDeIntegridade() = %v, quero ErrCRC — o chamador precisa saber que é corrupção, e não campo ausente", s.ErroDeIntegridade())
	}
}

// TestCRCIntegroNaoReportaErroDeIntegridade é a contraparte do teste de
// corrupção: sem ela, um ErroDeIntegridade que devolvesse ErrCRC SEMPRE
// passaria naquele teste, e a reconstrução seria disparada em cima de todo
// índice são do dataset.
func TestCRCIntegroNaoReportaErroDeIntegridade(t *testing.T) {
	s, err := segindex.AbreSelado(escreveSeladoDeTeste(t))
	if err != nil {
		t.Fatalf("AbreSelado: %v", err)
	}
	defer func() {
		if err := s.Fecha(); err != nil {
			t.Errorf("Fecha: %v", err)
		}
	}()
	if err := s.ErroDeIntegridade(); err != nil {
		t.Fatalf("ErroDeIntegridade() = %v, quero nil", err)
	}
}

// TestSeladoRecusaHeaderInvalido cobre as recusas de FORMATO, que são
// separadas do crc de propósito: formato inválido é bug ou versão de arquivo
// que este binário não conhece, e o remédio não é reconstruir. As três
// mutilações do header são as que levariam a busca binária para offsets
// arbitrários dentro do mapeamento se passassem — e o crc, que as pegaria, só
// é conferido na primeira CONSULTA, não aqui.
func TestSeladoRecusaHeaderInvalido(t *testing.T) {
	casos := map[string]func(b []byte) []byte{
		"magic trocado":       func(b []byte) []byte { copy(b[0:4], "XXXX"); return b },
		"versão desconhecida": func(b []byte) []byte { b[4] = segindex.VersaoSelado + 1; return b },
		"largura de posting inválida": func(b []byte) []byte {
			b[32] = 5
			return b
		},
		"dirOff além do fim": func(b []byte) []byte {
			// dirOff da primeira seção fica em HeaderSelado+8.
			for i := range 8 {
				b[segindex.HeaderSelado+8+i] = 0xFF
			}
			return b
		},
		"secCount maior que o arquivo": func(b []byte) []byte {
			b[6], b[7] = 0xFF, 0xFF
			return b
		},
		"truncado antes do header": func(b []byte) []byte { return b[:16] },
	}
	original, err := os.ReadFile(escreveSeladoDeTeste(t))
	if err != nil {
		t.Fatal(err)
	}
	for nome, estraga := range casos {
		t.Run(nome, func(t *testing.T) {
			b := estraga(append([]byte(nil), original...))
			caminho := segindex.CaminhoSelado(t.TempDir(), 1)
			if err := os.WriteFile(caminho, b, 0o644); err != nil {
				t.Fatal(err)
			}
			s, err := segindex.AbreSelado(caminho)
			if err == nil {
				_ = s.Fecha()
				t.Fatal("AbreSelado aceitou o header mutilado")
			}
			if !errors.Is(err, segindex.ErrFormato) {
				t.Errorf("erro = %v, quero ErrFormato", err)
			}
		})
	}
}

// TestLookupAposFechaPanicaEmVezDeLerMemoriaDesmontada trava a rede do nível
// de baixo. As fontes leem o mmap direto, sem cópia — é o que as torna
// gratuitas —, então consultar depois do Fecha leria memória já desmontada, e
// o desfecho seria SIGSEGV: sinal incapturável, sem nome de arquivo, longe da
// linha que errou. O pânico nomeia o método e o arquivo, e ainda é conversível
// em erro na fronteira da requisição.
//
// A garantia de que isso nunca acontece continua sendo do dono, que faz
// quiesce antes de fechar. Isto é o que sobra para o dia em que um caminho
// novo escapar.
func TestLookupAposFechaPanicaEmVezDeLerMemoriaDesmontada(t *testing.T) {
	s, err := segindex.AbreSelado(escreveSeladoDeTeste(t))
	if err != nil {
		t.Fatalf("AbreSelado: %v", err)
	}
	if err := s.Fecha(); err != nil {
		t.Fatalf("Fecha: %v", err)
	}
	defer func() {
		if recover() == nil {
			t.Error("Lookup após Fecha deveria entrar em pânico")
		}
	}()
	s.Lookup(7, 100)
}

// abreSeladoComNPostings monta um selado com um campo, um valor e n postings
// consecutivas. É o corpus do gate de alocação, onde o que importa é ter
// linhas suficientes para que uma cópia por linha apareça na média.
func abreSeladoComNPostings(t *testing.T, n int) *segindex.Selado {
	t.Helper()
	e := segindex.NovoEscritor()
	for i := range n {
		e.Adiciona(7, 100, types.RowID(i))
	}
	caminho := segindex.CaminhoSelado(t.TempDir(), 1)
	if err := e.Escreve(caminho, 1); err != nil {
		t.Fatalf("Escreve: %v", err)
	}
	s, err := segindex.AbreSelado(caminho)
	if err != nil {
		t.Fatalf("AbreSelado: %v", err)
	}
	return s
}
