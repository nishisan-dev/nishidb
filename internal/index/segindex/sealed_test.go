package segindex_test

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// collect esgota a fonte e devolve os rowIds emitidos, conferindo de passagem
// que Err() ficou nil — uma fonte que parasse cedo por erro e fosse comparada
// só pelo conteúdo passaria como "acertou os primeiros dois".
func collect(t *testing.T, f segindex.Source) []types.RowID {
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

// requireRows compara o que a fonte emitiu com o esperado.
func requireRows(t *testing.T, got, want []types.RowID, label string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: rows = %v, quero %v", label, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: rows = %v, quero %v", label, got, want)
		}
	}
}

// writeTestSealed emite um selado mínimo — um campo, um valor, três rows
// — e devolve o caminho. É o corpus dos testes que só precisam de um arquivo
// bem-formado para depois estragá-lo.
func writeTestSealed(t *testing.T) string {
	t.Helper()
	e := segindex.NewWriter()
	e.Add(7, 100, 10)
	e.Add(7, 100, 20)
	e.Add(7, 100, 30)
	path := segindex.SealedPath(t.TempDir(), 1)
	if err := e.WriteSealed(path, 1); err != nil {
		t.Fatalf("WriteSealed: %v", err)
	}
	return path
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
	e := segindex.NewWriter()
	// campo 7, valor A (hash 100): rows 30, 10, 30 (duplicata), 20
	e.Add(7, 100, 30)
	e.Add(7, 100, 10)
	e.Add(7, 100, 30)
	e.Add(7, 100, 20)
	// campo 7, valor B (hash 50): row 15  -- hash MENOR, adicionado depois
	e.Add(7, 50, 15)
	// campo 3 (fieldID MENOR, adicionado por último)
	e.Add(3, 999, 12)

	path := segindex.SealedPath(dir, 42)
	if err := e.WriteSealed(path, 42); err != nil {
		t.Fatalf("WriteSealed: %v", err)
	}
	s, err := segindex.OpenSealed(path)
	if err != nil {
		t.Fatalf("OpenSealed: %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
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

	fields := s.Fields()
	if len(fields) != 2 || fields[0] != 3 || fields[1] != 7 {
		t.Fatalf("Fields() = %v, quero [3 7] (ordenado por fieldID)", fields)
	}
	for _, c := range []types.FieldID{3, 7} {
		if !s.HasField(c) {
			t.Errorf("HasField(%d) = false, quero true", c)
		}
	}
	if s.HasField(5) {
		t.Error("HasField(5) = true, quero false — o campo 5 não tem seção neste segmento")
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
	requireRows(t, collect(t, f), []types.RowID{10, 20, 30}, "campo 7, hash 100")
	if err := f.Close(); err != nil {
		t.Errorf("Close da fonte: %v", err)
	}

	// O valor de posting única no MESMO campo: a busca binária no diretório
	// tem de achar as duas entradas, e o hash MENOR foi adicionado DEPOIS.
	f, ok = s.Lookup(7, 50)
	if !ok {
		t.Fatal("Lookup(7, 50) devolveu ok=false")
	}
	requireRows(t, collect(t, f), []types.RowID{15}, "campo 7, hash 50")

	// O outro campo, para provar que a busca binária na tabela de seções acha
	// o fieldID menor que foi adicionado por último.
	f, ok = s.Lookup(3, 999)
	if !ok {
		t.Fatal("Lookup(3, 999) devolveu ok=false")
	}
	requireRows(t, collect(t, f), []types.RowID{12}, "campo 3, hash 999")
}

// TestOSeladoLeAsPostingsDeOitoBytes fecha a outra metade do formato. Todos os
// outros corpora do pacote têm span < 2³² e produzem postingWidth 4, então o
// ramo de 8 bytes do leitor — que o formato promete e o escritor demonstra
// emitir (TestGoldenDoSelado tem um golden de largura 8 congelado em disco) —
// nunca era executado.
//
// A largura não é hipótese remota: o rowId de um segmento não é contíguo. No
// dataset materializado a linha é regravada no segmento corrente mantendo o
// rowId, então um segmento tardio carrega postings de entidades antigas e
// novas ao mesmo tempo, e o span entre elas cresce com a vida do dataset.
//
// A asserção sobre o byte 32 é parte do teste, não conferência decorativa: sem
// ela, uma mudança em widthFor faria este teste passar a exercitar o mesmo
// ramo de 4 bytes que todos os outros já cobrem, e a perda de cobertura seria
// invisível.
func TestOSeladoLeAsPostingsDeOitoBytes(t *testing.T) {
	const far = types.RowID(1) << 33 // span >= 2³² a partir de 0

	e := segindex.NewWriter()
	e.Add(7, 100, 0)
	e.Add(7, 100, far)
	path := segindex.SealedPath(t.TempDir(), 1)
	if err := e.WriteSealed(path, 1); err != nil {
		t.Fatalf("WriteSealed: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := b[offWidth]; got != 8 {
		t.Fatalf("postingWidth = %d, quero 8 — o corpus deixou de exercitar o ramo de 8 bytes", got)
	}

	s, err := segindex.OpenSealed(path)
	if err != nil {
		t.Fatalf("OpenSealed: %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	f, ok := s.Lookup(7, 100)
	if !ok {
		t.Fatal("Lookup(7, 100) devolveu ok=false")
	}
	if got, want := f.MinRowID(), types.RowID(0); got != want {
		t.Errorf("MinRowID() = %d, quero %d", got, want)
	}
	if got, want := f.MaxRowID(), far; got != want {
		t.Errorf("MaxRowID() = %d, quero %d", got, want)
	}
	requireRows(t, collect(t, f), []types.RowID{0, far}, "postings de 8 bytes")
}

// TestValorAusenteEhFonteVaziaCampoAusenteEhOkFalso trava a distinção que
// decide o plano do chamador. Field sem seção significa "este segmento não
// responde por este campo" e obriga a enumerar o segmento inteiro (§3.2 da
// spec); valor sem entrada no diretório significa "respondo, e a resposta é
// zero linhas". Confundir os dois nas duas direções é grave: um faz enumerar
// 224 mil linhas por nada, o outro faz PERDER linhas que existem.
func TestValorAusenteEhFonteVaziaCampoAusenteEhOkFalso(t *testing.T) {
	s, err := segindex.OpenSealed(writeTestSealed(t))
	if err != nil {
		t.Fatalf("OpenSealed: %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	if _, ok := s.Lookup(9, 100); ok {
		t.Error("Lookup de campo SEM seção devolveu ok=true — o chamador deixaria de enumerar o segmento")
	}
	f, ok := s.Lookup(7, 12345)
	if !ok {
		t.Fatal("Lookup de valor ausente em campo indexado devolveu ok=false — o chamador enumeraria o segmento por nada")
	}
	if rows := collect(t, f); len(rows) != 0 {
		t.Errorf("fonte de valor ausente emitiu %v, quero nada", rows)
	}
	// O contrato do Union (Tarefa 5): fonte que não vai emitir nada reporta
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
	path := writeTestSealed(t) // helper: 1 campo, 1 valor, 3 rows
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 0xFF // corrompe o crc
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := segindex.OpenSealed(path)
	if err != nil {
		t.Fatalf("OpenSealed não deve falhar por crc: %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	if _, ok := s.Lookup(7, 100); ok {
		t.Error("Lookup sobre arquivo com crc divergente devolveu ok=true")
	}
	if !errors.Is(s.IntegrityErr(), segindex.ErrCRC) {
		t.Errorf("IntegrityErr() = %v, quero ErrCRC — o chamador precisa saber que é corrupção, e não campo ausente", s.IntegrityErr())
	}
}

// TestCRCIntegroNaoReportaErroDeIntegridade é a contraparte do teste de
// corrupção: sem ela, um IntegrityErr que devolvesse ErrCRC SEMPRE
// passaria naquele teste, e a reconstrução seria disparada em cima de todo
// índice são do dataset.
func TestCRCIntegroNaoReportaErroDeIntegridade(t *testing.T) {
	s, err := segindex.OpenSealed(writeTestSealed(t))
	if err != nil {
		t.Fatalf("OpenSealed: %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	if err := s.IntegrityErr(); err != nil {
		t.Fatalf("IntegrityErr() = %v, quero nil", err)
	}
}

// Offsets do corpus de writeTestSealed — um campo, um valor, três rows.
// Estão escritos aqui, e não derivados do pacote, porque quem mutila um formato
// de disco tem de saber onde está pondo a mão: o arquivo tem 120 bytes, a
// tabela de seções vai de 64 a 88, o diretório de 88 a 104, as postings de 104
// a 116 e o crc de 116 a 120.
const (
	offSecCount       = 6                              // u16
	offBaseRowID      = 16                             // u64
	offWidth          = 32                             // u8
	offDistinctValues = segindex.SealedHeaderSize + 4  // u32 da seção 0
	offDirOff         = segindex.SealedHeaderSize + 8  // u64 da seção 0
	offPostOff        = segindex.SealedHeaderSize + 16 // u64 da seção 0
	offPostIdx        = 96                             // u32 do slot 0 do diretório
	offCount          = 100                            // u32 do slot 0 do diretório
	corpusTableEnd    = 88
	corpusBodyEnd     = 116
)

func putU64(b []byte, off int, v uint64) { binary.LittleEndian.PutUint64(b[off:off+8], v) }
func putU32(b []byte, off int, v uint32) { binary.LittleEndian.PutUint32(b[off:off+4], v) }

// TestSeladoRecusaHeaderInvalido cobre as recusas de FORMATO, que são
// separadas do crc de propósito: formato inválido é bug ou versão de arquivo
// que este binário não conhece, e o remédio não é reconstruir.
//
// A validação de limites existe MESMO havendo crc porque as duas não rodam no
// mesmo momento: o crc só é conferido na primeira consulta, e entre o Open e
// ela a tabela de seções já foi lida. Cada caso abaixo é um offset que, se
// passasse, levaria a busca binária a comparar bytes arbitrários do mapeamento
// — ou a estourar o slice com um pânico que não nomeia o arquivo. São
// mutilações CIRÚRGICAS, um guarda por caso: escrever 0xFF em tudo provaria só
// que o guarda mais externo funciona.
func TestSeladoRecusaHeaderInvalido(t *testing.T) {
	cases := map[string]func(b []byte) []byte{
		"magic trocado":       func(b []byte) []byte { copy(b[0:4], "XXXX"); return b },
		"versão desconhecida": func(b []byte) []byte { b[4] = segindex.SealedVersion + 1; return b },
		"largura de posting inválida": func(b []byte) []byte {
			b[offWidth] = 5
			return b
		},
		"truncado antes do header": func(b []byte) []byte { return b[:16] },

		// --- header ---
		"secCount zero": func(b []byte) []byte {
			// "Sem índice" é arquivo AUSENTE. Um selado sem seção nenhuma faria
			// a fila de reconstrução considerar o segmento pronto.
			binary.LittleEndian.PutUint16(b[offSecCount:offSecCount+2], 0)
			return b
		},
		"secCount maior que o arquivo": func(b []byte) []byte {
			binary.LittleEndian.PutUint16(b[offSecCount:offSecCount+2], 0xFFFF)
			return b
		},
		"baseRowID acima do maxRowID": func(b []byte) []byte {
			// base é a origem dos deltas: acima do máximo, todo delta seria
			// negativo, e em u32 isso vira um rowId gigante — falso negativo
			// silencioso.
			putU64(b, offBaseRowID, 100)
			return b
		},

		// --- tabela de seções ---
		"dirOff dentro da tabela de seções": func(b []byte) []byte {
			putU64(b, offDirOff, corpusTableEnd-8)
			return b
		},
		"dirOff com wrap na aritmética": func(b []byte) []byte {
			// dirOff + distintos*16 estoura o u64 e dá a volta: sem o guarda de
			// wrap, dirFim ficaria PEQUENO e passaria pelo teste do fim.
			putU64(b, offDirOff, ^uint64(0))
			return b
		},
		"diretório passando do fim do corpo": func(b []byte) []byte {
			putU32(b, offDistinctValues, 100) // 88 + 100*16 = 1688 > 116
			return b
		},
		"postOff dentro do diretório": func(b []byte) []byte {
			putU64(b, offPostOff, corpusTableEnd) // 88 < dirFim (104)
			return b
		},
		"postOff além do fim do corpo": func(b []byte) []byte {
			putU64(b, offPostOff, corpusBodyEnd+8)
			return b
		},
		"postings não múltiplas da largura": func(b []byte) []byte {
			// 116 - 105 = 11 bytes de postings, que não é múltiplo de 4. O
			// número de postings é DERIVADO da contiguidade das seções, e uma
			// derivação fracionária significa que a contiguidade quebrou.
			putU64(b, offPostOff, corpusBodyEnd-11)
			return b
		},
	}
	original, err := os.ReadFile(writeTestSealed(t))
	if err != nil {
		t.Fatal(err)
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			b := corrupt(append([]byte(nil), original...))
			path := segindex.SealedPath(t.TempDir(), 1)
			if err := os.WriteFile(path, b, 0o644); err != nil {
				t.Fatal(err)
			}
			s, err := segindex.OpenSealed(path)
			if err == nil {
				_ = s.Close()
				t.Fatal("OpenSealed aceitou o header mutilado")
			}
			if !errors.Is(err, segindex.ErrFormat) {
				t.Errorf("erro = %v, quero ErrFormat", err)
			}
		})
	}
}

// TestSeladoRecusaTabelaDeSecoesForaDeOrdem é o guarda que protege contra
// FALSO NEGATIVO, e por isso ganha teste próprio em vez de virar mais uma
// linha do mapa acima.
//
// A busca binária de secao() tem a ordenação por fieldID como pré-condição.
// Sem o guarda, uma tabela desordenada não derrubaria nada e não corromperia
// memória nenhuma: ela simplesmente faria o Lookup NÃO ACHAR um campo que está
// no arquivo, o segmento responderia "zero linhas" onde tem linhas, e a
// consulta devolveria menos resultados do que deveria — em silêncio, para
// sempre. É o único desfecho que este produto não tolera.
//
// Precisa de duas seções, o que o corpus de um campo não dá.
func TestSeladoRecusaTabelaDeSecoesForaDeOrdem(t *testing.T) {
	e := segindex.NewWriter()
	e.Add(3, 999, 12)
	e.Add(7, 100, 10)
	sourcePath := segindex.SealedPath(t.TempDir(), 1)
	if err := e.WriteSealed(sourcePath, 1); err != nil {
		t.Fatalf("WriteSealed: %v", err)
	}
	original, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}

	// Só os fieldIDs são trocados: os offsets das duas seções continuam
	// válidos, então a recusa só pode vir do guarda de ordenação.
	const offField0 = segindex.SealedHeaderSize
	const offField1 = segindex.SealedHeaderSize + segindex.SectionEntrySize
	cases := map[string][2]uint16{
		"decrescente": {7, 3},
		"repetido":    {7, 7},
	}
	for name, fields := range cases {
		t.Run(name, func(t *testing.T) {
			b := append([]byte(nil), original...)
			binary.LittleEndian.PutUint16(b[offField0:offField0+2], fields[0])
			binary.LittleEndian.PutUint16(b[offField1:offField1+2], fields[1])
			path := segindex.SealedPath(t.TempDir(), 1)
			if err := os.WriteFile(path, b, 0o644); err != nil {
				t.Fatal(err)
			}
			s, err := segindex.OpenSealed(path)
			if err == nil {
				_ = s.Close()
				t.Fatal("OpenSealed aceitou a tabela de seções fora de ordem")
			}
			if !errors.Is(err, segindex.ErrFormat) {
				t.Errorf("erro = %v, quero ErrFormat", err)
			}
		})
	}
}

// rewriteCRC recalcula o crc32c sobre [0, EOF-4) e o grava nos últimos quatro
// bytes.
//
// A tabela é montada AQUI, e não emprestada do pacote, porque um teste que
// reutilizasse a variável de produção não estaria conferindo o algoritmo — só
// que o arquivo é consistente consigo mesmo.
func rewriteCRC(b []byte) []byte {
	body := b[:len(b)-segindex.CRCSize]
	sum := crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(b[len(b)-segindex.CRCSize:], sum)
	return b
}

// TestPostingsForaDaFaixaDegradamEmVezDePanicar cobre o último guarda do
// caminho de leitura, e é o único que separa "resposta degradada" de PÂNICO DE
// SLICE — que no caminho de consulta derruba o processo, não a query.
//
// O caso que sobra para ele é o ESCRITOR COM BUG: um postIdx ou um count
// inválido gravado sobre bytes que o crc cobre corretamente. Por isso cada
// mutilação vem com o crc REGRAVADO — sem isso o Lookup sairia antes, no
// checkCRC, e o teste estaria medindo o guarda errado. A asserção de que
// IntegrityErr() continua nil é o que prova que foi este guarda, e não o
// checksum, que pegou.
func TestPostingsForaDaFaixaDegradamEmVezDePanicar(t *testing.T) {
	cases := map[string]func(b []byte){
		"count zero":            func(b []byte) { putU32(b, offCount, 0) },
		"count além das 3 rows": func(b []byte) { putU32(b, offCount, 999) },
		"postIdx além do fim":   func(b []byte) { putU32(b, offPostIdx, 5) },
		"postIdx e count estourando juntos": func(b []byte) {
			putU32(b, offPostIdx, 2)
			putU32(b, offCount, 3) // 2+3 = 5 > 3 postings
		},
	}
	original, err := os.ReadFile(writeTestSealed(t))
	if err != nil {
		t.Fatal(err)
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			b := append([]byte(nil), original...)
			corrupt(b)
			path := segindex.SealedPath(t.TempDir(), 1)
			if err := os.WriteFile(path, rewriteCRC(b), 0o644); err != nil {
				t.Fatal(err)
			}
			s, err := segindex.OpenSealed(path)
			if err != nil {
				t.Fatalf("OpenSealed: %v — o diretório só é lido na consulta, não na abertura", err)
			}
			defer func() {
				if err := s.Close(); err != nil {
					t.Errorf("Close: %v", err)
				}
			}()
			if _, ok := s.Lookup(7, 100); ok {
				t.Error("Lookup sobre postings fora da faixa devolveu ok=true")
			}
			if err := s.IntegrityErr(); err != nil {
				t.Errorf("IntegrityErr() = %v, quero nil — o crc está correto, quem recusou tem de ser o guarda de faixa", err)
			}
		})
	}
}

// TestLookupAposFechaPanicaEmVezDeLerMemoriaDesmontada trava a rede do nível
// de baixo. As fontes leem o mmap direto, sem cópia — é o que as torna
// gratuitas —, então consultar depois do Close leria memória já desmontada, e
// o desfecho seria SIGSEGV: sinal incapturável, sem nome de arquivo, longe da
// linha que errou. O pânico nomeia o método e o arquivo, e ainda é conversível
// em erro na fronteira da requisição.
//
// A garantia de que isso nunca acontece continua sendo do dono, que faz
// quiesce antes de fechar. Isto é o que sobra para o dia em que um caminho
// novo escapar.
func TestLookupAposFechaPanicaEmVezDeLerMemoriaDesmontada(t *testing.T) {
	s, err := segindex.OpenSealed(writeTestSealed(t))
	if err != nil {
		t.Fatalf("OpenSealed: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	defer func() {
		if recover() == nil {
			t.Error("Lookup após Close deveria entrar em pânico")
		}
	}()
	s.Lookup(7, 100)
}

// openSealedWithNPostings monta um selado com um campo, um valor e n postings
// consecutivas. É o corpus do gate de alocação, onde o que importa é ter
// linhas suficientes para que uma cópia por linha apareça na média.
func openSealedWithNPostings(t *testing.T, n int) *segindex.Sealed {
	t.Helper()
	e := segindex.NewWriter()
	for i := range n {
		e.Add(7, 100, types.RowID(i))
	}
	path := segindex.SealedPath(t.TempDir(), 1)
	if err := e.WriteSealed(path, 1); err != nil {
		t.Fatalf("WriteSealed: %v", err)
	}
	s, err := segindex.OpenSealed(path)
	if err != nil {
		t.Fatalf("OpenSealed: %v", err)
	}
	return s
}
