package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/storage/rowindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// histIndexado é o parComRetencao COM índice por segmento: sem campo indexado
// não nasce .sidx, e sem .sidx a janela não tem o que expulsar.
//
// O segmento é pequeno de propósito — a janela só tem sentido com vários
// segmentos selados, e a rotação é o que os produz dentro de um teste.
func histIndexado(t *testing.T, segSize int64) (*Store, *relogioFalso) {
	t.Helper()
	base := openStore(t, Options{})
	hist := openStore(t, Options{
		Dir:                    t.TempDir(),
		Dataset:                "events_history",
		SegmentSize:            segSize,
		Modo:                   ModoHistorico,
		DictCompartilhado:      base.FieldDict(),
		CampoDeEscrita:         "_writtenAt",
		CamposIndexadosPorNome: []string{"status"},
	})
	rel := &relogioFalso{t: time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)}
	hist.UsaRelogio(rel.agora)
	return hist, rel
}

// segmentosComIndice lista, em ordem, os segmentos que têm .sidx em disco. É o
// oráculo de toda esta suíte: o que a expulsão devolve é arquivo de índice.
func segmentosComIndice(t *testing.T, s *Store) []uint32 {
	t.Helper()
	refs, err := s.listaSegmentos()
	if err != nil {
		t.Fatalf("listaSegmentos: %v", err)
	}
	var out []uint32
	for _, ref := range refs {
		if _, err := os.Stat(segindex.CaminhoSelado(s.dirDeDados(), ref.ID)); err == nil {
			out = append(out, ref.ID)
		}
	}
	return out
}

// faixasDosSegmentos devolve o primeiro e o último rowId de cada segmento,
// lidos das locations. O teste precisa disso para saber ONDE cai a fronteira —
// e derivá-lo do locations.idx, e não do código da janela, é o que impede o
// teste de concordar com a implementação por construção.
func faixasDosSegmentos(t *testing.T, s *Store) map[uint32][2]types.RowID {
	t.Helper()
	faixas := map[uint32][2]types.RowID{}
	for id := uint64(s.RowIDBase()); id < s.nextRowID.Load(); id++ {
		packed, ok := s.locs.Get(types.RowID(id))
		if !ok {
			continue
		}
		seg, _, _ := rowindex.Unpack(packed)
		faixa, vista := faixas[seg]
		if !vista {
			faixa[0] = types.RowID(id)
		}
		faixa[1] = types.RowID(id)
		faixas[seg] = faixa
	}
	return faixas
}

// apendaComStatus apenda uma linha com o `status` dado — o campo INDEXADO desta
// suíte.
//
// Ele existe para produzir VARIEDADE de tamanho de .sidx: o índice de um segmento
// custa 16 bytes por valor distinto mais 4 por linha (§1.3), então um segmento
// com status todo diferente tem índice vários blocos maior que um de status
// constante. Sem essa variedade, `maior` é invariante à expulsão e NENHUMA
// escolha de estimador é distinguível — o teste do moinho aprovaria qualquer
// desenho.
func apendaComStatus(t *testing.T, hist *Store, status string, quando time.Time) types.RowID {
	t.Helper()
	ids := map[string]types.FieldID{}
	for _, n := range []string{"id", "status", "counter", "_writtenAt"} {
		id, err := hist.FieldDict().LookupOrCreate([]byte(n))
		if err != nil {
			t.Fatalf("LookupOrCreate(%s): %v", n, err)
		}
		ids[n] = id
	}
	type campo struct {
		id types.FieldID
		v  types.Value
	}
	campos := []campo{
		{ids["id"], types.Value{Type: types.TypeString, Bytes: []byte("a")}},
		{ids["status"], types.Value{Type: types.TypeString, Bytes: []byte(status)}},
		{ids["_writtenAt"], types.Value{Type: types.TypeTimestamp, I64: quando.UnixMilli()}},
	}
	slices.SortFunc(campos, func(a, b campo) int { return int(a.id) - int(b.id) })
	var p []byte
	for _, c := range campos {
		p = mustAppendField(t, p, c.id, c.v)
	}
	id, err := hist.Apenda(p)
	if err != nil {
		t.Fatalf("Apenda: %v", err)
	}
	return id
}

// povoaHistorico apenda n linhas espaçadas de passo, e devolve o instante de
// cada rowId.
func povoaHistorico(t *testing.T, hist *Store, rel *relogioFalso, n int, passo time.Duration) map[types.RowID]time.Time {
	t.Helper()
	tempos := map[types.RowID]time.Time{}
	for range n {
		tempos[apenda(t, hist, "a", rel.t)] = rel.t
		rel.avanca(passo)
	}
	return tempos
}

// aplicaJanela roda uma expulsão e falha o teste no erro.
func aplicaJanela(t *testing.T, s *Store, j JanelaDeIndice) int {
	t.Helper()
	n, err := s.AplicaJanela(t.Context(), j)
	if err != nil {
		t.Fatalf("AplicaJanela(%+v): %v", j, err)
	}
	return n
}

func pendentes(t *testing.T, s *Store, j JanelaDeIndice) []uint32 {
	t.Helper()
	out, err := s.SegmentosPendentesDeReconstrucao(j)
	if err != nil {
		t.Fatalf("SegmentosPendentesDeReconstrucao(%+v): %v", j, err)
	}
	return out
}

// selados devolve os segmentos selados (todos menos o corrente), em ordem.
func selados(t *testing.T, s *Store) []uint32 {
	t.Helper()
	refs, err := s.listaSegmentos()
	if err != nil {
		t.Fatalf("listaSegmentos: %v", err)
	}
	corrente := s.curSegID.Load()
	var out []uint32
	for _, ref := range refs {
		if ref.ID != corrente {
			out = append(out, ref.ID)
		}
	}
	return out
}

// TestFronteiraDaJanelaUsaORecordMaisNovoDoSegmento é a assimetria que decide a
// correção da janela.
//
// A janela promete "consigo consultar rápido as últimas N horas". Um segmento
// que ATRAVESSA a fronteira contém records que a promessa cobre, então ele
// mantém o índice — e a pergunta que decide isso é sobre o record MAIS NOVO
// dele. Perguntar pelo mais antigo tiraria o índice de dados que a janela
// promete servir rápido, que é falso negativo de desempenho sem aviso nenhum.
//
// É a assimetria espelhada da retenção, que só aposenta o segmento
// INTEIRAMENTE coberto pela marca-d'água (candidatoDeRetencao).
func TestFronteiraDaJanelaUsaORecordMaisNovoDoSegmento(t *testing.T) {
	hist, rel := histIndexado(t, 512)
	tempos := povoaHistorico(t, hist, rel, 40, time.Minute)

	sel := selados(t, hist)
	if len(sel) < 3 {
		t.Fatalf("%d segmentos selados; o teste precisa de rotação", len(sel))
	}
	faixas := faixasDosSegmentos(t, hist)
	alvo := sel[1] // um selado com vizinho mais velho e vizinho mais novo
	faixa := faixas[alvo]
	if faixa[1]-faixa[0] < 2 {
		t.Fatalf("o segmento %d tem %d linhas; o teste precisa de um que ATRAVESSE a fronteira",
			alvo, faixa[1]-faixa[0]+1)
	}
	// O corte cai DENTRO do alvo: depois do primeiro record dele e antes do
	// último. Pelo mais novo ele fica; pelo mais antigo ele sairia.
	corte := tempos[faixa[0]].Add(time.Minute)
	if !corte.After(tempos[faixa[0]]) || !corte.Before(tempos[faixa[1]]) {
		t.Fatalf("o corte %s não atravessa o segmento %d (%s..%s)",
			corte, alvo, tempos[faixa[0]], tempos[faixa[1]])
	}
	const janela = time.Hour
	rel.t = corte.Add(janela)

	aplicaJanela(t, hist, JanelaDeIndice{Duracao: janela})

	comIndice := segmentosComIndice(t, hist)
	if !slices.Contains(comIndice, alvo) {
		t.Fatalf("o segmento %d atravessa a fronteira e perdeu o índice (com índice: %v); "+
			"a fronteira está olhando o record mais ANTIGO", alvo, comIndice)
	}
	// E o vizinho mais velho, inteiramente do outro lado, perdeu o dele.
	velho := sel[0]
	if slices.Contains(comIndice, velho) {
		t.Fatalf("o segmento %d está inteiro fora da janela e manteve o índice (com índice: %v)",
			velho, comIndice)
	}
}

// Estreitar a janela devolve disco: os .sidx que saíram são expulsos na passada
// seguinte.
func TestEstreitarAJanelaExpulsaOsIndicesQueSairam(t *testing.T) {
	hist, rel := histIndexado(t, 512)
	povoaHistorico(t, hist, rel, 40, time.Minute)

	antes := segmentosComIndice(t, hist)
	if len(antes) < 3 {
		t.Fatalf("%d segmentos com índice; o teste precisa de rotação", len(antes))
	}
	// Janela larga: nada sai.
	if n := aplicaJanela(t, hist, JanelaDeIndice{Duracao: 24 * time.Hour}); n != 0 {
		t.Fatalf("a janela larga expulsou %d índices; ela cobre o dataset inteiro", n)
	}
	if got := segmentosComIndice(t, hist); !slices.Equal(got, antes) {
		t.Fatalf("com índice = %v, quer %v: a janela larga não pode tirar nada", got, antes)
	}
	// Estreitar até cobrir só os últimos minutos.
	expulsos := aplicaJanela(t, hist, JanelaDeIndice{Duracao: 5 * time.Minute})
	if expulsos == 0 {
		t.Fatal("estreitar a janela não expulsou nenhum índice")
	}
	depois := segmentosComIndice(t, hist)
	if len(depois) >= len(antes) {
		t.Fatalf("%d segmentos com índice depois, %d antes: a janela estreitou e nada saiu",
			len(depois), len(antes))
	}
	if len(depois)+expulsos != len(antes) {
		t.Fatalf("%d expulsos não explicam a diferença entre %d e %d",
			expulsos, len(antes), len(depois))
	}
}

// Fora da janela NÃO é pendente — é deliberadamente sem índice.
//
// Sem esta regra, o laço de reconstrução (§2.4) refaria eternamente o índice de
// um segmento que a janela acabou de expulsar. É o moinho, e é a razão de a
// pendência EXIGIR a janela como parâmetro: o compilador cobra de quem montar o
// laço o que um comentário não cobraria.
func TestSegmentoForaDaJanelaNaoEhPendente(t *testing.T) {
	hist, rel := histIndexado(t, 512)
	povoaHistorico(t, hist, rel, 40, time.Minute)

	estreita := JanelaDeIndice{Duracao: 5 * time.Minute}
	if aplicaJanela(t, hist, estreita) == 0 {
		t.Fatal("estreitar a janela não expulsou nenhum índice")
	}
	if fila := pendentes(t, hist, estreita); len(fila) != 0 {
		t.Fatalf("pendentes = %v, quer vazio: o que a janela acabou de expulsar não é pendente — "+
			"o laço da §2.4 reconstruiria para a janela expulsar de novo, para sempre", fila)
	}
}

// Alargar volta a indexar, e é por isto que a reconstrução existe: sem ela a
// janela seria um botão de mão única, que só sabe apertar.
func TestAlargarAJanelaAgendaReconstrucao(t *testing.T) {
	hist, rel := histIndexado(t, 512)
	povoaHistorico(t, hist, rel, 40, time.Minute)

	estreita := JanelaDeIndice{Duracao: 5 * time.Minute}
	larga := JanelaDeIndice{Duracao: 24 * time.Hour}

	antes := segmentosComIndice(t, hist)
	if aplicaJanela(t, hist, estreita) == 0 {
		t.Fatal("estreitar a janela não expulsou nenhum índice")
	}
	fila := pendentes(t, hist, larga)
	if len(fila) == 0 {
		t.Fatal("alargar a janela não agendou reconstrução nenhuma: a janela seria um botão de mão única")
	}
	for _, seg := range fila {
		if err := hist.ConstroiIndiceDoSegmento(t.Context(), seg, hist.CamposIndexados()); err != nil {
			t.Fatalf("ConstroiIndiceDoSegmento(%d): %v", seg, err)
		}
	}
	if fila := pendentes(t, hist, larga); len(fila) != 0 {
		t.Fatalf("pendentes = %v depois de reconstruir: a fila não esvaziou", fila)
	}
	if depois := segmentosComIndice(t, hist); !slices.Equal(depois, antes) {
		t.Fatalf("com índice = %v depois de alargar e reconstruir, quer %v", depois, antes)
	}
}

// O teto de bytes é a política FORTE: ele engole o prazo declarado, exatamente
// como o history.maxBytes engole o expireAfterWrite na retenção. E quem mostra
// isso é a JanelaEfetiva — sem ela o operador leria `window: 24h` no YAML e não
// teria como saber que o índice cobre muito menos.
func TestOTetoDeBytesVenceOPrazoDeclarado(t *testing.T) {
	hist, rel := histIndexado(t, 512)
	povoaHistorico(t, hist, rel, 40, time.Minute)

	const declarada = 24 * time.Hour
	semTeto := JanelaDeIndice{Duracao: declarada}
	if n := aplicaJanela(t, hist, semTeto); n != 0 {
		t.Fatalf("a janela sem teto expulsou %d índices", n)
	}
	cobertaSemTeto := hist.JanelaEfetiva()
	if cobertaSemTeto <= 0 {
		t.Fatal("JanelaEfetiva = 0 com o dataset inteiro indexado")
	}

	// Teto de UM índice: o mais novo cabe, o resto não.
	teto := maiorIndiceEmDisco(t, hist)
	if teto == 0 {
		t.Fatal("nenhum .sidx em disco")
	}
	comTeto := JanelaDeIndice{Duracao: declarada, MaxBytes: teto}
	if n := aplicaJanela(t, hist, comTeto); n == 0 {
		t.Fatal("o teto de bytes não expulsou nada")
	}
	cobertaComTeto := hist.JanelaEfetiva()
	if cobertaComTeto >= cobertaSemTeto {
		t.Fatalf("JanelaEfetiva = %s com teto e %s sem teto: o teto não estreitou a cobertura",
			cobertaComTeto, cobertaSemTeto)
	}
	// E quem mandou foi o TETO, não o relógio: a janela declarada é maior que a
	// cobertura que o dataset inteiro daria, então o prazo não cortou nada. É
	// esta a comparação que o operador faz no painel.
	if cobertaSemTeto >= declarada {
		t.Fatalf("a cobertura sem teto (%s) alcançou a janela declarada (%s): o teste não separa "+
			"o corte por bytes do corte por tempo", cobertaSemTeto, declarada)
	}
}

// Um teto menor que um único índice não pode zerar o índice do dataset: o mais
// novo dentro da janela por tempo nunca sai só por bytes.
//
// É o espelho do "só sobrou o segmento corrente" da retenção, que fica acima do
// teto quando não há mais o que aposentar. Sem esta regra, um maxBytes
// digitado pequeno demais transformaria a janela em desligamento total do
// índice — e o operador leria um teto no YAML enquanto o dataset inteiro
// enumera.
func TestOMaisNovoNuncaSaiSoPorBytes(t *testing.T) {
	hist, rel := histIndexado(t, 512)
	povoaHistorico(t, hist, rel, 40, time.Minute)

	// Um byte de teto: nem o menor .sidx cabe.
	aplicaJanela(t, hist, JanelaDeIndice{MaxBytes: 1})
	comIndice := segmentosComIndice(t, hist)
	if len(comIndice) != 1 {
		t.Fatalf("com índice = %v, quer exatamente o mais novo selado: um teto impossível de "+
			"perseguir não pode zerar o índice do dataset", comIndice)
	}
	sel := selados(t, hist)
	if comIndice[0] != sel[len(sel)-1] {
		t.Fatalf("sobrou o índice do segmento %d, quer o mais novo selado (%d)",
			comIndice[0], sel[len(sel)-1])
	}
	// E ele não é pendente: a expulsão e a pendência concordam sobre quem fica.
	if fila := pendentes(t, hist, JanelaDeIndice{MaxBytes: 1}); len(fila) != 0 {
		t.Fatalf("pendentes = %v sob o teto impossível: a fila discordou da expulsão", fila)
	}
}

// maiorIndiceEmDisco devolve o tamanho do maior .sidx do dataset, em BLOCOS
// alocados.
//
// Em blocos, e não no tamanho aparente, porque é assim que o teto conta — e a
// diferença decide os testes: um .sidx de teste tem centenas de bytes e ocupa
// um bloco inteiro, então um teto calculado sobre o tamanho aparente ficaria
// ABAIXO do custo de um único índice e expulsaria tudo, fazendo os testes
// passarem por um motivo que não é o que eles afirmam.
func maiorIndiceEmDisco(t *testing.T, s *Store) int64 {
	t.Helper()
	var maior int64
	for _, seg := range segmentosComIndice(t, s) {
		info, err := os.Stat(segindex.CaminhoSelado(s.dirDeDados(), seg))
		if err != nil {
			t.Fatalf("Stat(%d): %v", seg, err)
		}
		if b := blocosDe(info); b > maior {
			maior = b
		}
	}
	return maior
}

// TestAJanelaConvergeEmVezDeMoer é o teste do MOINHO, e ele é o motivo de a
// cobrança do segmento não construído existir e de ela ser MONÓTONA.
//
// O fixture tem VARIEDADE de propósito: o segmento mais velho tem índice
// atípico (status todo distinto) e os outros quatro têm índice pequeno. Sem
// isso, `maior` seria invariante à expulsão e NENHUMA escolha de estimador
// seria distinguível — o teste aprovaria qualquer desenho, inclusive os dois
// defeituosos que ele existe para reprovar.
//
// O traço, com os números que o fixture produz (atípico 20K, pequenos 8K cada,
// teto = soma dos pequenos + uma vaga de 8K):
//
//	passada 1   maior=20K → os quatro pequenos cabem (32K ≤ 40K)
//	                      → o atípico não (32K+20K > 40K) → EXPULSO
//	pendência   maior=8K  ← o de 20K ACABOU de ser apagado
//	                      → cobrado max(8K, tamanhoExpulso=20K) = 20K
//	                      → 32K+20K > 40K → FORA. Não é pendente.
//	passada 2   nada sai. Ponto fixo.
//
// Ele DISCRIMINA os dois desenhos errados, e a vaga de folga no teto é o que
// torna isso possível:
//
//   - cobrar ZERO pelo não construído → 32K+0 ≤ 40K → readmitido;
//   - cobrar só o MAIOR EM DISCO → 32K+8K = 40K ≤ 40K → readmitido.
//
// Nos dois casos o segmento recém-expulso volta para a fila, é reconstruído,
// volta a ocupar 20K e é expulso de novo. Para sempre.
func TestAJanelaConvergeEmVezDeMoer(t *testing.T) {
	hist, rel := histIndexado(t, 64<<10)
	povoaComAtipicoNoMaisVelho(t, hist, rel, 5)

	sel := selados(t, hist)
	if len(sel) < 3 {
		t.Fatalf("%d segmentos selados; o teste precisa de uma pilha", len(sel))
	}
	atipico := sel[0]
	tamanhoAtipico := hist.bytesDoIndiceDoSegmento(atipico)
	var somaPequenos, maiorPequeno int64
	for _, seg := range sel[1:] {
		b := hist.bytesDoIndiceDoSegmento(seg)
		somaPequenos += b
		maiorPequeno = max(maiorPequeno, b)
	}
	// A precondição é o teste do próprio fixture: sem um índice maior que os
	// outros, os três estimadores dariam a mesma resposta e o oráculo abaixo
	// não provaria nada.
	if tamanhoAtipico <= maiorPequeno {
		t.Fatalf("o segmento mais velho tem índice de %d bytes e o maior dos outros tem %d: "+
			"o fixture não produziu variedade, e sem ela nenhum estimador é distinguível",
			tamanhoAtipico, maiorPequeno)
	}
	// Teto com UMA vaga de folga além dos pequenos: é ela que um estimador
	// subestimado usaria para readmitir o atípico.
	j := JanelaDeIndice{MaxBytes: somaPequenos + maiorPequeno}

	if n := aplicaJanela(t, hist, j); n == 0 {
		t.Fatal("o teto de bytes não expulsou nada; o moinho não pode nem começar")
	}
	primeira := segmentosComIndice(t, hist)
	if slices.Contains(primeira, atipico) {
		t.Fatalf("com índice = %v: o atípico %d não cabia no teto e continuou indexado", primeira, atipico)
	}
	if fila := pendentes(t, hist, j); len(fila) != 0 {
		t.Fatalf("pendentes = %v logo depois da expulsão: o que o teto acabou de expulsar não é "+
			"pendente — cobrá-lo por menos do que ele custou o readmite, e o laço da §2.4 o "+
			"reconstrói para a passada seguinte expulsá-lo de novo", fila)
	}

	// Segunda rodada: nada mais sai, e nada volta para a fila. É a definição de
	// convergência — o conjunto é ponto fixo.
	if n := aplicaJanela(t, hist, j); n != 0 {
		t.Fatalf("a segunda passada expulsou mais %d índices: o conjunto não é ponto fixo", n)
	}
	segunda := segmentosComIndice(t, hist)
	if !slices.Equal(segunda, primeira) {
		t.Fatalf("com índice = %v na segunda rodada, %v na primeira: a janela está moendo",
			segunda, primeira)
	}
	if fila := pendentes(t, hist, j); len(fila) != 0 {
		t.Fatalf("pendentes = %v na segunda rodada: o segmento que o teto expulsou voltou para a fila", fila)
	}
}

// Alargar o teto traz de volta o segmento que a memória de expulsão mantinha
// fora: a cobrança monótona não pode virar catraca.
//
// É o par obrigatório do teste acima. Um estimador que só soubesse EXCLUIR
// passaria naquele e falharia aqui — e a reversibilidade é metade do que a §4.4
// promete.
func TestOTamanhoLembradoNaoViraCatraca(t *testing.T) {
	hist, rel := histIndexado(t, 64<<10)
	povoaComAtipicoNoMaisVelho(t, hist, rel, 5)

	sel := selados(t, hist)
	atipico := sel[0]
	tamanhoAtipico := hist.bytesDoIndiceDoSegmento(atipico)
	var somaPequenos, maiorPequeno int64
	for _, seg := range sel[1:] {
		b := hist.bytesDoIndiceDoSegmento(seg)
		somaPequenos += b
		maiorPequeno = max(maiorPequeno, b)
	}
	apertado := JanelaDeIndice{MaxBytes: somaPequenos + maiorPequeno}
	if n := aplicaJanela(t, hist, apertado); n == 0 {
		t.Fatal("o teto apertado não expulsou nada")
	}
	// Agora o teto comporta todo mundo, inclusive o atípico pelo tamanho REAL
	// que a expulsão lembrou.
	folgado := JanelaDeIndice{MaxBytes: somaPequenos + tamanhoAtipico}
	fila := pendentes(t, hist, folgado)
	if !slices.Contains(fila, atipico) {
		t.Fatalf("pendentes = %v: subir o teto não trouxe o segmento %d de volta; "+
			"a cobrança lembrada virou catraca", fila, atipico)
	}
}

// Janela infinita é o default e o comportamento de sempre: indexa tudo que a
// retenção mantém. É o que vale no dataset materializado, onde a janela não tem
// sentido — a linha vive no segmento corrente independente da idade dela.
func TestJanelaInfinitaNaoExpulsaNada(t *testing.T) {
	hist, rel := histIndexado(t, 512)
	povoaHistorico(t, hist, rel, 40, time.Minute)
	rel.avanca(30 * 24 * time.Hour) // muito além de qualquer prazo

	antes := segmentosComIndice(t, hist)
	if n := aplicaJanela(t, hist, JanelaDeIndice{}); n != 0 {
		t.Fatalf("a janela infinita expulsou %d índices", n)
	}
	if depois := segmentosComIndice(t, hist); !slices.Equal(depois, antes) {
		t.Fatalf("com índice = %v, quer %v", depois, antes)
	}
	if !(JanelaDeIndice{}).Infinita() {
		t.Fatal("a janela zerada precisa ser infinita: é o bloco index: ausente")
	}
	if (JanelaDeIndice{MaxBytes: 1}).Infinita() {
		t.Fatal("um teto declarado não é janela infinita")
	}
}

// A janela é EXCLUSIVA do histórico. Pedi-la sobre um dataset materializado é
// erro de chamador, não configuração a ignorar: no materializado a linha vive
// no segmento corrente independente da idade, e cortar por tempo ali tiraria o
// índice de dado corrente.
func TestJanelaFinitaSobreMaterializadoEhRecusada(t *testing.T) {
	s := abreStoreIndexadoEm(t, t.TempDir(), []string{"status"}, 512)
	if _, err := s.AplicaJanela(t.Context(), JanelaDeIndice{Duracao: time.Hour}); err == nil {
		t.Fatal("a janela finita foi aceita sobre um dataset materializado")
	}
	// E a infinita passa sem erro: é o default de todo dataset materializado.
	if n, err := s.AplicaJanela(t.Context(), JanelaDeIndice{}); err != nil || n != 0 {
		t.Fatalf("AplicaJanela(infinita) = %d, %v", n, err)
	}
}

// A passada de retenção é quem aplica a janela: um laço de fundo a menos. Ela
// já roda a cada minuto, já é exclusiva do histórico e já tem a lista de
// segmentos na mão.
func TestAPassadaDeRetencaoAplicaAJanela(t *testing.T) {
	hist, rel := histIndexado(t, 512)
	povoaHistorico(t, hist, rel, 40, time.Minute)

	antes := segmentosComIndice(t, hist)
	res := passada(t, hist, LimitesDeRetencao{
		MaxBytes: 1 << 30, // teto de dado folgado: nada é aposentado
		Janela:   JanelaDeIndice{Duracao: 5 * time.Minute},
	})
	if res.IndicesExpulsos == 0 {
		t.Fatal("a passada de retenção não aplicou a janela")
	}
	depois := segmentosComIndice(t, hist)
	if len(depois) >= len(antes) {
		t.Fatalf("%d segmentos com índice depois, %d antes", len(depois), len(antes))
	}
	// E o DADO continua no lugar: a janela governa índice, não retenção.
	if n := len(selados(t, hist)); n != len(antes) {
		t.Fatalf("%d segmentos selados depois da janela, %d antes: a janela apagou dado", n, len(antes))
	}
}

// A aposentadoria de um segmento leva o .sidx junto, então a janela nunca
// enxerga índice de segmento que já não existe. Este teste protege a ordem: a
// janela roda DEPOIS das aposentadorias na mesma passada.
func TestAJanelaNaoDeixaIndiceDeSegmentoAposentado(t *testing.T) {
	hist, rel := histIndexado(t, 512)
	povoaHistorico(t, hist, rel, 40, time.Minute)
	rel.avanca(10 * time.Hour)

	res := passada(t, hist, LimitesDeRetencao{
		ExpireAfterWrite: time.Hour,
		Janela:           JanelaDeIndice{Duracao: 24 * time.Hour},
	})
	if res.PorTempo == 0 {
		t.Fatal("nenhum segmento foi aposentado; o teste precisa da purga")
	}
	vivos := selados(t, hist)
	for _, seg := range segmentosComIndice(t, hist) {
		if !slices.Contains(vivos, seg) && seg != hist.curSegID.Load() {
			t.Fatalf("sobrou .sidx do segmento aposentado %d (vivos: %v)", seg, vivos)
		}
	}
	// E o diretório não guarda .sidx órfão nenhum.
	entradas, err := os.ReadDir(filepath.Join(hist.opts.Dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entradas {
		if filepath.Ext(e.Name()) != ".sidx" {
			continue
		}
		if _, err := os.Stat(filepath.Join(hist.opts.Dir, "data",
			e.Name()[:len(e.Name())-len(".sidx")]+".dat")); err != nil {
			t.Fatalf("o índice %s não tem .dat", e.Name())
		}
	}
}

// povoaComAtipicoNoMaisVelho monta o dataset que o teste do moinho exige: o
// segmento MAIS VELHO com índice atípico (status todo distinto) e os demais com
// índice pequeno (status constante).
//
// A fase 1 termina pela ROTAÇÃO observada, e não por uma contagem de linhas
// calculada à mão: o tamanho do record é detalhe de codificação, e amarrar o
// fixture a ele o quebraria no dia em que um campo mudasse de largura.
func povoaComAtipicoNoMaisVelho(t *testing.T, hist *Store, rel *relogioFalso, selados uint32) {
	t.Helper()
	primeiro := hist.curSegID.Load()
	for i := 0; hist.curSegID.Load() == primeiro; i++ {
		apendaComStatus(t, hist, fmt.Sprintf("valor-%08d", i), rel.t)
		rel.avanca(time.Second)
	}
	for hist.curSegID.Load() < primeiro+selados {
		apendaComStatus(t, hist, "aberto", rel.t)
		rel.avanca(time.Second)
	}
}

// No bootstrap — nenhum .sidx em disco, nada lembrado — o teto admite SÓ O MAIS
// NOVO, e não o dataset inteiro por "custar zero".
//
// A diferença decide a MIGRAÇÃO, que é o gatilho 1 da §2.4: um dataset com
// milhares de selados aberto pela primeira vez com este formato não tem índice
// nenhum. Admitindo todos, o laço da §2.4 varreria o dataset inteiro — centenas
// de MiB por segmento — antes de a primeira expulsão cortar. Admitindo um, a
// passada seguinte já tem a medição e admite quantos couberem de verdade.
func TestNoBootstrapOTetoAdmiteSoOMaisNovo(t *testing.T) {
	hist, rel := histIndexado(t, 64<<10)
	povoaComAtipicoNoMaisVelho(t, hist, rel, 5)

	sel := selados(t, hist)
	// O dataset migrado: nenhum .sidx em disco, e nada lembrado em memória.
	for _, seg := range sel {
		if err := os.Remove(segindex.CaminhoSelado(hist.dirDeDados(), seg)); err != nil {
			t.Fatalf("Remove(%d): %v", seg, err)
		}
		hist.esqueceSelado(seg)
	}
	if n := len(segmentosComIndice(t, hist)); n != 0 {
		t.Fatalf("%d índices em disco; o teste precisa do dataset sem índice nenhum", n)
	}

	fila := pendentes(t, hist, JanelaDeIndice{MaxBytes: 1 << 30})
	if len(fila) != 1 {
		t.Fatalf("pendentes = %v, quer só o mais novo selado: sem nada medido, o teto não pode "+
			"admitir o dataset inteiro — o laço da §2.4 varreria tudo antes do primeiro corte", fila)
	}
	if fila[0] != sel[len(sel)-1] {
		t.Fatalf("pendente = %d, quer o mais novo selado (%d)", fila[0], sel[len(sel)-1])
	}
	// Construído o primeiro, a medição existe e a passada seguinte admite o
	// resto: o bootstrap atrasa UMA passada, não trava a fila.
	if err := hist.ConstroiIndiceDoSegmento(t.Context(), fila[0], hist.CamposIndexados()); err != nil {
		t.Fatalf("ConstroiIndiceDoSegmento: %v", err)
	}
	if n := len(pendentes(t, hist, JanelaDeIndice{MaxBytes: 1 << 30})); n != len(sel)-1 {
		t.Fatalf("%d pendentes depois da primeira medição, quer %d: o bootstrap travou a fila",
			n, len(sel)-1)
	}
}
