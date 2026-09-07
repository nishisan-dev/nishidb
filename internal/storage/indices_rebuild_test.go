package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/storage/segment"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// datasetComPendencias monta o estado que o débito 22 descreve: um dataset
// POVOADO em que o índice é declarado depois, de modo que todo segmento selado
// entra na fila de reconstrução.
//
// É o gatilho 1 da §2.4 (dataset gravado por um binário que não escrevia índice)
// e o 4 (campo acrescentado ao schema) na mesma montagem — eles são o mesmo
// estado em disco, e é isso que faz os quatro gatilhos serem um mecanismo só.
func datasetComPendencias(t *testing.T, segmentos int, campos []string) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	s := abreStoreIndexadoEm(t, dir, nil, 64<<10) // sem campo indexado nenhum
	for range segmentos {
		poeLinhasAteRotacionar(t, s, s.curSegID.Load())
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close da montagem: %v", err)
	}
	return abreStoreIndexadoEm(t, dir, campos, 64<<10), dir
}

// passadaDeReconstrucao roda uma passada e devolve quantos segmentos ela varreu
// — o número que o laço loga e o único observável que distingue "não há o que
// fazer" de "está tudo sendo refeito de novo".
func passadaDeReconstrucao(t *testing.T, s *Store) int {
	t.Helper()
	feitos, err := s.passadaDeReconstrucao(t.Context(), JanelaDeIndice{})
	if err != nil {
		t.Fatalf("passadaDeReconstrucao: %v", err)
	}
	return feitos
}

// TestAReconstrucaoConvergeEDeixaAFilaVazia é o que o débito 22 pedia de ponta a
// ponta: um dataset com pendências chega a fila vazia — e FICA, sem que a
// passada seguinte volte a varrer nada.
//
// As duas asserções são independentes. Só "a fila esvaziou" não prova
// convergência: um laço que refizesse tudo a cada passada também esvaziaria a
// fila, todas as vezes. Só "a segunda passada não varreu nada" também não: uma
// passada que não fizesse nada nunca satisfaria a primeira.
func TestAReconstrucaoConvergeEDeixaAFilaVazia(t *testing.T) {
	s, _ := datasetComPendencias(t, 3, []string{"node"})
	fila := pendentes(t, s, JanelaDeIndice{})
	if len(fila) < 2 {
		t.Fatalf("montagem: %d segmento(s) pendente(s); com menos de dois a passada não exercita a fila", len(fila))
	}

	antes := s.varreduras.Load()
	if feitos := passadaDeReconstrucao(t, s); feitos != len(fila) {
		t.Fatalf("a primeira passada reconstruiu %d de %d segmentos pendentes", feitos, len(fila))
	}
	if lidas := s.varreduras.Load() - antes; lidas != int64(len(fila)) {
		t.Errorf("a primeira passada varreu %d segmentos para %d pendentes: a leitura sequencial é o custo inteiro "+
			"da operação, e pagá-la mais de uma vez por segmento é pagar o dobro", lidas, len(fila))
	}
	if resto := pendentes(t, s, JanelaDeIndice{}); len(resto) != 0 {
		t.Fatalf("a fila não esvaziou: %v continuam pendentes depois de uma passada completa", resto)
	}

	antes = s.varreduras.Load()
	if feitos := passadaDeReconstrucao(t, s); feitos != 0 {
		t.Errorf("a segunda passada reconstruiu %d segmentos sobre uma fila vazia", feitos)
	}
	if lidas := s.varreduras.Load() - antes; lidas != 0 {
		t.Errorf("a segunda passada varreu %d segmentos sobre uma fila vazia: o laço não convergiu", lidas)
	}
}

// TestSegmentoSemCampoIndexadoNaoVoltaParaAFila é o moinho que a fila-
// superconjunto arma, e ele NÃO é hipotético: o Escritor vazio não cria arquivo
// (de propósito — um .sidx vazio faria o segmento passar por indexado), então um
// segmento cujos records não trazem nenhum campo declarado continua "sem seção"
// e volta à fila para sempre.
//
// O que se afirma aqui é que a fila continua a mesma — ela é derivada do disco e
// não tem como deixar de listá-lo — e que o TRABALHO para. Sem isso, o laço
// varreria até 512 MiB por segmento a cada cinco segundos, indefinidamente, sem
// nunca publicar um byte.
//
// A pré-condição é a metade que impede o teste de passar por vacuidade: se a
// fila esvaziasse, ele não estaria medindo moinho nenhum.
func TestSegmentoSemCampoIndexadoNaoVoltaParaAFila(t *testing.T) {
	s, _ := datasetComPendencias(t, 2, []string{"campo-que-nenhum-record-tem"})
	fila := pendentes(t, s, JanelaDeIndice{})
	if len(fila) == 0 {
		t.Fatal("montagem: a fila nasceu vazia, e um segmento sem campo indexado tem de nascer pendente")
	}

	antes := s.varreduras.Load()
	if feitos := passadaDeReconstrucao(t, s); feitos != len(fila) {
		t.Fatalf("a primeira passada reconstruiu %d de %d segmentos", feitos, len(fila))
	}
	if lidas := s.varreduras.Load() - antes; lidas != int64(len(fila)) {
		t.Fatalf("a primeira passada varreu %d segmentos para %d pendentes", lidas, len(fila))
	}
	if depois := pendentes(t, s, JanelaDeIndice{}); !slices.Equal(depois, fila) {
		t.Fatalf("a fila mudou de %v para %v: este teste só mede o moinho enquanto o segmento CONTINUA pendente", fila, depois)
	}

	antes = s.varreduras.Load()
	for range 3 {
		if feitos := passadaDeReconstrucao(t, s); feitos != 0 {
			t.Fatalf("uma passada seguinte reconstruiu %d segmentos que já estavam no ponto fixo", feitos)
		}
	}
	if lidas := s.varreduras.Load() - antes; lidas != 0 {
		t.Errorf("três passadas seguintes varreram %d segmentos sem publicar nada: o laço está moendo, e cada volta "+
			"custa a leitura sequencial de todo o segmento", lidas)
	}
}

// TestOPontoFixoCaiQuandoOIndiceSaiDeDisco é a outra metade do ponto fixo, e sem
// ela a memória do teste acima vira falso negativo PERMANENTE.
//
// A montagem é o cruzamento exato dos dois mecanismos, e é o único estado em que
// eles se contradizem: um segmento com DOIS campos declarados, um deles presente
// nos records e o outro em nenhum. Ele fica pendente para sempre (a seção que
// falta não tem como existir), então entra na memória do ponto fixo — e ele TEM
// um .sidx, que a expulsão da janela pode remover. Alargar a janela depois disso
// é o gatilho 2 da §2.4, e uma memória que fosse só um booleano "já tentei"
// deixaria esse segmento sem índice para sempre, com o campo que EXISTE voltando
// a enumerar.
//
// O que se anota, por isso, é o conjunto de seções que o arquivo tem — a memória
// se invalida sozinha quando o disco muda por baixo dela.
func TestOPontoFixoCaiQuandoOIndiceSaiDeDisco(t *testing.T) {
	s, dir := datasetComPendencias(t, 1, []string{"node", "campo-que-nenhum-record-tem"})
	alvo := pendentes(t, s, JanelaDeIndice{})[0]
	if feitos := passadaDeReconstrucao(t, s); feitos != 1 {
		t.Fatalf("a primeira passada reconstruiu %d segmentos, quero 1", feitos)
	}
	if campos := camposDoSelado(t, dir, alvo); len(campos) != 1 {
		t.Fatalf("montagem: o .sidx cobre %v; o teste precisa de um arquivo que EXISTA e continue incompleto", campos)
	}
	if feitos := passadaDeReconstrucao(t, s); feitos != 0 {
		t.Fatalf("a segunda passada refez %d segmentos: sem ponto fixo, este teste não mede a invalidação dele", feitos)
	}

	// A expulsão da janela em forma mínima: o .sidx sai de disco e o segmento
	// continua no dataset.
	if err := os.Remove(segindex.SealedPath(dir+"/data", alvo)); err != nil {
		t.Fatalf("remover o índice do segmento %d: %v", alvo, err)
	}
	if feitos := passadaDeReconstrucao(t, s); feitos != 1 {
		t.Fatalf("a passada depois da expulsão reconstruiu %d segmentos, quero 1: o índice expulso não voltou, e "+
			"alargar a janela seria um botão de mão única", feitos)
	}
	if campos := camposDoSelado(t, dir, alvo); len(campos) != 1 {
		t.Errorf("o índice do segmento %d voltou cobrindo %v", alvo, campos)
	}
}

// TestAReconstrucaoResolveTodosOsCamposPendentesNaMesmaLeitura é a unidade de
// trabalho da §2.4: ela é (segmento, campo), mas o PROCESSAMENTO é por segmento.
// Ler o segmento duas vezes para dois campos pagaria o dobro pelo I/O, que é o
// custo inteiro da operação.
//
// A montagem produz um segmento com a seção de UM campo e sem a do outro — o
// estado da §2.6, um campo acrescentado ao schema depois de o segmento ter sido
// selado —, e o oráculo é o contador de varreduras: uma, para os dois campos.
func TestAReconstrucaoResolveTodosOsCamposPendentesNaMesmaLeitura(t *testing.T) {
	dir := t.TempDir()
	s := abreStoreIndexadoEm(t, dir, []string{"node"}, 64<<10)
	alvo := s.curSegID.Load()
	poeLinhasAteRotacionar(t, s, alvo)
	if err := s.Close(); err != nil {
		t.Fatalf("Close da montagem: %v", err)
	}
	s2 := abreStoreIndexadoEm(t, dir, []string{"node", "type"}, 64<<10)
	if campos := camposDoSelado(t, dir, alvo); len(campos) != 1 {
		t.Fatalf("montagem: o .sidx do segmento %d cobre %v; o teste precisa de um campo presente e outro faltando",
			alvo, campos)
	}

	antes := s2.varreduras.Load()
	if feitos := passadaDeReconstrucao(t, s2); feitos != 1 {
		t.Fatalf("a passada reconstruiu %d segmentos, quero 1", feitos)
	}
	if lidas := s2.varreduras.Load() - antes; lidas != 1 {
		t.Errorf("o segmento foi varrido %d vezes para resolver dois campos pendentes: a leitura sequencial é o "+
			"custo inteiro da operação, e uma por campo a duplica", lidas)
	}
	if campos := camposDoSelado(t, dir, alvo); len(campos) != 2 {
		t.Errorf("o .sidx reconstruído cobre %v, quero os dois campos indexados: reescrever só o campo que faltava "+
			"apagaria a seção que já estava lá", campos)
	}
}

// TestAPassadaDeReconstrucaoNaoTomaOWmu é a exigência da §2.4 depois da emenda:
// a leitura, a extração e a publicação acontecem TODAS fora do lock do writer.
// Sem isso ela vira o terceiro laço de fundo disputando o writer, que é o
// defeito que o ciclo anterior mediu e nomeou.
//
// O oráculo é positivo e não uma espera por travamento: o teste SEGURA o wmu e
// exige que a passada termine assim mesmo. Uma implementação que o tomasse
// (mesmo "só no rename") ficaria presa aqui e o teste falharia por prazo.
func TestAPassadaDeReconstrucaoNaoTomaOWmu(t *testing.T) {
	s, _ := datasetComPendencias(t, 2, []string{"node"})
	if len(pendentes(t, s, JanelaDeIndice{})) == 0 {
		t.Fatal("montagem: sem pendência a passada não tomaria lock nenhum e a asserção passaria por vacuidade")
	}

	s.wmu.Lock()
	pronto := make(chan int, 1)
	go func() {
		feitos, err := s.passadaDeReconstrucao(context.Background(), JanelaDeIndice{})
		if err != nil {
			t.Errorf("passadaDeReconstrucao com o wmu na mão: %v", err)
		}
		pronto <- feitos
	}()
	select {
	case feitos := <-pronto:
		if feitos == 0 {
			t.Error("a passada terminou sem reconstruir nada: ela não exercitou a publicação")
		}
	case <-time.After(30 * time.Second):
		s.wmu.Unlock()
		t.Fatal("a passada de reconstrução não terminou com o wmu segurado por outro: ela serializa a reconstrução " +
			"contra a ingestão, que é exatamente o defeito que este ciclo existe para eliminar")
	}
	s.wmu.Unlock()
}

// TestOSegmentoAposentadoNoMeioDaPassadaNaoDerrubaOLaco é a corrida que a
// aposentadoria concorrente arma. A fila é uma FOTOGRAFIA, e entre ela e a
// varredura a retenção pode ter removido um segmento — num histórico isso
// acontece a cada minuto.
//
// Tratar esse desfecho como falha faria o laço PARAR de vez (é o que a doutrina
// manda fazer com erro de infra) na primeira sobreposição, e o dataset inteiro
// voltaria a enumerar sem nada avisando além de uma linha de log. O teste entrega
// à passada uma fila que descreve um instante que já passou — exatamente o que
// ela recebe em produção — e exige que o segmento sobrevivente seja reconstruído.
func TestOSegmentoAposentadoNoMeioDaPassadaNaoDerrubaOLaco(t *testing.T) {
	s, dir := datasetComPendencias(t, 3, []string{"node"})
	fila := pendentes(t, s, JanelaDeIndice{})
	if len(fila) < 2 {
		t.Fatalf("montagem: %d pendente(s); o teste precisa de um segmento que some e outro que sobreviva", len(fila))
	}
	sumido, sobrevivente := fila[0], fila[1]
	aposenta(t, s, dir, sumido)

	feitos, err := s.reconstroiOsPendentes(t.Context(), fila)
	if err != nil {
		t.Fatalf("a passada parou por causa de um segmento aposentado entre a fila e a varredura: %v\n"+
			"num histórico a aposentadoria acontece a cada minuto, então isto derruba o laço no primeiro encontro", err)
	}
	if feitos != len(fila)-1 {
		t.Errorf("a passada reconstruiu %d de %d segmentos vivos", feitos, len(fila)-1)
	}
	if campos := camposDoSelado(t, dir, sobrevivente); len(campos) != 1 {
		t.Errorf("o segmento %d, que não sumiu, ficou com o índice cobrindo %v", sobrevivente, campos)
	}
}

// aposenta tira o segmento do dataset pelo MESMO passo que a aposentadoria de
// verdade usa: fechar o descritor cacheado e apagar o arquivo, sob o aposMu.
//
// Pelo passo real, e não por um os.Remove solto, porque os dois estados não são
// o mesmo: com o descritor ainda em cache, o pread sobre o arquivo já removido
// devolve os bytes certos — foi assim que a primeira versão deste teste
// reconstruiu com sucesso o índice de um segmento que já não existia. O erro que
// o laço precisa tolerar só aparece depois do FechaSegmento.
func aposenta(t *testing.T, s *Store, dir string, seg uint32) {
	t.Helper()
	if err := s.fechaODescritorEApaga(seg, caminhoCanonicoDoSegmento(dir+"/data", seg)); err != nil {
		t.Fatalf("aposentar o segmento %d: %v", seg, err)
	}
}

// TestOErroDoSegmentoSumidoEhOQueOLacoTolera trava a CLASSIFICAÇÃO contra o erro
// real, e não contra um valor sintético: é o erro que o ScanFrom devolve quando
// o .dat já não está lá. Uma lista de sentinelas escrita de cabeça deixaria de
// casar no dia em que o segment.Reader embrulhasse o erro de outro jeito, e o
// laço voltaria a parar de vez numa corrida benigna.
func TestOErroDoSegmentoSumidoEhOQueOLacoTolera(t *testing.T) {
	s, dir := datasetComPendencias(t, 2, []string{"node"})
	alvo := pendentes(t, s, JanelaDeIndice{})[0]
	aposenta(t, s, dir, alvo)
	_, err := s.reconstroiUmSegmento(t.Context(), alvo)
	if err == nil {
		t.Fatal("reconstruir um segmento que já não existe devolveu nil")
	}
	if !oSegmentoSaiuDoDataset(err) {
		t.Errorf("o erro %v não foi classificado como segmento que saiu do dataset: o laço o trataria como falha "+
			"de infra e pararia de vez", err)
	}
}

// TestAReconstrucaoInterrompidaNaoPublicaIndiceParcial é a garantia que torna a
// tolerância acima segura. Uma varredura que para no meio viu parte dos records,
// e publicar o que ela juntou seria um .sidx IMUTÁVEL sem as postings do resto —
// falso negativo petrificado, que é a única coisa que a revalidação do §44 não
// sabe consertar.
func TestAReconstrucaoInterrompidaNaoPublicaIndiceParcial(t *testing.T) {
	s, dir := datasetComPendencias(t, 2, []string{"node"})
	alvo := pendentes(t, s, JanelaDeIndice{})[0]
	ctx, cancela := context.WithCancel(context.Background())
	cancela()

	err := s.ConstroiIndiceDoSegmento(ctx, alvo, s.CamposIndexados())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ConstroiIndiceDoSegmento com ctx cancelado: err = %v, quero context.Canceled", err)
	}
	if _, err := os.Stat(segindex.SealedPath(dir+"/data", alvo)); !os.IsNotExist(err) {
		t.Errorf("Stat do .sidx depois de uma varredura cancelada = %v, quero IsNotExist: um índice de varredura "+
			"parcial é imutável e falso negativo para sempre", err)
	}
}

// TestOMedidorDaReconstrucaoSobeAntesDaGoroutine é a regra do §92: quem chamou
// tem direito de ver o estado que acabou de instalar. Subir o medidor DENTRO da
// goroutine deixa uma janela em que IniciaReconstrucao já voltou e
// ReconstrucaoAtiva ainda diz false.
func TestOMedidorDaReconstrucaoSobeAntesDaGoroutine(t *testing.T) {
	s, _ := datasetComPendencias(t, 2, []string{"node"})
	if s.ReconstrucaoAtiva() {
		t.Fatal("montagem: o medidor já dizia 1 antes de alguém iniciar a reconstrução")
	}
	s.IniciaReconstrucao(context.Background(), JanelaDeIndice{})
	if !s.ReconstrucaoAtiva() {
		t.Error("ReconstrucaoAtiva = false logo depois do Inicia: quem ligou o laço lê no painel o mesmo número de " +
			"quem não o tem")
	}
	s.ParaReconstrucao()
	if s.ReconstrucaoAtiva() {
		t.Error("ReconstrucaoAtiva = true depois do Para, que CANCELA e AGUARDA: série presa em 1 é alerta que nunca dispara")
	}
}

// TestOLacoDeReconstrucaoDrenaAFilaSozinho é o que separa "o mecanismo existe" de
// "o produto resolve", e é o que fecha o débito 22: sem esta goroutine a fila só
// era drenada por quem chamasse o ConstroiIndiceDoSegmento na mão.
//
// A primeira passada é IMEDIATA, então a fila esvazia sem esperar o cochilo — o
// teste falharia por prazo se ela fosse agendada para depois do intervalo.
func TestOLacoDeReconstrucaoDrenaAFilaSozinho(t *testing.T) {
	s, dir := datasetComPendencias(t, 3, []string{"node"})
	fila := pendentes(t, s, JanelaDeIndice{})
	if len(fila) == 0 {
		t.Fatal("montagem: a fila nasceu vazia")
	}

	s.IniciaReconstrucao(context.Background(), JanelaDeIndice{})
	defer s.ParaReconstrucao()
	esperaAte(t, func() bool { return len(pendentes(t, s, JanelaDeIndice{})) == 0 },
		"o laço de fundo não drenou a fila: o operador declara index e o campo continua enumerando indefinidamente")
	for _, seg := range fila {
		if campos := camposDoSelado(t, dir, seg); len(campos) != 1 {
			t.Errorf("o segmento %d saiu da fila com o índice cobrindo %v", seg, campos)
		}
	}
}

// TestCloseCancelaEEsperaAReconstrucao é a decisão de sempre, na quarta
// goroutine: o Close cancela e AGUARDA antes do quiesce. Sem o join, ou o Close
// espera a varredura inteira, ou — pior — vence a corrida e fecha o segR sob os
// pés de quem está lendo 512 MiB.
//
// O oráculo é o WaitGroup: depois do Close nenhuma goroutine de reconstrução
// pode continuar contada. Um Close que só cancelasse devolveria o controle com a
// varredura ainda em voo.
func TestCloseCancelaEEsperaAReconstrucao(t *testing.T) {
	s, _ := datasetComPendencias(t, 3, []string{"node"})
	s.IniciaReconstrucao(context.Background(), JanelaDeIndice{})
	if err := s.Close(); err != nil {
		t.Fatalf("Close com a reconstrução no ar: %v", err)
	}
	if s.ReconstrucaoAtiva() {
		t.Error("ReconstrucaoAtiva = true depois do Close: a goroutine ficou órfã sobre um dataset fechado")
	}
	terminou := make(chan struct{})
	go func() { s.recEmVoo.Wait(); close(terminou) }()
	select {
	case <-terminou:
	case <-time.After(30 * time.Second):
		t.Fatal("o Close devolveu o controle com a passada de reconstrução ainda em voo: ela lê pelo segR, que o " +
			"Close acabou de fechar")
	}
}

// TestSegundaPartidaDaReconstrucaoNaoSobeSegundaGoroutine é a guarda de dupla
// partida. O estrago dela não é contagem: a segunda partida SOBRESCREVE o
// recCancela, de modo que o ParaReconstrucao do Close cancelaria só a última
// goroutine e ficaria esperando para sempre pela primeira — o processo não
// encerra.
func TestSegundaPartidaDaReconstrucaoNaoSobeSegundaGoroutine(t *testing.T) {
	s, _ := datasetComPendencias(t, 2, []string{"node"})
	s.IniciaReconstrucao(context.Background(), JanelaDeIndice{})
	s.IniciaReconstrucao(context.Background(), JanelaDeIndice{})

	parou := make(chan struct{})
	go func() { s.ParaReconstrucao(); close(parou) }()
	select {
	case <-parou:
	case <-time.After(30 * time.Second):
		t.Fatal("o ParaReconstrucao não voltou: a segunda partida subiu outra goroutine e sobrescreveu o cancelamento " +
			"da primeira, que ninguém mais alcança")
	}
	if s.ReconstrucaoAtiva() {
		t.Error("sobrou goroutine de reconstrução de pé depois do Para")
	}
}

// TestReconstrucaoNaoSobeSemCampoIndexado: sem campo declarado não há fila, e uma
// goroutine que acordasse a cada cinco segundos para responder sempre a mesma
// coisa é readdir por nada. É também o que permite ao servidor ligá-la sobre a
// lista inteira de datasets sem perguntar quais têm índice.
func TestReconstrucaoNaoSobeSemCampoIndexado(t *testing.T) {
	s := abreStoreIndexadoEm(t, t.TempDir(), nil, 64<<10)
	s.IniciaReconstrucao(context.Background(), JanelaDeIndice{})
	defer s.ParaReconstrucao()
	if s.ReconstrucaoAtiva() {
		t.Error("a reconstrução subiu num dataset sem campo indexado")
	}
}

// TestReconstrucaoRecusaJanelaFinitaNoMaterializado fecha a porta que a
// assinatura deixa aberta: ela obriga a passar ALGUMA janela, e o compilador não
// sabe qual é a certa. Janela finita sobre um materializado é erro de quem monta
// os limites — lá a linha vive no segmento corrente independente da idade dela —,
// e deixar o laço subir faria cada passada falhar e o ERROR de parada aparecer
// como se o mecanismo tivesse quebrado.
func TestReconstrucaoRecusaJanelaFinitaNoMaterializado(t *testing.T) {
	s, _ := datasetComPendencias(t, 1, []string{"node"})
	s.IniciaReconstrucao(context.Background(), JanelaDeIndice{Duracao: time.Hour})
	defer s.ParaReconstrucao()
	if s.ReconstrucaoAtiva() {
		t.Error("a reconstrução subiu com janela finita num dataset materializado, onde a janela é infinita por construção")
	}
}

// TestOMapeamentoAbertoAntesDaReescritaNaoEhPublicado é a TERCEIRA aresta do
// cache de selados, e ela deixou de ser teórica no instante em que este laço
// subiu: o garanteSelado abre o arquivo fora de lock nenhum (o crc32c de um
// índice grande não pode rodar sob o selMu, que é o lock da rotação), então entre
// abrir e publicar cabe uma reescrita inteira da reconstrução.
//
// O mapeamento velho sobrevive ao rename — o mmap segura o INODE, não o nome —,
// então publicá-lo depois da evição deixa o segmento respondendo pelo índice
// anterior, sem a seção do campo novo, ou seja ENUMERANDO, até a próxima abertura
// do dataset. O teste se mete exatamente entre as duas chamadas, que é o único
// lugar de onde a janela é visível.
func TestOMapeamentoAbertoAntesDaReescritaNaoEhPublicado(t *testing.T) {
	dir := t.TempDir()
	s := abreStoreIndexadoEm(t, dir, []string{"node"}, 64<<10)
	alvo := s.curSegID.Load()
	poeLinhasAteRotacionar(t, s, alvo)
	if err := s.Close(); err != nil {
		t.Fatalf("Close da montagem: %v", err)
	}
	s2 := abreStoreIndexadoEm(t, dir, []string{"node", "type"}, 64<<10)

	// O que uma consulta concorrente teria em mãos: a geração lida e o arquivo
	// VELHO já aberto, ainda não publicado.
	s2.selMu.RLock()
	geracao := s2.selGeracao
	s2.selMu.RUnlock()
	velho, err := segindex.OpenSealed(segindex.SealedPath(dir+"/data", alvo))
	if err != nil {
		t.Fatalf("OpenSealed do índice antigo: %v", err)
	}
	if campos := velho.Fields(); len(campos) != 1 {
		t.Fatalf("montagem: o mapeamento antigo cobre %v; o teste precisa dele SEM a seção do campo novo", campos)
	}

	if feitos := passadaDeReconstrucao(t, s2); feitos != 1 {
		t.Fatalf("a passada reconstruiu %d segmentos, quero 1", feitos)
	}
	s2.publicaSelado(alvo, velho, geracao)

	s2.selMu.RLock()
	publicado := s2.selados[alvo]
	s2.selMu.RUnlock()
	if publicado == velho {
		t.Fatal("o mapeamento aberto ANTES da reescrita foi publicado DEPOIS dela: o segmento responde pelo índice " +
			"velho — sem a seção do campo novo, portanto enumerando — até a próxima abertura do dataset, e ainda " +
			"prende o inode substituído")
	}
	if campos := camposDoSeladoEmCache(t, s2, alvo); len(campos) != 2 {
		t.Errorf("o cache serve um índice que cobre %v depois da reconstrução, quero os dois campos", campos)
	}
}

// camposDoSeladoEmCache resolve o segmento pelo caminho da CONSULTA (o
// garanteSelado) e devolve o que o mapeamento em cache cobre. É o oráculo do que
// a query enxerga, que é diferente do que está em disco quando o cache guarda um
// mapeamento velho.
func camposDoSeladoEmCache(t *testing.T, s *Store, seg uint32) []types.FieldID {
	t.Helper()
	s.garanteSelado(seg)
	s.selMu.RLock()
	defer s.selMu.RUnlock()
	sel := s.selados[seg]
	if sel == nil {
		t.Fatalf("o segmento %d não tem índice em cache depois do garanteSelado", seg)
	}
	return sel.Fields()
}

// TestOSegmentoQueVirouCorrenteEntreAFotografiaEAVarreduraNaoMataOLaco fecha a
// corrida simétrica à do segmento aposentado: o .dat do segmento novo nasce
// dentro do Append, ANTES de o aposAppend trocar o curSegID, então a fotografia
// da fila pode listá-lo enquanto o Load ainda devolve o anterior — e quando o
// laço o alcança, ele JÁ é o corrente. A guarda de imutabilidade recusa (certo),
// mas sem a sentinela a recusa subia como falha da passada, o laço logava
// reconstrucao.parou e NÃO voltava até a reabertura do dataset: uma janela de
// milissegundos que se resolve sozinha matava a cura da §2.4 inteira.
func TestOSegmentoQueVirouCorrenteEntreAFotografiaEAVarreduraNaoMataOLaco(t *testing.T) {
	s, _ := abreStoreIndexadoComSegmentoPequeno(t, []string{"node"}, 1<<20)
	poeLinhas(t, s, 5)
	corrente := s.curSegID.Load()

	// A fila fotografada com o corrente dentro — o estado que a corrida produz.
	feitos, err := s.reconstroiOsPendentes(context.Background(), []uint32{corrente})
	if err != nil {
		t.Fatalf("a passada morreu com o corrente na fila: %v — o laço pararia de vez por uma corrida benigna", err)
	}
	if feitos != 0 {
		t.Errorf("feitos = %d, quero 0: o corrente é pulado, não reconstruído", feitos)
	}

	// A recusa DIRETA continua classificada para o chamador externo: pular no
	// laço não pode afrouxar o contrato de quem pede a reconstrução na mão.
	err = s.ConstroiIndiceDoSegmento(context.Background(), corrente, s.camposIndexados)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("ConstroiIndiceDoSegmento(corrente) = %v, quero ErrInvalidArgument", err)
	}
}

// TestAReconstrucaoRecusaSegmentoSeladoComCorrupcaoNoMeio guarda o desfecho
// mais caro do produto. O ScanFrom para no primeiro record inválido SEM erro
// (§52); num segmento selado isso é bit rot no meio do arquivo, não tail de
// crash — e a recovery não o pegaria, porque só varre do corrente em diante.
// Sem a guarda do fim válido, a reconstrução publicava um .sidx com todas as
// seções declaradas e SEM as postings das rows posteriores à corrupção: a fila
// o contava como pronto (ela compara seções, nunca conteúdo), o crc do arquivo
// — escrito corretamente a partir de uma varredura incompleta — passava, e as
// rows omitidas nunca viravam candidatas enquanto o locations continuava a
// serví-las. Falso negativo permanente. A compaction guarda a operação idêntica
// com a mesma comparação (fimValido != tamanho).
func TestAReconstrucaoRecusaSegmentoSeladoComCorrupcaoNoMeio(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir, SegmentSize: 8 << 10, CamposIndexadosPorNome: []string{"node"}})
	alvo := s.curSegID.Load()
	poeLinhasAteRotacionar(t, s, alvo)
	// O checkpoint avança o ponto S do manifest para DEPOIS do segmento alvo:
	// é o estado normal de qualquer segmento selado em regime, e é o que deixa
	// a corrupção dele INVISÍVEL para a recovery — o replay da abertura varre
	// [CurrentSegment..max], e dos anteriores confere só o tamanho.
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// O segmento selado perde o índice (fica pendente) e ganha um byte podre no
	// MEIO do dado — depois do header do segmento, longe do fim.
	caminhoSidx := segindex.SealedPath(filepath.Join(dir, "data"), alvo)
	if err := os.Remove(caminhoSidx); err != nil {
		t.Fatal(err)
	}
	caminhoDat := filepath.Join(dir, "data", fmt.Sprintf("segment-%08d.dat", alvo))
	info, err := os.Stat(caminhoDat)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(caminhoDat, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	meio := int64(segment.HeaderSize) + (info.Size()-int64(segment.HeaderSize))/2
	if _, err := f.WriteAt([]byte{0xFF, 0xFF, 0xFF, 0xFF}, meio); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := openStore(t, Options{Dir: dir, SegmentSize: 8 << 10, CamposIndexadosPorNome: []string{"node"}})
	err = s2.ConstroiIndiceDoSegmento(context.Background(), alvo, s2.camposIndexados)
	if !errors.Is(err, ErrDatasetCorrupt) {
		t.Fatalf("ConstroiIndiceDoSegmento sobre segmento podre = %v, quero ErrDatasetCorrupt: "+
			"a varredura parou no meio e o índice sairia sem as rows posteriores", err)
	}
	if _, statErr := os.Stat(caminhoSidx); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("um .sidx foi publicado (%v) a partir de uma varredura incompleta: "+
			"ele declararia completo um índice com falso negativo permanente", statErr)
	}
}

// TestOSegmentoAusenteNaGuardaDeTamanhoEhCorridaBenigna guarda a classificação
// que a re-revisão dos consertos exigiu: a guarda de varredura incompleta mede
// o tamanho por stat direto, e um segmento que a retenção aposentou tem de
// sair como a corrida benigna que o laço já tolera (os.ErrNotExist) — a
// primeira forma da guarda media DEPOIS, pela listagem, e devolvia a ausência
// como ErrDatasetCorrupt: o laço morria de vez por uma sobreposição com a
// aposentadoria que ele próprio declara tolerar.
func TestOSegmentoAusenteNaGuardaDeTamanhoEhCorridaBenigna(t *testing.T) {
	s, _ := abreStoreIndexadoComSegmentoPequeno(t, []string{"node"}, 1<<20)
	poeLinhas(t, s, 3)

	_, err := s.constroiIndiceDoSegmento(context.Background(), 999, s.camposIndexados)
	if !oSegmentoSaiuDoDataset(err) {
		t.Fatalf("segmento ausente classificado como %v: tem de ser a corrida benigna (os.ErrNotExist), "+
			"senão a aposentadoria concorrente mata o laço", err)
	}
	feitos, err := s.reconstroiOsPendentes(context.Background(), []uint32{999})
	if err != nil {
		t.Fatalf("a passada morreu com um segmento ausente na fila: %v", err)
	}
	if feitos != 0 {
		t.Errorf("feitos = %d, quero 0", feitos)
	}
}
