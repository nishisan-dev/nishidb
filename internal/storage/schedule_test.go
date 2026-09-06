package storage

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/rowindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// A CONCORDÂNCIA CRUZADA, que é o risco principal desta estrutura.
//
// prazoVencido continua sendo a fonte de verdade da visibilidade — ele lê
// expiry.idx e prazos.idx, e não a agenda. A agenda decide apenas QUANDO o
// coletor olha para a linha. Se as duas divergirem, um bug de agenda vira um
// bug de query: a linha some da consulta e nunca é marcada, ou é marcada
// enquanto a consulta ainda a mostra.
//
// Este teste sujeita as duas à mesma carga aleatória de escritas, prazos
// próprios, imortalidade e lápides, e exige que concordem linha a linha em
// vários instantes diferentes.
func TestAgendaConcordaComPrazoVencidoSobCargaAleatoria(t *testing.T) {
	s, rel := storeComExpiracao(t, t.TempDir(), time.Hour)
	r := rand.New(rand.NewPCG(17, 23))

	const linhas = 400
	ids := make([]types.RowID, 0, linhas)
	for i := 0; i < linhas; i++ {
		key := fmt.Sprintf("k-%03d", i)
		campos := map[string]types.Value{
			"id":         {Type: types.TypeString, Bytes: []byte(key)},
			"_writtenAt": {Type: types.TypeTimestamp, I64: rel.t.UnixMilli()},
		}
		switch r.UintN(4) {
		case 0: // prazo próprio curto
			campos["_expiresIn"] = types.Value{Type: types.TypeInt64, I64: int64(5 * time.Minute / time.Millisecond)}
		case 1: // prazo próprio longo
			campos["_expiresIn"] = types.Value{Type: types.TypeInt64, I64: int64(10 * time.Hour / time.Millisecond)}
		case 2: // imortal
			campos["_expiresIn"] = types.Value{Type: types.TypeInt64, I64: 0}
		}
		// O caso 3 fica sem _expiresIn: cai na duração de fundo.
		id, _ := mustPut(t, s, key, 1, payloadOrdenado(t, s, campos), nil)
		ids = append(ids, id)
		rel.avanca(time.Duration(r.UintN(60)) * time.Second)
	}

	confere := func(momento string) {
		t.Helper()
		agora := uint64(s.agora().UnixMilli())
		for _, id := range ids {
			venc := s.agenda.Get(id)
			agendadoVencido := venc != rowindex.Nunca && venc <= agora
			// Carencia é a linha sem instante conhecido: ela pede atenção do
			// coletor sem ter vencido, e prazoVencido diz false para ela com
			// razão. É a única divergência legítima.
			if venc == rowindex.Carencia {
				continue
			}
			if agendadoVencido != s.prazoVencido(id, s.agora()) {
				ms, conhecido := s.escritas.Get(id)
				t.Fatalf("%s: row %d diverge — agenda diz vencida=%v (venc=%d, agora=%d), prazoVencido diz %v (escrita=%d conhecido=%v, duração=%v)",
					momento, id, agendadoVencido, venc, agora, s.prazoVencido(id, s.agora()), ms, conhecido, s.duracaoEfetiva(id))
			}
		}
	}

	confere("logo após a carga")
	for i, salto := range []time.Duration{time.Minute, 30 * time.Minute, 2 * time.Hour, 24 * time.Hour} {
		rel.avanca(salto)
		confere(fmt.Sprintf("salto %d (+%v)", i, salto))
	}
	if err := s.agenda.Confere(); err != nil {
		t.Fatal(err)
	}
}

// A lápide tira a linha da agenda de vez.
//
// Ela preserva o instante de escrita de propósito, então a linha tombstonada
// continua "vencida" para sempre. Sem sair da agenda ela seria reentregue em
// toda descida, e o coletor chamaria Expira — que toma o lock do writer só
// para ver a flag e devolver false. É o mesmo desperdício que o pré-filtro de
// tombstone da varredura existia para evitar, e aqui ele some por construção.
func TestLinhaTombstonadaSaiDaAgenda(t *testing.T) {
	s, rel := storeComExpiracao(t, t.TempDir(), time.Hour)
	id, _ := mustPut(t, s, "A", 1, payloadComEscrita(t, s, "A", "Start", rel.t), nil)
	if s.agenda.Get(id) == rowindex.Nunca {
		t.Fatal("a linha nasceu fora da agenda")
	}
	rel.avanca(2 * time.Hour)
	expirou, err := s.Expira(id)
	if err != nil || !expirou {
		t.Fatalf("Expira devolveu %v, %v", expirou, err)
	}
	if v := s.agenda.Get(id); v != rowindex.Nunca {
		t.Fatalf("a linha tombstonada continua agendada para %d", v)
	}
	// E a descida não a entrega mais, por mais longe que o relógio vá.
	rel.avanca(1000 * time.Hour)
	s.agenda.Vencidos(uint64(s.agora().UnixMilli()), func(row types.RowID) bool {
		if row == id {
			t.Fatal("a descida entregou uma linha tombstonada")
		}
		return true
	})
}

// A renovação empurra o vencimento agendado junto com o prazo. É o caso de
// todo evento em regime, e é o que impede a linha renovada de ser reexaminada.
func TestEscritaNovaEmpurraOVencimentoAgendado(t *testing.T) {
	s, rel := storeComExpiracao(t, t.TempDir(), time.Hour)
	id, _ := mustPut(t, s, "A", 1, payloadComEscrita(t, s, "A", "Start", rel.t), nil)
	primeiro := s.agenda.Get(id)

	rel.avanca(30 * time.Minute)
	mustPut(t, s, "A", 2, payloadComEscrita(t, s, "A", "Run", rel.t), nil)
	segundo := s.agenda.Get(id)

	if segundo <= primeiro {
		t.Fatalf("o vencimento não andou: %d → %d", primeiro, segundo)
	}
	if querido := uint64(rel.t.Add(time.Hour).UnixMilli()); segundo != querido {
		t.Fatalf("vencimento agendado é %d, quer %d (escrita + duração de fundo)", segundo, querido)
	}
}

// A linha sem instante de escrita é agendada para a CARÊNCIA, e não para ∞.
//
// Ela não venceu — não há de onde tirar um prazo —, mas precisa da atenção do
// coletor para receber o prazo cheio. Agendá-la para ∞ a deixaria imortal por
// acidente de calendário, que é exatamente o que a carência existe para
// impedir; agendá-la para um instante derivado do relógio daria árvores
// diferentes para o mesmo dataset conforme QUANDO ele foi aberto.
func TestLinhaSemInstanteEhAgendadaParaACarencia(t *testing.T) {
	s := openStore(t, Options{
		Dir: t.TempDir(), TTLAtivo: true, DuracaoDeFundo: time.Hour,
		CampoDeEscrita: "_writtenAt", CampoDePrazo: "_expiresIn",
	})
	rel := &relogioDeTeste{t: time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)}
	s.UsaRelogio(rel.agora)

	id, _ := mustPut(t, s, "legada", 1, payloadSemEscrita(t, s, "legada"), nil)
	if v := s.agenda.Get(id); v != rowindex.Carencia {
		t.Fatalf("a linha sem instante foi agendada para %d, quer %d (carência)", v, rowindex.Carencia)
	}
	// E depois da carência ela ganha vencimento de verdade.
	if _, err := s.daCarencia(id); err != nil {
		t.Fatal(err)
	}
	v := s.agenda.Get(id)
	if v == rowindex.Carencia || v == rowindex.Nunca {
		t.Fatalf("depois da carência a linha continua em %d", v)
	}
	if querido := uint64(rel.t.Add(time.Hour).UnixMilli()); v != querido {
		t.Fatalf("vencimento após a carência é %d, quer %d", v, querido)
	}
}

// A linha imortal fica fora da agenda: ela não tem vencimento, e o coletor
// não pode ser acordado por ela.
func TestLinhaImortalNaoEntraNaAgenda(t *testing.T) {
	s, rel := storeComExpiracao(t, t.TempDir(), 0) // sem duração de fundo
	id, _ := mustPut(t, s, "A", 1, payloadComEscrita(t, s, "A", "Start", rel.t), nil)
	if v := s.agenda.Get(id); v != rowindex.Nunca {
		t.Fatalf("linha sem prazo nenhum foi agendada para %d", v)
	}
}

// A ORDEM DE DURABILIDADE com a expiração ligada, que é a sequência que o
// TestCheckpointOrdemDeDurabilidade não cobre — o dataset dele não tem TTL.
//
// A agenda entra depois dos dois índices de que ela deriva e antes do
// manifest. Antes do manifest porque é ele que declara o ponto S recuperável:
// um msync depois dele declararia durável uma árvore ainda em página suja, e
// a recovery seguinte confiaria na declaração.
//
// MUTAÇÃO QUE ESTE TESTE PEGA: remover o msync da agenda do checkpoint.
func TestCheckpointDurabilizaAAgendaAntesDoManifest(t *testing.T) {
	s, rel := storeComExpiracao(t, t.TempDir(), time.Hour)
	var trace []string
	s.SetCheckpointTraceForTest(func(step string) { trace = append(trace, step) })
	mustPut(t, s, "A", 1, payloadComEscrita(t, s, "A", "Start", rel.t), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	pos := func(passo string) int {
		for i, p := range trace {
			if p == passo {
				return i
			}
		}
		return -1
	}
	agenda, manifest := pos("msyncVencimentos"), pos("manifest")
	if agenda < 0 {
		t.Fatalf("o checkpoint não durabilizou a agenda: %v", trace)
	}
	if manifest < 0 || agenda > manifest {
		t.Fatalf("a agenda foi durabilizada depois do manifest: %v", trace)
	}
	for _, antes := range []string{"msyncPrazos", "msyncEscritas"} {
		if p := pos(antes); p < 0 || p > agenda {
			t.Fatalf("%s não veio antes da agenda que deriva dele: %v", antes, trace)
		}
	}
}

// opcoesComExpiracao repete a configuração de storeComExpiracao para os testes
// que precisam reabrir o MESMO diretório com parâmetros variados.
func opcoesComExpiracao(dir string, d time.Duration) Options {
	return Options{
		Dir: dir, SegmentSize: 1 << 20, TTLAtivo: true, DuracaoDeFundo: d,
		CampoDeEscrita: "_writtenAt", CampoDePrazo: "_expiresIn",
	}
}

// simulaDatasetAnteriorAAgenda deixa o dataset exatamente como um binário sem
// agenda o teria deixado: os arquivos da árvore não existem, e o manifest não
// tem a seção 0x8003.
//
// As duas coisas juntas, e não só a primeira: apagar os arquivos mantendo a
// seção descreveria um dataset que nenhum binário produz, e o teste passaria a
// exercitar um estado impossível em vez da migração real.
func simulaDatasetAnteriorAAgenda(t *testing.T, dir string) {
	t.Helper()
	arquivos, err := filepath.Glob(filepath.Join(dir, "indexes", "vencimentos*.idx"))
	if err != nil || len(arquivos) == 0 {
		t.Fatalf("nenhum arquivo de agenda encontrado em %s (glob: %v)", dir, err)
	}
	for _, p := range arquivos {
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
	}
	manifest := filepath.Join(dir, "checkpoint", "manifest.bin")
	m, err := checkpoint.Load(manifest)
	if err != nil {
		t.Fatal(err)
	}
	m.Agenda = nil
	if err := checkpoint.Write(manifest, m); err != nil {
		t.Fatal(err)
	}
}

// A MIGRAÇÃO, e a razão de ela não ser o débito 22.
//
// Um dataset materializado antes de a agenda existir tem expiry.idx e
// prazos.idx populados e nenhum arquivo de vencimentos. Ele NÃO precisa ser
// recriado nem reingerido: a agenda é derivada desses dois índices mais as
// locations, todos densos e em mmap, e a construção os lê sequencialmente sem
// tocar em um único record de segmento.
//
// É essa a fronteira contra o débito 22, que é sobre o índice secundário: ele
// indexa valores EXTRAÍDOS do payload, e reconstruí-lo exige ler todo record
// de todo segmento — a janela de 18,8 h que o piloto teria de pagar de uma
// vez, e que o índice por segmento parte em uma varredura por segmento. Aqui
// não há record nenhum a ler.
func TestAgendaAusenteEhConstruidaNaAberturaSemLerRecord(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, opcoesComExpiracao(dir, time.Hour))
	rel := &relogioDeTeste{t: time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)}
	s.UsaRelogio(rel.agora)
	var ids []types.RowID
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("k-%02d", i)
		id, _ := mustPut(t, s, key, 1, payloadComEscrita(t, s, key, "Start", rel.t), nil)
		ids = append(ids, id)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Apaga os arquivos da agenda: é o estado de um dataset anterior a ela.
	simulaDatasetAnteriorAAgenda(t, dir)

	s2 := openStore(t, opcoesComExpiracao(dir, time.Hour))
	s2.UsaRelogio(rel.agora)
	for _, id := range ids {
		if v := s2.agenda.Get(id); v != s2.vencimentoDe(id) {
			t.Fatalf("row %d ficou em %d depois da construção, quer %d", id, v, s2.vencimentoDe(id))
		}
	}
	if err := s2.AuditaAgenda(); err != nil {
		t.Fatal(err)
	}
}

// A DURAÇÃO DE FUNDO mudou: a agenda em disco derivou de um número que este
// binário já não honra, e adotá-la deixaria as linhas com prazo desconhecido
// vencendo na data errada — ou nunca. Tem de ser reconstruída.
//
// MUTAÇÃO QUE ESTE TESTE PEGA: adotar a agenda sem comparar os parâmetros do
// manifest.
func TestAgendaEhReconstruidaQuandoADuracaoDeFundoMuda(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)
	s := openStore(t, opcoesComExpiracao(dir, time.Hour))
	rel := &relogioDeTeste{t: base}
	s.UsaRelogio(rel.agora)
	// Sem _expiresIn: a linha herda a duração de FUNDO, que é o que muda.
	id, _ := mustPut(t, s, "A", 1, payloadComEscrita(t, s, "A", "Start", rel.t), nil)
	antes := s.agenda.Get(id)
	if querido := uint64(base.Add(time.Hour).UnixMilli()); antes != querido {
		t.Fatalf("vencimento inicial %d, quer %d", antes, querido)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reabre com OUTRA duração de fundo.
	s2 := openStore(t, opcoesComExpiracao(dir, 10*time.Hour))
	s2.UsaRelogio(rel.agora)
	depois := s2.agenda.Get(id)
	if querido := uint64(base.Add(10 * time.Hour).UnixMilli()); depois != querido {
		t.Fatalf("depois de mudar a duração de fundo o vencimento é %d, quer %d (a agenda não foi reconstruída)",
			depois, querido)
	}
	if err := s2.AuditaAgenda(); err != nil {
		t.Fatal(err)
	}
}

// A agenda íntegra e com os mesmos parâmetros é ADOTADA: reconstruir a cada
// abertura desperdiçaria uma varredura completa do dataset justamente na
// operação que a agenda existe para tornar desnecessária.
func TestAgendaCoerenteEhAdotadaSemReconstrucao(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, opcoesComExpiracao(dir, time.Hour))
	rel := &relogioDeTeste{t: time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)}
	s.UsaRelogio(rel.agora)
	id, _ := mustPut(t, s, "A", 1, payloadComEscrita(t, s, "A", "Start", rel.t), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := openStore(t, opcoesComExpiracao(dir, time.Hour))
	s2.UsaRelogio(rel.agora)
	if s2.agendaReconstruida {
		t.Fatal("a agenda foi reconstruída sem nenhum parâmetro ter mudado")
	}
	if v := s2.agenda.Get(id); v != s2.vencimentoDe(id) {
		t.Fatalf("a agenda adotada diverge: %d != %d", v, s2.vencimentoDe(id))
	}
}

// A auditoria reprova a agenda que diverge do que os índices dizem — é ela
// que dá aos testes de crash uma afirmação a fazer sobre o dataset inteiro.
func TestAuditoriaReprovaAAgendaDivergente(t *testing.T) {
	s, rel := storeComExpiracao(t, t.TempDir(), time.Hour)
	id, _ := mustPut(t, s, "A", 1, payloadComEscrita(t, s, "A", "Start", rel.t), nil)
	if err := s.AuditaAgenda(); err != nil {
		t.Fatalf("agenda íntegra reprovada: %v", err)
	}
	// Empurra a folha para o infinito: a linha continua vencendo pelos
	// índices, mas o coletor nunca mais a alcança.
	if err := s.agenda.Atualiza(id, 0); err != nil {
		t.Fatal(err)
	}
	if err := s.AuditaAgenda(); err == nil {
		t.Fatal("a auditoria aceitou uma linha que saiu da agenda sem ter saído do prazo")
	}
}

// A seção do manifest declara os PARÂMETROS da agenda, não a existência dela.
//
// Um dataset cujos arquivos de vencimento sumiram — apagados por engano,
// perdidos numa cópia parcial, truncados por um filesystem cheio — abriria com
// a seção batendo e uma árvore vazia. Árvore vazia significa "nada vence
// nunca": é o mesmo modo de falha silencioso que a seção crítica existe para
// impedir, entrando pela porta que ela não cobre. Daí a checagem de cobertura,
// que custa um Size() por nível.
//
// MUTAÇÃO QUE ESTE TESTE PEGA: decidir a adoção só pelos parâmetros do
// manifest, sem conferir se os arquivos alcançam as linhas.
func TestAgendaQueNaoCobreODatasetEhReconstruida(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, opcoesComExpiracao(dir, time.Hour))
	rel := &relogioDeTeste{t: time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)}
	s.UsaRelogio(rel.agora)
	id, _ := mustPut(t, s, "A", 1, payloadComEscrita(t, s, "A", "Start", rel.t), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Só os arquivos: o manifest continua declarando a agenda, com os mesmos
	// parâmetros desta abertura.
	arquivos, err := filepath.Glob(filepath.Join(dir, "indexes", "vencimentos*.idx"))
	if err != nil || len(arquivos) == 0 {
		t.Fatalf("nenhum arquivo de agenda encontrado (glob: %v)", err)
	}
	for _, p := range arquivos {
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
	}

	s2 := openStore(t, opcoesComExpiracao(dir, time.Hour))
	s2.UsaRelogio(rel.agora)
	if !s2.agendaReconstruida {
		t.Fatal("a agenda vazia foi adotada: nenhuma linha voltaria a vencer")
	}
	if v := s2.agenda.Get(id); v != s2.vencimentoDe(id) {
		t.Fatalf("row %d ficou em %d, quer %d", id, v, s2.vencimentoDe(id))
	}
	if err := s2.AuditaAgenda(); err != nil {
		t.Fatal(err)
	}
}

// O CRITÉRIO DE ACEITE DA FASE: o esforço da passada acompanha o que venceu,
// não o tamanho do dataset.
//
// Era esta a conta que custava 19% do CPU da ingestão no piloto. A varredura
// lia toda linha para achar as poucas vencidas — com a regra condicional de
// três minutos, uma volta completa nos 4,57 milhões a cada ~2,5 minutos, e
// expiry_scanned_total marcando 24,1 milhões contra 0 no histórico.
//
// A afirmação é por ESCALA, e não por um teto absoluto, porque um teto não
// distingue as duas implementações: uma varredura que filtrasse por
// prazoVencido antes de entregar produziria exatamente as mesmas linhas
// expiradas e o mesmo número de marcas. O que a separa é como o ESFORÇO
// responde ao tamanho do dataset — e é isso que se mede, rodando a mesma carga
// de vencidas sobre dois datasets de tamanhos bem diferentes.
//
// MUTAÇÃO QUE ESTE TESTE PEGA: voltar a percorrer as linhas em vez de descer a
// agenda, com ou sem pré-filtro.
func TestOEsforcoDaPassadaNaoAcompanhaOTamanhoDoDataset(t *testing.T) {
	esforco := func(linhas int) int {
		t.Helper()
		s, rel := storeComExpiracao(t, t.TempDir(), 100*time.Hour)
		// Todas com prazo longo; as três primeiras com prazo curto, para que
		// o TRABALHO seja idêntico nos dois datasets e só o tamanho mude.
		for i := 0; i < linhas; i++ {
			key := fmt.Sprintf("k-%05d", i)
			campos := map[string]types.Value{
				"id":         {Type: types.TypeString, Bytes: []byte(key)},
				"_writtenAt": {Type: types.TypeTimestamp, I64: rel.t.UnixMilli()},
			}
			if i < 3 {
				campos["_expiresIn"] = types.Value{Type: types.TypeInt64, I64: int64(time.Minute / time.Millisecond)}
			}
			mustPut(t, s, key, 1, payloadOrdenado(t, s, campos), nil)
		}
		rel.avanca(5 * time.Minute) // vence as três curtas, nenhuma das longas

		r, err := s.ExpiraUmaPassada(context.Background(), limitesDeTeste())
		if err != nil {
			t.Fatal(err)
		}
		if r.Expiradas != 3 {
			t.Fatalf("dataset de %d linhas: expiradas=%d, quer 3 (%+v)", linhas, r.Expiradas, r)
		}
		if r.Examinadas == 0 {
			t.Fatalf("dataset de %d linhas: a passada expirou 3 linhas sem tocar a agenda — não está descendo a árvore", linhas)
		}
		return r.Examinadas
	}

	pequeno, grande := esforco(200), esforco(20_000)
	// Cem vezes mais linhas, o mesmo trabalho. A descida toca alguns nós a
	// mais porque a árvore fica mais funda, mas o esforço não pode ACOMPANHAR
	// o tamanho: o dobro é folga larga para a profundidade, e uma varredura
	// mediria cem vezes.
	if grande > 2*pequeno {
		t.Fatalf("esforço saltou de %d para %d ao multiplicar o dataset por 100: a passada está varrendo",
			pequeno, grande)
	}
}

// O coletor dorme até o vencimento, e não por relógio fixo.
//
// É a outra metade do ganho: sem a agenda ele acordava de minuto em minuto
// para varrer o dataset e quase sempre não achar nada. O teto de um minuto
// continua valendo por cima, porque uma linha pode entrar na agenda com
// vencimento anterior àquele em que o coletor foi dormir.
func TestOColetorDormeAteOProximoVencimento(t *testing.T) {
	s, rel := storeComExpiracao(t, t.TempDir(), time.Hour)
	mustPut(t, s, "A", 1, payloadComEscrita(t, s, "A", "Start", rel.t), nil)

	// Falta muito mais que o teto: a espera é limitada por ele.
	if espera := s.esperaAteOProximoVencimento(ResultadoDaExpiracao{}); espera != tetoDeEspera {
		t.Fatalf("com o vencimento a uma hora, a espera é %v; quer o teto de %v", espera, tetoDeEspera)
	}
	// Agora falta menos que o teto: a espera encolhe para o vencimento.
	rel.avanca(time.Hour - 10*time.Second)
	espera := s.esperaAteOProximoVencimento(ResultadoDaExpiracao{})
	if espera != 10*time.Second {
		t.Fatalf("faltando 10s para o vencimento, a espera é %v", espera)
	}
	// Passada que bateu o teto volta já: há trabalho represado.
	if espera := s.esperaAteOProximoVencimento(ResultadoDaExpiracao{BateuTeto: true}); espera != 0 {
		t.Fatalf("passada represada esperou %v", espera)
	}
	// Sem nada agendado, o teto: dormir para sempre deixaria a primeira linha
	// escrita depois disso sem coletor nenhum.
	s2, _ := storeComExpiracao(t, t.TempDir(), time.Hour)
	if espera := s2.esperaAteOProximoVencimento(ResultadoDaExpiracao{}); espera != tetoDeEspera {
		t.Fatalf("agenda vazia esperou %v; quer o teto de %v", espera, tetoDeEspera)
	}
}

// A entrada FROUXA deixada por uma interrupção é corrigida na primeira visita.
//
// Sem isso, uma folha que promete um vencimento que já não existe seria
// reentregue em toda passada, para sempre: o coletor gastaria o teto de tempo
// inteiro tomando o lock do writer para descobrir que não há nada a fazer. É a
// diferença entre uma estrutura auto-corretiva e um laço.
func TestEntradaFrouxaEhCorrigidaNaPrimeiraVisita(t *testing.T) {
	s, rel := storeComExpiracao(t, t.TempDir(), time.Hour)
	id, _ := mustPut(t, s, "A", 1, payloadComEscrita(t, s, "A", "Start", rel.t), nil)

	// Simula o que uma interrupção deixa: a folha promete um vencimento muito
	// anterior ao real. A linha NÃO venceu — prazoVencido diz que não.
	if err := s.agenda.Atualiza(id, rowindex.PrimeiroVencimentoReal); err != nil {
		t.Fatal(err)
	}
	primeira, err := s.ExpiraUmaPassada(context.Background(), limitesDeTeste())
	if err != nil {
		t.Fatal(err)
	}
	if primeira.Renovadas != 1 || primeira.Expiradas != 0 || primeira.Examinadas == 0 {
		t.Fatalf("a primeira passada devia descer até a entrada frouxa e não marcar nada: %+v", primeira)
	}
	segunda, err := s.ExpiraUmaPassada(context.Background(), limitesDeTeste())
	if err != nil {
		t.Fatal(err)
	}
	if segunda.Examinadas != 0 || segunda.Renovadas != 0 {
		t.Fatalf("a entrada frouxa foi reentregue: %+v", segunda)
	}
	if err := s.AuditaAgenda(); err != nil {
		t.Fatal(err)
	}
}

// O REPLAY DO TAIL reconstrói a agenda junto com os índices de que ela deriva.
//
// A janela é a de sempre: records no segmento que o manifest ainda não conhece.
// A agenda passa por ela de graça, porque applyRecord é o único caminho de
// aplicação de estado e o replay usa o mesmo — mas "de graça" é uma afirmação,
// e sem teste ela seria só uma expectativa.
func TestAgendaEhReconstruidaPeloReplayDoTail(t *testing.T) {
	dir := t.TempDir()
	s, rel := storeComExpiracao(t, dir, time.Hour)
	escrita := rel.t
	id, _ := mustPut(t, s, "A", 1, payloadComEscrita(t, s, "A", "Start", escrita), nil)
	// SEM checkpoint: nem o record nem a agenda estão duráveis.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, rel2 := storeComExpiracao(t, dir, time.Hour)
	rel2.t = escrita
	if v := s2.agenda.Get(id); v != uint64(escrita.Add(time.Hour).UnixMilli()) {
		t.Fatalf("depois do replay a agenda diz %d, quer %d", v, escrita.Add(time.Hour).UnixMilli())
	}
	if err := s2.AuditaAgenda(); err != nil {
		t.Fatal(err)
	}
	// E o vencimento veio do PAYLOAD, não do relógio do replay: o coletor
	// entrega a linha no prazo original.
	rel2.t = escrita.Add(2 * time.Hour)
	r, err := s2.ExpiraUmaPassada(context.Background(), limitesDeTeste())
	if err != nil || r.Expiradas != 1 {
		t.Fatalf("a linha replayada não venceu no prazo do payload: %+v (err=%v)", r, err)
	}
}

// A CARÊNCIA não gera record, e é por isso que ela merece um teste de crash
// próprio.
//
// Uma queda depois da carência e antes do checkpoint pode deixar o instante
// de escrita gravado e a agenda para trás, ou o contrário — o replay do tail
// não repõe nenhum dos dois, porque não há record que os descreva. Os dois
// desencontros têm de se resolver sozinhos na primeira visita, e nenhum pode
// deixar a linha imortal.
func TestCarenciaInterrompidaSeResolveNaVisitaSeguinte(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, opcoesComExpiracao(dir, time.Hour))
	rel := &relogioDeTeste{t: time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)}
	s.UsaRelogio(rel.agora)
	id, _ := mustPut(t, s, "legada", 1, payloadSemEscrita(t, s, "legada"), nil)

	// O desencontro: o instante de escrita foi gravado e a agenda ficou no
	// sentinela de carência, como se a queda tivesse acontecido entre os dois.
	if err := s.escritas.Set(id, rel.t.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if s.agenda.Get(id) != rowindex.Carencia {
		t.Fatal("o cenário não montou: a agenda não está na carência")
	}

	r, err := s.ExpiraUmaPassada(context.Background(), limitesDeTeste())
	if err != nil {
		t.Fatal(err)
	}
	if r.Carencias != 0 || r.Expiradas != 0 {
		t.Fatalf("a passada agiu sobre uma linha que já tinha instante: %+v", r)
	}
	if v := s.agenda.Get(id); v != uint64(rel.t.Add(time.Hour).UnixMilli()) {
		t.Fatalf("a agenda ficou em %d; a carência pendente não foi resolvida", v)
	}
	// E a linha vence no prazo, em vez de ficar presa no sentinela.
	rel.avanca(2 * time.Hour)
	r2, err := s.ExpiraUmaPassada(context.Background(), limitesDeTeste())
	if err != nil || r2.Expiradas != 1 {
		t.Fatalf("a linha não venceu depois da carência resolvida: %+v (err=%v)", r2, err)
	}
}
