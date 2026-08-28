package storage

import (
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
			if agendadoVencido != s.prazoVencido(id) {
				ms, conhecido := s.escritas.Get(id)
				t.Fatalf("%s: row %d diverge — agenda diz vencida=%v (venc=%d, agora=%d), prazoVencido diz %v (escrita=%d conhecido=%v, duração=%v)",
					momento, id, agendadoVencido, venc, agora, s.prazoVencido(id), ms, conhecido, s.duracaoEfetiva(id))
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
// É essa a fronteira contra o débito 22, que é sobre bitmap e hash: aqueles
// indexam valores EXTRAÍDOS do payload, e reconstruí-los exige ler todo
// record de todo segmento — a janela de 18,8 h que o piloto teria de pagar.
// Aqui não há record nenhum a ler.
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
