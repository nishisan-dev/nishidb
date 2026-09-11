package storage

import (
	"context"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func limitesOfTest() LimitesOfExpiration {
	return LimitesOfExpiration{MaxByPass: 1000, TempoByPass: 5 * time.Second}
}

// O caminho principal: quem passou do prazo é marcado, quem não passou fica.
func TestPassadaMarcaSoQuemVenceu(t *testing.T) {
	s, rel := storeWithExpiration(t, t.TempDir(), time.Hour)
	velha, _ := mustPut(t, s, "velha", 1, payloadWithWrite(t, s, "velha", "Start", rel.t), nil)

	rel.advance(90 * time.Minute)
	nw, _ := mustPut(t, s, "nova", 1, payloadWithWrite(t, s, "nova", "Start", rel.t), nil)

	r, err := s.ExpiraOnePass(context.Background(), limitesOfTest())
	if err != nil {
		t.Fatal(err)
	}
	if r.Expiradas != 1 {
		t.Fatalf("expiradas=%d, quer 1 (só a velha): %+v", r.Expiradas, r)
	}
	if _, _, ok, _ := s.GetByRowID(s.presente(), velha, nil); ok {
		t.Error("a entidade vencida continua visível")
	}
	if _, _, ok, _ := s.GetByRowID(s.presente(), nw, nil); !ok {
		t.Error("a entidade dentro do prazo foi marcada")
	}
}

// O freio de QUANTIDADE: marcar é escrever, e uma leva grande disputaria o
// lock com a ingestão.
func TestPassadaParaNoLimiteDeQuantidade(t *testing.T) {
	s, rel := storeWithExpiration(t, t.TempDir(), time.Hour)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		mustPut(t, s, k, 1, payloadWithWrite(t, s, k, "Start", rel.t), nil)
	}
	rel.advance(2 * time.Hour)

	r, err := s.ExpiraOnePass(context.Background(), LimitesOfExpiration{MaxByPass: 2, TempoByPass: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if r.Expiradas != 2 || !r.BateuCeiling {
		t.Fatalf("expiradas=%d bateuTeto=%v; quer 2 e true", r.Expiradas, r.BateuCeiling)
	}
	// A passada seguinte retoma de onde parou, sem reexaminar as já marcadas.
	r2, err := s.ExpiraOnePass(context.Background(), LimitesOfExpiration{MaxByPass: 2, TempoByPass: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if r2.Expiradas != 2 {
		t.Fatalf("segunda passada expirou %d, quer 2", r2.Expiradas)
	}
	r3, err := s.ExpiraOnePass(context.Background(), limitesOfTest())
	if err != nil {
		t.Fatal(err)
	}
	if r3.Expiradas != 1 {
		t.Fatalf("terceira passada expirou %d, quer a última (1)", r3.Expiradas)
	}
}

// O freio de TEMPO: varrer muito sem achar nada não escreve, mas seguraria a
// goroutine. O relógio de teste avança sozinho a cada leitura, então o limite
// é atingido sem o teste dormir.
func TestPassadaParaNoLimiteDeTempo(t *testing.T) {
	s, rel := storeWithExpiration(t, t.TempDir(), time.Hour)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		mustPut(t, s, k, 1, payloadWithWrite(t, s, k, "Start", rel.t), nil)
	}
	rel.advance(2 * time.Hour)

	// Cada consulta ao relógio empurra 1s: o teto de 2s cai em poucas voltas.
	base := rel.t
	passos := 0
	s.UsaRelogio(func() time.Time {
		passos++
		return base.Add(time.Duration(passos) * time.Second)
	})
	r, err := s.ExpiraOnePass(context.Background(), LimitesOfExpiration{MaxByPass: 1000, TempoByPass: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if !r.BateuCeiling {
		t.Fatalf("a passada não parou pelo tempo: %+v", r)
	}
	if r.Expiradas >= 5 {
		t.Fatalf("o limite de tempo não freou nada: %+v", r)
	}
}

// Carência: entidade materializada ANTES de a expiração ser ligada não tem
// instante de escrita, e não pode ficar imortal por acidente de calendário —
// nem sumir por falta de informação.
func TestPassadaDaCarenciaAQuemNaoTemInstante(t *testing.T) {
	s, rel := storeWithExpiration(t, t.TempDir(), time.Hour)
	id, _ := mustPut(t, s, "legada", 1, payloadWithoutWrite(t, s, "legada"), nil)

	rel.advance(100 * time.Hour) // muito além do prazo, mas ela não tem prazo
	r, err := s.ExpiraOnePass(context.Background(), limitesOfTest())
	if err != nil {
		t.Fatal(err)
	}
	if r.Expiradas != 0 {
		t.Fatal("entidade sem instante de escrita foi marcada")
	}
	if _, _, ok, _ := s.GetByRowID(s.presente(), id, nil); !ok {
		t.Fatal("entidade sem instante sumiu")
	}
	// Agora ela TEM prazo, contado a partir da carência.
	rel.advance(30 * time.Minute)
	if _, _, ok, _ := s.GetByRowID(s.presente(), id, nil); !ok {
		t.Fatal("expirou antes do prazo cheio de carência")
	}
	rel.advance(31 * time.Minute)
	if _, _, ok, _ := s.GetByRowID(s.presente(), id, nil); ok {
		t.Fatal("a carência não venceu: a entidade ficou imortal")
	}
}

// A DESCOBERTA OPORTUNISTA: a compaction já leu o record e já está no lock;
// ela grava a lápide direto no destino em vez de copiar o payload grande de
// uma entidade que não aparece em consulta nenhuma.
func TestCompactionMarcaOVencidoQueEncontra(t *testing.T) {
	s := openStore(t, Options{
		Dir: t.TempDir(), SegmentSize: 8 << 10,
		ActiveTTL: true, FallbackDuration: time.Hour,
		WriteInstantFieldName: "_writtenAt", TTLFieldName: "_expiresIn",
	})
	rel := &relogioOfTest{t: time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)}
	s.UsaRelogio(rel.agora)

	write := func(k string, seq int64) types.RowID {
		t.Helper()
		p := payloadOrdenado(t, s, map[string]types.Value{
			"id":         {Type: types.TypeString, Bytes: []byte(k)},
			"status":     {Type: types.TypeString, Bytes: []byte(k + "-payload-de-tamanho-razoavel-para-encher-segmento")},
			"_writtenAt": {Type: types.TypeTimestamp, I64: rel.t.UnixMilli()},
		})
		rec, _, pres, err := s.State([]byte(k), nil)
		if err != nil {
			t.Fatal(err)
		}
		var prev *types.Record
		if pres.Exists() {
			prev = &rec
		}
		id, _, err := s.Put([]byte(k), seq, 0, p, prev)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}

	// Abandonadas: escritas uma vez e nunca mais tocadas.
	abandonadas := []types.RowID{write("abandonada-1", 1), write("abandonada-2", 1)}

	// Ativas: muitas rodadas de update, que é o que produz garbage e faz
	// segmentos selados virarem candidatos. Sem isso a compaction não elege
	// nada e a descoberta oportunista nunca acontece — é o ponto cego que o
	// coletor existe para cobrir.
	ativas := make([]string, 20)
	for i := range ativas {
		ativas[i] = "ativa-" + string(rune('a'+i))
	}
	rodada := int64(1)
	for r := 0; r < 15; r++ {
		for _, k := range ativas {
			write(k, rodada)
		}
		rodada++
	}

	rel.advance(2 * time.Hour) // TUDO venceu, inclusive as ativas

	// As ativas recebem update DEPOIS do avanço: renovam o prazo e continuam
	// gerando garbage nos segmentos antigos.
	for r := 0; r < 15; r++ {
		for _, k := range ativas {
			write(k, rodada)
		}
		rodada++
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if len(s.candidatos(0.5)) == 0 {
		t.Fatal("nenhum segmento candidato: o cenário não exercita a descoberta oportunista")
	}

	before := s.expiredCount.Load()
	if _, err := s.CompactaUmaVez(context.Background(), 0.5); err != nil {
		t.Fatal(err)
	}
	if depois := s.expiredCount.Load(); depois == before {
		t.Fatal("a compaction copiou os vencidos para frente em vez de marcá-los")
	}
	for _, id := range abandonadas {
		if _, _, ok, _ := s.GetByRowID(s.presente(), id, nil); ok {
			t.Errorf("row abandonada %d continua visível depois da compaction", id)
		}
	}
	// E as ativas, renovadas, sobrevivem: a compaction não pode marcar quem
	// está no prazo.
	for _, k := range ativas {
		if _, _, pres, _ := s.State([]byte(k), nil); pres != types.PresenceLive {
			t.Errorf("row ativa %q foi marcada: %v", k, pres)
		}
	}
}

// A compaction NÃO pode marcar quem ainda está no prazo — seria apagar dado
// vivo numa rotina que existe para recuperar espaço.
func TestCompactionNaoMarcaQuemEstaNoPrazo(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{
		Dir: dir, SegmentSize: 4 << 10,
		ActiveTTL: true, FallbackDuration: 100 * time.Hour,
		WriteInstantFieldName: "_writtenAt", TTLFieldName: "_expiresIn",
	})
	rel := &relogioOfTest{t: time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)}
	s.UsaRelogio(rel.agora)
	grande := make([]byte, 512)
	var ids []types.RowID
	for _, k := range []string{"a", "b", "c", "d"} {
		p := payloadOrdenado(t, s, map[string]types.Value{
			"id":         {Type: types.TypeString, Bytes: []byte(k)},
			"status":     {Type: types.TypeString, Bytes: grande},
			"_writtenAt": {Type: types.TypeTimestamp, I64: rel.t.UnixMilli()},
		})
		id, _ := mustPut(t, s, k, 1, p, nil)
		ids = append(ids, id)
	}
	if _, err := s.CompactaUmaVez(context.Background(), 0.99); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if _, _, ok, err := s.GetByRowID(s.presente(), id, nil); err != nil || !ok {
			t.Fatalf("a compaction marcou uma row dentro do prazo: id=%d ok=%v err=%v", id, ok, err)
		}
	}
}

// Sem expiração declarada não há goroutine nem passada: o dataset não paga
// thread para não fazer nada.
func TestSemExpiracaoNaoHaColetor(t *testing.T) {
	s := openStore(t, Options{Dir: t.TempDir()})
	if s.HasExpiration() {
		t.Fatal("HasExpiration verdadeiro sem a chave declarada")
	}
	s.StartExpiration(context.Background(), limitesOfTest())
	if s.expActive.Load() {
		t.Fatal("o coletor subiu num dataset sem expiração")
	}
	r, err := s.ExpiraOnePass(context.Background(), limitesOfTest())
	if err != nil || r.Examinadas != 0 {
		t.Fatalf("passada em dataset sem expiração: %+v, %v", r, err)
	}
}

func TestLimitesInvalidosSaoRecusados(t *testing.T) {
	s, _ := storeWithExpiration(t, t.TempDir(), time.Hour)
	for _, lim := range []LimitesOfExpiration{
		{MaxByPass: 0, TempoByPass: time.Second},
		{MaxByPass: 10, TempoByPass: 0},
		{MaxByPass: -1, TempoByPass: time.Second},
	} {
		if _, err := s.ExpiraOnePass(context.Background(), lim); err == nil {
			t.Errorf("limite inválido aceito: %+v", lim)
		}
	}
}

// O Close aguarda a passada em voo: sem isso ele desmapearia o índice sob os
// pés dela.
func TestParaExpiracaoAguardaAPassadaEmVoo(t *testing.T) {
	s, rel := storeWithExpiration(t, t.TempDir(), time.Hour)
	for i := 0; i < 50; i++ {
		k := string(rune('a' + i%26))
		mustPut(t, s, k, int64(i+1), payloadWithWrite(t, s, k, "Start", rel.t), nil)
	}
	rel.advance(2 * time.Hour)

	s.StartExpiration(context.Background(), limitesOfTest())
	if !s.expActive.Load() {
		t.Fatal("o medidor não subiu junto com a chamada")
	}
	s.ForExpiration()
	if s.expActive.Load() {
		t.Fatal("o medidor ficou em 1 depois do ForExpiration")
	}
	// Idempotente, como o ForCompaction.
	s.ForExpiration()
}

// Cancelamento é checado a cada entrada, e não só entre marcas: uma passada
// que não acha nada para expirar não passaria por ponto de cancelamento
// nenhum, e o Close esperaria a varredura inteira.
func TestPassadaHonraOCancelamentoMesmoSemNadaAExpirar(t *testing.T) {
	s, rel := storeWithExpiration(t, t.TempDir(), 100*time.Hour)
	for i := 0; i < 100; i++ {
		k := string(rune('a'+i%26)) + string(rune('0'+i/26))
		mustPut(t, s, k, 1, payloadWithWrite(t, s, k, "Start", rel.t), nil)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, err := s.ExpiraOnePass(ctx, limitesOfTest())
	if err != nil {
		t.Fatal(err)
	}
	if r.Examinadas > 1 {
		t.Fatalf("a passada ignorou o cancelamento e examinou %d entradas", r.Examinadas)
	}
}

// O PONTO CEGO, e é ele que justifica o coletor existir ao lado da descoberta
// oportunista.
//
// Um segmento só vira candidato quando mais da metade dele é garbage, e
// garbage vem de UPDATE. Uma entidade abandonada, por definição, não recebe
// update — então o segmento onde ela mora continua "cheio de coisa viva" e a
// compaction nunca o elege. A rota oportunista não a alcança NUNCA, por mais
// que se espere.
//
// O teste separa as duas rotas: escreve entidades uma única vez (nenhum
// garbage), roda a compaction até cansar e exige que ela não tenha marcado
// nada, e só então mostra o coletor pegando o que ela não podia pegar.
func TestSoOColetorAlcancaAbandonadaEmSegmentoSemGarbage(t *testing.T) {
	s, rel := storeWithExpiration(t, t.TempDir(), time.Hour)

	var ids []types.RowID
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		// Uma escrita por key: nada vira versão superada, nada vira garbage.
		id, _ := mustPut(t, s, k, 1, payloadWithWrite(t, s, k, "Start", rel.t), nil)
		ids = append(ids, id)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	rel.advance(2 * time.Hour) // todas venceram

	// A compaction não tem o que eleger, então não descobre nada.
	if cands := s.candidatos(0.5); len(cands) != 0 {
		t.Fatalf("o cenário não isola o ponto cego: %d candidato(s)", len(cands))
	}
	for i := 0; i < 5; i++ {
		if _, err := s.CompactaUmaVez(context.Background(), 0.5); err != nil {
			t.Fatal(err)
		}
	}
	if n := s.expiredCount.Load(); n != 0 {
		t.Fatalf("a compaction marcou %d entidades num segmento que ela nem elege", n)
	}
	for _, id := range ids {
		if _, ok := s.locs.Get(id); !ok {
			t.Fatalf("row %d perdeu location", id)
		}
	}

	// O coletor alcança, porque ele varre o índice e não os segmentos.
	r, err := s.ExpiraOnePass(context.Background(), limitesOfTest())
	if err != nil {
		t.Fatal(err)
	}
	if r.Expiradas != len(ids) {
		t.Fatalf("o coletor marcou %d de %d: %+v", r.Expiradas, len(ids), r)
	}
}

// Coletor e ingestão escrevem no MESMO índice, cada um pelo seu caminho: a
// carência pelo coletor, o applyRecord pela ingestão. Este teste os coloca
// para rodar juntos.
//
// Ele NÃO prova ausência de corrida de dados — não haveria: o valor do índice
// é gravado com AtomicStore64 e o remapeamento é protegido pelo mutex da
// região, então as duas rotas já eram seguras para a memória. O que ele
// exerce é o caminho concorrente inteiro, que nenhum outro teste monta:
// carência e ingestão disputando o mesmo lock de escrita, com o índice
// crescendo por baixo das duas.
func TestColetorEIngestaoRodamJuntosSemQuebrar(t *testing.T) {
	s, rel := storeWithExpiration(t, t.TempDir(), time.Hour)
	// Metade das entidades sem instante, para a carência ter trabalho.
	for i := 0; i < 200; i++ {
		k := "k" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		mustPut(t, s, k, 1, payloadWithoutWrite(t, s, k), nil)
	}

	pronto := make(chan struct{})
	erros := make(chan error, 2)
	go func() {
		defer close(pronto)
		for i := 0; i < 200; i++ {
			k := "n" + string(rune('a'+i%26)) + string(rune('a'+i/26))
			p := payloadWithWrite(t, s, k, "Start", rel.t)
			if _, _, err := s.Put([]byte(k), 1, 0, p, nil); err != nil {
				erros <- err
				return
			}
		}
	}()

	for i := 0; i < 20; i++ {
		if _, err := s.ExpiraOnePass(context.Background(), limitesOfTest()); err != nil {
			erros <- err
			break
		}
	}
	<-pronto
	close(erros)
	for err := range erros {
		t.Fatal(err)
	}
}

// A passada que SÓ dá carência precisa aparecer no log: é exatamente o que
// acontece ao ligar a expiração num dataset já materializado, e sem contá-la
// o operador não distingue "está trabalhando" de "não subiu".
func TestPassadaContaAsCarenciasQueDeu(t *testing.T) {
	s, _ := storeWithExpiration(t, t.TempDir(), 90*24*time.Hour)
	const n = 50
	for i := 0; i < n; i++ {
		k := "k" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		mustPut(t, s, k, 1, payloadWithoutWrite(t, s, k), nil)
	}
	r, err := s.ExpiraOnePass(context.Background(), limitesOfTest())
	if err != nil {
		t.Fatal(err)
	}
	if r.Carencias != n {
		t.Fatalf("carencias=%d, quer %d: %+v", r.Carencias, n, r)
	}
	// Segunda passada não tem mais o que dar.
	r2, err := s.ExpiraOnePass(context.Background(), limitesOfTest())
	if err != nil {
		t.Fatal(err)
	}
	if r2.Carencias != 0 {
		t.Fatalf("a carência foi dada duas vezes: %+v", r2)
	}
}
