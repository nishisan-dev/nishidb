package counter

import (
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/keyformat"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// FieldIDs do dataset de teste, em ordem crescente como o payload exige.
const (
	cOLT  types.FieldID = 1
	cSlot types.FieldID = 2
	cPort types.FieldID = 3
	cAuth types.FieldID = 4
)

// testEvaluator é o `where authenticated = true` sem passar pelo binder:
// o contador só conhece a interface, e o teste unitário não precisa de um
// schema compilado para exercitá-la.
type testEvaluator struct{ fr encoding.FieldReader }

func (a *testEvaluator) Evaluate(payload []byte) (bool, error) {
	a.fr.Reset(payload)
	for {
		id, v, ok := a.fr.Next()
		if !ok {
			return false, a.fr.Err()
		}
		if id == cAuth {
			return v.Type == types.TypeBool && v.I64 == 1, nil
		}
	}
}

func defByOLT(name string, withWhere bool, ceiling uint32) Definition {
	d := Definition{
		Name:      name,
		Columns:   []types.FieldID{cOLT},
		Names:     []string{"olt"},
		Types:     []types.FieldType{types.TypeString},
		MaxGroups: ceiling,
	}
	d.Format = keyformat.Default(keyformat.Columns(d.Names, d.Types))
	if withWhere {
		d.Where = "authenticated = true"
		d.NewEvaluator = func() Evaluator { return &testEvaluator{} }
	}
	return d
}

func defByPON(name string, ceiling uint32) Definition {
	d := Definition{
		Name:         name,
		Columns:      []types.FieldID{cOLT, cSlot, cPort},
		Names:        []string{"olt", "slot", "port"},
		Types:        []types.FieldType{types.TypeString, types.TypeInt64, types.TypeInt64},
		Where:        "authenticated = true",
		NewEvaluator: func() Evaluator { return &testEvaluator{} },
		MaxGroups:    ceiling,
	}
	d.Format = keyformat.Default(keyformat.Columns(d.Names, d.Types))
	return d
}

// payload monta o estado de uma linha. olt vazio OMITE a coluna.
func payload(t *testing.T, olt string, slot, port int64, auth bool) []byte {
	t.Helper()
	var buf []byte
	var err error
	if olt != "" {
		if buf, err = encoding.AppendField(buf, cOLT, types.Value{Type: types.TypeString, Bytes: []byte(olt)}); err != nil {
			t.Fatal(err)
		}
	}
	if buf, err = encoding.AppendField(buf, cSlot, types.Value{Type: types.TypeInt64, I64: slot}); err != nil {
		t.Fatal(err)
	}
	if buf, err = encoding.AppendField(buf, cPort, types.Value{Type: types.TypeInt64, I64: port}); err != nil {
		t.Fatal(err)
	}
	a := types.Value{Type: types.TypeBool}
	if auth {
		a.I64 = 1
	}
	if buf, err = encoding.AppendField(buf, cAuth, a); err != nil {
		t.Fatal(err)
	}
	return buf
}

func openCounter(t *testing.T, dir string, def Definition, recreate bool) *Counter {
	t.Helper()
	c, err := Open(dir, def, 1024, recreate)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// totalOf devolve total e refs do grupo cuja chave é a OLT dada.
func totalOf(t *testing.T, c *Counter, olt string) (total, refs int64) {
	t.Helper()
	key, err := valuekey.AppendComposite(nil, []types.Value{{Type: types.TypeString, Bytes: []byte(olt)}})
	if err != nil {
		t.Fatal(err)
	}
	id, ok := c.dict.ByKey(key)
	if !ok {
		return 0, 0
	}
	_, _, total, refs, _ = c.Group(id)
	return total, refs
}

// requireCoherentAggregates é o oráculo dos agregados: eles são a SEGUNDA fonte
// para o mesmo fato que os vetores por grupo, e o que os prova é a comparação
// com a primeira. Todo teste de agregado passa por aqui — sem isso um teste
// que só olhasse os dois números aceitaria qualquer aritmética consistente
// consigo mesma.
//
// A soma é sobre TODOS os ids, vivos ou não: grupo aposentado tem refs 0 e,
// por totals[g] <= refs[g], total 0 — logo não muda a soma, que é exatamente
// a razão de a aposentadoria não precisar mexer nos agregados.
func requireCoherentAggregates(t *testing.T, c *Counter) {
	t.Helper()
	var rows, total int64
	for id := uint32(1); id < c.Limit(); id++ {
		_, _, tg, rg, _ := c.Group(id)
		rows += rg
		total += tg
	}
	stats := c.Stats()
	if stats.Rows != rows || stats.Total != total {
		t.Fatalf("agregados = linhas %d total %d; a soma dos grupos dá %d e %d",
			stats.Rows, stats.Total, rows, total)
	}
}

func TestTransicaoNovaLinhaContaNoGrupo(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLT("aut", true, 100), false)
	if err := c.Transition(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.Transition(1, payload(t, "A", 1, 2, false), false); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalOf(t, c, "A"); total != 1 || refs != 2 {
		t.Fatalf("A: total %d refs %d, quer 1 e 2", total, refs)
	}
	if c.Transitions() != 2 {
		t.Fatalf("transições = %d, quer 2", c.Transitions())
	}
}

// TestTransicaoNovaLinhaContaNosAgregados: a mesma bancada do teste acima,
// medida no contador inteiro. Duas linhas na OLT A, uma satisfazendo o where.
func TestTransicaoNovaLinhaContaNosAgregados(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLT("aut", true, 100), false)
	if err := c.Transition(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.Transition(1, payload(t, "A", 1, 2, false), false); err != nil {
		t.Fatal(err)
	}
	// Uma terceira linha em OUTRO grupo: o agregado é do contador, não do grupo.
	if err := c.Transition(2, payload(t, "B", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if stats := c.Stats(); stats.Rows != 3 || stats.Total != 2 {
		t.Fatalf("agregados = linhas %d total %d, quer 3 e 2", stats.Rows, stats.Total)
	}
	requireCoherentAggregates(t, c)
}

// TestAgregadosAcompanhamAsDuasPontasDaTransicao: mudar de grupo tira de um e
// põe no outro, e o agregado não pode mexer — a linha continua UMA. Mudar a
// condição no MESMO grupo mexe só no total.
func TestAgregadosAcompanhamAsDuasPontasDaTransicao(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLT("aut", true, 100), false)
	if err := c.Transition(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if stats := c.Stats(); stats.Rows != 1 || stats.Total != 1 {
		t.Fatalf("depois da entrada: linhas %d total %d, quer 1 e 1", stats.Rows, stats.Total)
	}
	// Troca de grupo, mesma condição: os dois agregados ficam parados.
	if err := c.Transition(0, payload(t, "B", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if stats := c.Stats(); stats.Rows != 1 || stats.Total != 1 {
		t.Fatalf("depois da troca de grupo: linhas %d total %d, quer 1 e 1", stats.Rows, stats.Total)
	}
	requireCoherentAggregates(t, c)
	// Mesma linha, mesmo grupo, condição deixa de valer: só o total desce.
	if err := c.Transition(0, payload(t, "B", 1, 1, false), false); err != nil {
		t.Fatal(err)
	}
	if stats := c.Stats(); stats.Rows != 1 || stats.Total != 0 {
		t.Fatalf("depois de perder a condição: linhas %d total %d, quer 1 e 0", stats.Rows, stats.Total)
	}
	requireCoherentAggregates(t, c)
}

// TestLapideDescontaOsAgregados: a lápide entra como "sem grupo", e é a
// entrada ANTIGA que lembra de onde a linha saiu.
func TestLapideDescontaOsAgregados(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLT("aut", true, 100), false)
	for i, cond := range []bool{true, false} {
		if err := c.Transition(types.RowID(i), payload(t, "A", 1, int64(i), cond), false); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Transition(0, nil, true); err != nil {
		t.Fatal(err)
	}
	if stats := c.Stats(); stats.Rows != 1 || stats.Total != 0 {
		t.Fatalf("depois da lápide: linhas %d total %d, quer 1 e 0", stats.Rows, stats.Total)
	}
	if err := c.Transition(1, nil, true); err != nil {
		t.Fatal(err)
	}
	if stats := c.Stats(); stats.Rows != 0 || stats.Total != 0 {
		t.Fatalf("com tudo lapidado: linhas %d total %d, quer 0 e 0", stats.Rows, stats.Total)
	}
	requireCoherentAggregates(t, c)
}

// TestAgregadoNaoDescontaEntradaAlemDoDicionario: entrada apontando para um id
// que o dicionário não tem — janela de crash — não decrementa grupo nenhum,
// porque não existe slot. O agregado tem de ficar parado JUNTO.
//
// O número que sai daqui é 3 linhas para 2 rows, e é o comportamento correto:
// a rowID 1 é contada duas vezes no grupo A porque a entrada que a tirava de
// lá aponta para o nada. O agregado não existe para consertar isso — existe
// para dizer a MESMA coisa que os grupos dizem. Quem conserta é a Fold, que
// trata a entrada como órfã, e a auditoria, que a acusa. Um agregado que
// descesse aqui ficaria em 2 contra grupos somando 3, e a divergência não
// teria como aparecer.
func TestAgregadoNaoDescontaEntradaAlemDoDicionario(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLT("aut", true, 100), false)
	if err := c.Transition(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.Transition(1, payload(t, "A", 1, 2, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.ApplyOverlay(); err != nil {
		t.Fatal(err)
	}
	// Um id que o dicionário nunca emitiu, escrito por baixo do contador.
	if err := c.vector.Set(1, conditionBit|99); err != nil {
		t.Fatal(err)
	}
	if err := c.Transition(1, payload(t, "A", 1, 2, true), false); err != nil {
		t.Fatal(err)
	}
	if stats := c.Stats(); stats.Rows != 3 || stats.Total != 3 {
		t.Fatalf("agregados = linhas %d total %d, quer 3 e 3 (a rowID 1 conta duas vezes no grupo A)", stats.Rows, stats.Total)
	}
	requireCoherentAggregates(t, c)
}

// TestReprocessarOMesmoEventoNaoMexe é a segunda linha da tabela da proposta
// §5.5: a reentrega é uma transição de X para X, e o contador não tem o que
// ajustar. Nenhuma contabilidade de "quais mensagens já contei" existe.
func TestReprocessarOMesmoEventoNaoMexe(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLT("aut", true, 100), false)
	p := payload(t, "A", 1, 1, true)
	for i := 0; i < 3; i++ {
		if err := c.Transition(7, p, false); err != nil {
			t.Fatal(err)
		}
	}
	if total, refs := totalOf(t, c, "A"); total != 1 || refs != 1 {
		t.Fatalf("A: total %d refs %d, quer 1 e 1", total, refs)
	}
	if c.Transitions() != 1 {
		t.Fatalf("reprocessar não é transição: transições = %d, quer 1", c.Transitions())
	}
}

func TestMudarDeGrupoAjustaAsDuasPontas(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLT("aut", true, 100), false)
	if err := c.Transition(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.Transition(0, payload(t, "B", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalOf(t, c, "A"); total != 0 || refs != 0 {
		t.Fatalf("A depois da migração: total %d refs %d, quer 0 e 0", total, refs)
	}
	if total, refs := totalOf(t, c, "B"); total != 1 || refs != 1 {
		t.Fatalf("B depois da migração: total %d refs %d, quer 1 e 1", total, refs)
	}
	// A OLT antiga NÃO some: o grupo sobrevive ao próprio total (proposta §5.4).
	if _, _, _, _, live := c.Group(1); !live {
		t.Fatal("o grupo que esvaziou continua vivo, com total 0")
	}
	// Só a condição mudando também é transição, e só mexe no total.
	if err := c.Transition(0, payload(t, "B", 1, 1, false), false); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalOf(t, c, "B"); total != 0 || refs != 1 {
		t.Fatalf("B com condição falsa: total %d refs %d, quer 0 e 1", total, refs)
	}
}

func TestLapideDescontaEPreservaOGrupo(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLT("aut", true, 100), false)
	if err := c.Transition(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	// A lápide chega com payload reduzido (só o instante de escrita) e a flag:
	// o contador não lê nada dela — a entrada antiga do vetor é a memória.
	if err := c.Transition(0, nil, true); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalOf(t, c, "A"); total != 0 || refs != 0 {
		t.Fatalf("A depois da lápide: total %d refs %d, quer 0 e 0", total, refs)
	}
	if _, _, total, _, live := c.Group(1); !live || total != 0 {
		t.Fatalf("o grupo permanece na tabela com total 0 (vivo=%v total=%d)", live, total)
	}
	// Lápide sobre lápide (replay) não mexe.
	if err := c.Transition(0, nil, true); err != nil {
		t.Fatal(err)
	}
	if c.Transitions() != 2 {
		t.Fatalf("transições = %d, quer 2", c.Transitions())
	}
}

func TestRessurreicaoVoltaAContar(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLT("aut", true, 100), false)
	if err := c.Transition(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.Transition(0, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := c.Transition(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalOf(t, c, "A"); total != 1 || refs != 1 {
		t.Fatalf("A ressuscitada: total %d refs %d, quer 1 e 1", total, refs)
	}
}

// TestLinhaSemColunaDeAgrupamentoNaoConta: entidade sem a coluna (ausente ou
// nula) recebe o grupo reservado e NÃO é contada (proposta §6.3).
func TestLinhaSemColunaDeAgrupamentoNaoConta(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLT("aut", true, 100), false)
	if err := c.Transition(0, payload(t, "", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if c.Limit() != 1 {
		t.Fatalf("nenhum grupo devia ter sido internado, limite = %d", c.Limit())
	}
	if c.Transitions() != 0 {
		t.Fatal("sem grupo antes e depois não é transição")
	}
	// A linha ganha a coluna: entra no grupo.
	if err := c.Transition(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalOf(t, c, "A"); total != 1 || refs != 1 {
		t.Fatalf("A: total %d refs %d", total, refs)
	}
	// E perde de novo: sai.
	if err := c.Transition(0, payload(t, "", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalOf(t, c, "A"); total != 0 || refs != 0 {
		t.Fatalf("A depois de perder a coluna: total %d refs %d", total, refs)
	}
}

func TestSemWhereContaTodaLinhaComGrupo(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLT("serv", false, 100), false)
	if err := c.Transition(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.Transition(1, payload(t, "A", 1, 1, false), false); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalOf(t, c, "A"); total != 2 || refs != 2 {
		t.Fatalf("sem where: total %d refs %d, quer 2 e 2", total, refs)
	}
}

func TestTetoDeGruposRecusaEConta(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLT("aut", true, 1), false)
	if err := c.Transition(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	// A segunda OLT não cabe: a linha entra como "sem grupo", e a recusa conta.
	if err := c.Transition(1, payload(t, "B", 1, 1, true), false); err != nil {
		t.Fatalf("o teto não pode virar erro de escrita: %v", err)
	}
	stats := c.Stats()
	if stats.Refused != 1 || stats.Groups != 1 {
		t.Fatalf("estatísticas = %+v, quer Refused 1 e Groups 1", stats)
	}
	if total, refs := totalOf(t, c, "B"); total != 0 || refs != 0 {
		t.Fatalf("B não pode ter sido contada: %d %d", total, refs)
	}
	// A linha recusada, ao ser reescrita numa OLT que cabe, entra normalmente.
	if err := c.Transition(1, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalOf(t, c, "A"); total != 2 || refs != 2 {
		t.Fatalf("A: total %d refs %d, quer 2 e 2", total, refs)
	}
}

// TestDobraReproduzOsTotais é o teste que sustenta a consistência através de
// crash: os totais NÃO são persistidos, são dobra do vetor na abertura. Uma
// segunda instância aberta sobre os mesmos arquivos tem de chegar exatamente
// aos mesmos números.
func TestDobraReproduzOsTotais(t *testing.T) {
	dir := t.TempDir()
	def := defByPON("pon", 1000)
	c := openCounter(t, dir, def, false)
	rnd := rand.New(rand.NewSource(7))
	olts := []string{"A", "B", "C", "D"}
	for i := 0; i < 1000; i++ {
		row := types.RowID(rnd.Intn(200))
		if rnd.Intn(10) == 0 {
			if err := c.Transition(row, nil, true); err != nil {
				t.Fatal(err)
			}
			continue
		}
		p := payload(t, olts[rnd.Intn(len(olts))], int64(rnd.Intn(3)), int64(rnd.Intn(4)), rnd.Intn(2) == 1)
		if err := c.Transition(row, p, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.ApplyOverlay(); err != nil {
		t.Fatal(err)
	}
	if err := c.Msync(); err != nil {
		t.Fatal(err)
	}
	snap := c.AuditSnapshot()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	c2 := openCounter(t, dir, def, false)
	if err := c2.Fold(); err != nil {
		t.Fatal(err)
	}
	if c2.Limit() != snap.Limit {
		t.Fatalf("limite %d, quer %d", c2.Limit(), snap.Limit)
	}
	for id := uint32(1); id < snap.Limit; id++ {
		_, _, total, refs, _ := c2.Group(id)
		if total != snap.Totals[id] || refs != snap.Refs[id] {
			t.Fatalf("grupo %d: dobra deu total %d refs %d, quer %d e %d", id, total, refs, snap.Totals[id], snap.Refs[id])
		}
	}
	if c2.Stats().Orphans != 0 {
		t.Fatal("nenhuma entrada órfã era esperada")
	}
}

// TestDobraReproduzOsAgregados é a irmã do teste acima para os agregados do
// contador inteiro: eles também são dobra do vetor, e uma segunda instância
// aberta sobre os mesmos arquivos tem de chegar aos mesmos dois números.
//
// O corpo repete a sequência aleatória de propósito — é a mesma propriedade,
// medida noutro nível: se o agregado deixasse de ser dobra, os grupos ainda
// bateriam e só estes dois números divergiriam.
func TestDobraReproduzOsAgregados(t *testing.T) {
	dir := t.TempDir()
	def := defByPON("pon", 1000)
	c := openCounter(t, dir, def, false)
	rnd := rand.New(rand.NewSource(11))
	olts := []string{"A", "B", "C", "D"}
	for i := 0; i < 1000; i++ {
		row := types.RowID(rnd.Intn(200))
		if rnd.Intn(10) == 0 {
			if err := c.Transition(row, nil, true); err != nil {
				t.Fatal(err)
			}
			continue
		}
		p := payload(t, olts[rnd.Intn(len(olts))], int64(rnd.Intn(3)), int64(rnd.Intn(4)), rnd.Intn(2) == 1)
		if err := c.Transition(row, p, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.ApplyOverlay(); err != nil {
		t.Fatal(err)
	}
	if err := c.Msync(); err != nil {
		t.Fatal(err)
	}
	requireCoherentAggregates(t, c)
	snap := c.AuditSnapshot()
	if snap.Rows == 0 || snap.Total == 0 || snap.Rows == snap.Total {
		// Rows == Total passaria com os dois trocados; a fixture tem where.
		t.Fatalf("a bancada não discrimina: linhas %d, total %d", snap.Rows, snap.Total)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	c2 := openCounter(t, dir, def, false)
	if err := c2.Fold(); err != nil {
		t.Fatal(err)
	}
	stats := c2.Stats()
	if stats.Rows != snap.Rows || stats.Total != snap.Total {
		t.Fatalf("dobra deu linhas %d total %d, quer %d e %d", stats.Rows, stats.Total, snap.Rows, snap.Total)
	}
	requireCoherentAggregates(t, c2)

	// A dobra é idempotente: zerar antes de acumular é o que a torna assim.
	if err := c2.Fold(); err != nil {
		t.Fatal(err)
	}
	if stats2 := c2.Stats(); stats2.Rows != snap.Rows || stats2.Total != snap.Total {
		t.Fatalf("a segunda dobra acumulou sobre a primeira: linhas %d total %d", stats2.Rows, stats2.Total)
	}
}

// TestOverlayNaoVaiAoMmapAntesDoApply: o vetor em disco só muda no
// ApplyOverlay, que o checkpoint chama DEPOIS do segSync. É o que impede a
// página do vetor de ficar à frente do log durável numa cauda rasgada.
func TestOverlayNaoVaiAoMmapAntesDoApply(t *testing.T) {
	dir := t.TempDir()
	def := defByOLT("aut", true, 100)
	c := openCounter(t, dir, def, false)
	for r := types.RowID(0); r < 10; r++ {
		if err := c.Transition(r, payload(t, "A", 1, 1, true), false); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Msync(); err != nil {
		t.Fatal(err)
	}
	// Segunda instância sobre os mesmos arquivos, SEM apply: dobra zero.
	// (O dicionário já tem a OLT A — ele é fsyncado por entrada.)
	peeker := openCounter(t, dir, def, false)
	if err := peeker.Fold(); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalOf(t, peeker, "A"); total != 0 || refs != 0 {
		t.Fatalf("antes do ApplyOverlay o mmap tem de estar zerado: total %d refs %d", total, refs)
	}
	if err := peeker.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.ApplyOverlay(); err != nil {
		t.Fatal(err)
	}
	if err := c.Msync(); err != nil {
		t.Fatal(err)
	}
	peeker2 := openCounter(t, dir, def, false)
	if err := peeker2.Fold(); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalOf(t, peeker2, "A"); total != 10 || refs != 10 {
		t.Fatalf("depois do ApplyOverlay: total %d refs %d, quer 10 e 10", total, refs)
	}
	// E a instância viva continua lendo o próprio overlay depois do apply.
	if err := c.Transition(0, payload(t, "A", 1, 1, false), false); err != nil {
		t.Fatal(err)
	}
	if total, _ := totalOf(t, c, "A"); total != 9 {
		t.Fatalf("total depois do apply e de uma transição = %d, quer 9", total)
	}
}

// TestDobraTrataIdAposentadoComoOrfa: uma entrada em disco apontando para um
// grupo aposentado (ou inexistente) não pode contar em lugar nenhum. Ela é
// reescrita para WithoutGroup — o Open não tem leitores — e contada em Orphans.
func TestDobraTrataIdAposentadoComoOrfa(t *testing.T) {
	dir := t.TempDir()
	def := defByOLT("aut", true, 100)
	c := openCounter(t, dir, def, false)
	if err := c.Transition(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.Transition(1, payload(t, "B", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.ApplyOverlay(); err != nil {
		t.Fatal(err)
	}
	// Retire A por baixo (o dicionário é de teste aqui) e grava uma entrada
	// apontando para um id que nunca existiu.
	if err := c.dict.Retire(1); err != nil {
		t.Fatal(err)
	}
	if err := c.vector.Set(2, 99); err != nil {
		t.Fatal(err)
	}
	if err := c.Msync(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c2 := openCounter(t, dir, def, false)
	if err := c2.Fold(); err != nil {
		t.Fatal(err)
	}
	if stats := c2.Stats(); stats.Orphans != 2 {
		t.Fatalf("órfãs = %d, quer 2 (uma aposentada, uma inexistente)", stats.Orphans)
	}
	if _, _, total, refs, live := c2.Group(1); live || total != 0 || refs != 0 {
		t.Fatalf("grupo aposentado não pode contar: vivo=%v total=%d refs=%d", live, total, refs)
	}
	if total, refs := totalOf(t, c2, "B"); total != 1 || refs != 1 {
		t.Fatalf("B: %d %d", total, refs)
	}
	if v, _ := c2.vector.Get(0); v != WithoutGroup {
		t.Fatalf("a entrada órfã tinha de ser reescrita para WithoutGroup, é %#x", v)
	}
	if v, _ := c2.vector.Get(2); v != WithoutGroup {
		t.Fatalf("a entrada inexistente tinha de ser reescrita para WithoutGroup, é %#x", v)
	}
	// A entrada que grupo nenhum recebeu também não pode ser creditada ao
	// agregado: sobra UMA linha viva, a do grupo B. Sem esta asserção, uma
	// dobra que acumulasse antes do continue da órfã passaria verde com os
	// grupos certos e o agregado inflado pelas duas órfãs.
	if stats := c2.Stats(); stats.Rows != 1 || stats.Total != 1 {
		t.Fatalf("agregados = linhas %d total %d, quer 1 e 1 (as duas órfãs não contam)", stats.Rows, stats.Total)
	}
	requireCoherentAggregates(t, c2)
}

func TestCobreMexidasAcompanhaAsTransicoes(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLT("aut", true, 100), false)
	if err := c.Transition(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.Transition(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if c.Transitions() != 1 {
		t.Fatalf("transições = %d, quer 1 (reprocessar não conta)", c.Transitions())
	}
	c.RecordCoveredTouches(1)
	c.RecordCoveredTouches(0) // nunca regride
	if c.CoveredTouches() != 1 {
		t.Fatalf("cobertas = %d, quer 1", c.CoveredTouches())
	}
}

func TestGrupoCompostoTemColunasTipadas(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByPON("pon", 100), false)
	if err := c.Transition(0, payload(t, "PRCTA_G1M10", 1, 9, true), false); err != nil {
		t.Fatal(err)
	}
	key, text, total, refs, live := c.Group(1)
	if !live || total != 1 || refs != 1 {
		t.Fatalf("grupo 1: vivo=%v total=%d refs=%d", live, total, refs)
	}
	vals, err := valuekey.DecodeComposite(key, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 3 || string(vals[0].Bytes) != "PRCTA_G1M10" || vals[1].I64 != 1 || vals[2].I64 != 9 {
		t.Fatalf("componentes = %+v", vals)
	}
	if string(text) != "PRCTA_G1M10|1|9" {
		t.Fatalf("texto = %q", text)
	}
	if id, ok := c.ByText([]byte("PRCTA_G1M10|1|9")); !ok || id != 1 {
		t.Fatalf("ByText = %d, %v", id, ok)
	}
}

func TestZeraLimpaTotaisEVetorMasMantemODicionario(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLT("aut", true, 100), false)
	if err := c.Transition(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.Clear(); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalOf(t, c, "A"); total != 0 || refs != 0 {
		t.Fatalf("depois do Clear: %d %d", total, refs)
	}
	if c.Limit() != 2 {
		t.Fatal("o dicionário fica: os ids são estáveis")
	}
	// Reconstruir é transicionar de novo, do zero.
	if err := c.Transition(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalOf(t, c, "A"); total != 1 || refs != 1 {
		t.Fatalf("depois de reconstruir: %d %d", total, refs)
	}
}

// TestZeraLimpaOsAgregados: os agregados zeram junto com os vetores. Sem isto,
// a reconstrução — que transiciona de novo cada linha viva — os deixaria com o
// DOBRO do valor ao lado de grupos corretos, e o número errado seria o que sai
// por SQL e por /v1/stats.
func TestZeraLimpaOsAgregados(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLT("aut", true, 100), false)
	for i, cond := range []bool{true, true, false} {
		if err := c.Transition(types.RowID(i), payload(t, "A", 1, int64(i), cond), false); err != nil {
			t.Fatal(err)
		}
	}
	if stats := c.Stats(); stats.Rows != 3 || stats.Total != 2 {
		t.Fatalf("antes do Clear: linhas %d total %d, quer 3 e 2", stats.Rows, stats.Total)
	}
	if err := c.Clear(); err != nil {
		t.Fatal(err)
	}
	if stats := c.Stats(); stats.Rows != 0 || stats.Total != 0 {
		t.Fatalf("depois do Clear: linhas %d total %d, quer 0 e 0", stats.Rows, stats.Total)
	}
	// Reconstruir tem de chegar aos MESMOS números, não ao dobro.
	for i, cond := range []bool{true, true, false} {
		if err := c.Transition(types.RowID(i), payload(t, "A", 1, int64(i), cond), false); err != nil {
			t.Fatal(err)
		}
	}
	if stats := c.Stats(); stats.Rows != 3 || stats.Total != 2 {
		t.Fatalf("depois de reconstruir: linhas %d total %d, quer 3 e 2", stats.Rows, stats.Total)
	}
	requireCoherentAggregates(t, c)
}

func TestAbrirComRecriaComecaDoZero(t *testing.T) {
	dir := t.TempDir()
	def := defByOLT("aut", true, 100)
	c := openCounter(t, dir, def, false)
	if err := c.Transition(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.ApplyOverlay(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c2 := openCounter(t, dir, def, true)
	if !c2.Born() || c2.Limit() != 1 {
		t.Fatalf("recria tinha de nascer vazio: nasceu=%v limite=%d", c2.Born(), c2.Limit())
	}
	// Os arquivos antigos ficam de lado até o dono os apagar.
	if _, err := os.Stat(filepath.Join(dir, "grupos.dict.old")); err != nil {
		t.Fatalf("o dicionário antigo tinha de ser renomeado: %v", err)
	}
	if err := c2.RemoveOld(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "grupos.dict.old")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("RemoveOld tinha de remover o .old: %v", err)
	}
}

func TestCongelaCopiaOsTotais(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLT("aut", true, 100), false)
	if err := c.Transition(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	snap := c.AuditSnapshot()
	if snap.Limit != 2 || snap.Totals[1] != 1 || snap.Refs[1] != 1 {
		t.Fatalf("congelado = %v %v %d", snap.Totals, snap.Refs, snap.Limit)
	}
	// Os agregados viajam no MESMO retrato: é a coincidência de instante que
	// a auditoria compara.
	if snap.Rows != 1 || snap.Total != 1 {
		t.Fatalf("congelado: linhas %d total %d, quer 1 e 1", snap.Rows, snap.Total)
	}
	if err := c.Transition(0, nil, true); err != nil {
		t.Fatal(err)
	}
	if snap.Totals[1] != 1 || snap.Rows != 1 || snap.Total != 1 {
		t.Fatal("a cópia não pode acompanhar o writer")
	}
}

func TestCobreENasceu(t *testing.T) {
	dir := t.TempDir()
	def := defByOLT("aut", true, 100)
	c := openCounter(t, dir, def, false)
	if !c.Born() || !c.Covers(1024) || c.Covers(1<<30) {
		t.Fatalf("nasceu=%v cobre(1024)=%v cobre(2^30)=%v", c.Born(), c.Covers(1024), c.Covers(1<<30))
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c2 := openCounter(t, dir, def, false)
	if c2.Born() {
		t.Fatal("reabertura não nasce")
	}
	if c2.Def().Name != "aut" || c2.Def().RetainEmptyGroups != time.Duration(0) {
		t.Fatalf("Def = %+v", c2.Def())
	}
}
