package contador

import (
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// FieldIDs do dataset de teste, em ordem crescente como o payload exige.
const (
	cOlt  types.FieldID = 1
	cSlot types.FieldID = 2
	cPort types.FieldID = 3
	cAuth types.FieldID = 4
)

// avaliadorDeTeste é o `where authenticated = true` sem passar pelo binder:
// o contador só conhece a interface, e o teste unitário não precisa de um
// schema compilado para exercitá-la.
type avaliadorDeTeste struct{ fr encoding.FieldReader }

func (a *avaliadorDeTeste) Avalia(payload []byte) (bool, error) {
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

func defPorOLT(nome string, comWhere bool, teto uint32) Definicao {
	d := Definicao{
		Nome:      nome,
		Colunas:   []types.FieldID{cOlt},
		Nomes:     []string{"olt"},
		Tipos:     []types.FieldType{types.TypeString},
		MaxGroups: teto,
	}
	if comWhere {
		d.Where = "authenticated = true"
		d.NovoAvaliador = func() Avaliador { return &avaliadorDeTeste{} }
	}
	return d
}

func defPorPON(nome string, teto uint32) Definicao {
	return Definicao{
		Nome:          nome,
		Colunas:       []types.FieldID{cOlt, cSlot, cPort},
		Nomes:         []string{"olt", "slot", "port"},
		Tipos:         []types.FieldType{types.TypeString, types.TypeInt64, types.TypeInt64},
		Where:         "authenticated = true",
		NovoAvaliador: func() Avaliador { return &avaliadorDeTeste{} },
		MaxGroups:     teto,
	}
}

// payload monta o estado de uma linha. olt vazio OMITE a coluna.
func payload(t *testing.T, olt string, slot, port int64, auth bool) []byte {
	t.Helper()
	var buf []byte
	var err error
	if olt != "" {
		if buf, err = encoding.AppendField(buf, cOlt, types.Value{Type: types.TypeString, Bytes: []byte(olt)}); err != nil {
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

func abreContador(t *testing.T, dir string, def Definicao, recria bool) *Contador {
	t.Helper()
	c, err := Abre(dir, def, 1024, recria)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// totalDe devolve total e refs do grupo cuja chave é a OLT dada.
func totalDe(t *testing.T, c *Contador, olt string) (total, refs int64) {
	t.Helper()
	chave, err := valuekey.AppendComposta(nil, []types.Value{{Type: types.TypeString, Bytes: []byte(olt)}})
	if err != nil {
		t.Fatal(err)
	}
	id, ok := c.dict.PorChave(chave)
	if !ok {
		return 0, 0
	}
	_, _, total, refs, _ = c.Grupo(id)
	return total, refs
}

func TestTransicaoNovaLinhaContaNoGrupo(t *testing.T) {
	c := abreContador(t, t.TempDir(), defPorOLT("aut", true, 100), false)
	if err := c.Transiciona(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.Transiciona(1, payload(t, "A", 1, 2, false), false); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalDe(t, c, "A"); total != 1 || refs != 2 {
		t.Fatalf("A: total %d refs %d, quer 1 e 2", total, refs)
	}
	if c.Transicoes() != 2 {
		t.Fatalf("transições = %d, quer 2", c.Transicoes())
	}
}

// TestReprocessarOMesmoEventoNaoMexe é a segunda linha da tabela da proposta
// §5.5: a reentrega é uma transição de X para X, e o contador não tem o que
// ajustar. Nenhuma contabilidade de "quais mensagens já contei" existe.
func TestReprocessarOMesmoEventoNaoMexe(t *testing.T) {
	c := abreContador(t, t.TempDir(), defPorOLT("aut", true, 100), false)
	p := payload(t, "A", 1, 1, true)
	for i := 0; i < 3; i++ {
		if err := c.Transiciona(7, p, false); err != nil {
			t.Fatal(err)
		}
	}
	if total, refs := totalDe(t, c, "A"); total != 1 || refs != 1 {
		t.Fatalf("A: total %d refs %d, quer 1 e 1", total, refs)
	}
	if c.Transicoes() != 1 {
		t.Fatalf("reprocessar não é transição: transições = %d, quer 1", c.Transicoes())
	}
}

func TestMudarDeGrupoAjustaAsDuasPontas(t *testing.T) {
	c := abreContador(t, t.TempDir(), defPorOLT("aut", true, 100), false)
	if err := c.Transiciona(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.Transiciona(0, payload(t, "B", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalDe(t, c, "A"); total != 0 || refs != 0 {
		t.Fatalf("A depois da migração: total %d refs %d, quer 0 e 0", total, refs)
	}
	if total, refs := totalDe(t, c, "B"); total != 1 || refs != 1 {
		t.Fatalf("B depois da migração: total %d refs %d, quer 1 e 1", total, refs)
	}
	// A OLT antiga NÃO some: o grupo sobrevive ao próprio total (proposta §5.4).
	if _, _, _, _, vivo := c.Grupo(1); !vivo {
		t.Fatal("o grupo que esvaziou continua vivo, com total 0")
	}
	// Só a condição mudando também é transição, e só mexe no total.
	if err := c.Transiciona(0, payload(t, "B", 1, 1, false), false); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalDe(t, c, "B"); total != 0 || refs != 1 {
		t.Fatalf("B com condição falsa: total %d refs %d, quer 0 e 1", total, refs)
	}
}

func TestLapideDescontaEPreservaOGrupo(t *testing.T) {
	c := abreContador(t, t.TempDir(), defPorOLT("aut", true, 100), false)
	if err := c.Transiciona(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	// A lápide chega com payload reduzido (só o instante de escrita) e a flag:
	// o contador não lê nada dela — a entrada antiga do vetor é a memória.
	if err := c.Transiciona(0, nil, true); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalDe(t, c, "A"); total != 0 || refs != 0 {
		t.Fatalf("A depois da lápide: total %d refs %d, quer 0 e 0", total, refs)
	}
	if _, _, total, _, vivo := c.Grupo(1); !vivo || total != 0 {
		t.Fatalf("o grupo permanece na tabela com total 0 (vivo=%v total=%d)", vivo, total)
	}
	// Lápide sobre lápide (replay) não mexe.
	if err := c.Transiciona(0, nil, true); err != nil {
		t.Fatal(err)
	}
	if c.Transicoes() != 2 {
		t.Fatalf("transições = %d, quer 2", c.Transicoes())
	}
}

func TestRessurreicaoVoltaAContar(t *testing.T) {
	c := abreContador(t, t.TempDir(), defPorOLT("aut", true, 100), false)
	if err := c.Transiciona(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.Transiciona(0, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := c.Transiciona(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalDe(t, c, "A"); total != 1 || refs != 1 {
		t.Fatalf("A ressuscitada: total %d refs %d, quer 1 e 1", total, refs)
	}
}

// TestLinhaSemColunaDeAgrupamentoNaoConta: entidade sem a coluna (ausente ou
// nula) recebe o grupo reservado e NÃO é contada (proposta §6.3).
func TestLinhaSemColunaDeAgrupamentoNaoConta(t *testing.T) {
	c := abreContador(t, t.TempDir(), defPorOLT("aut", true, 100), false)
	if err := c.Transiciona(0, payload(t, "", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if c.Limite() != 1 {
		t.Fatalf("nenhum grupo devia ter sido internado, limite = %d", c.Limite())
	}
	if c.Transicoes() != 0 {
		t.Fatal("sem grupo antes e depois não é transição")
	}
	// A linha ganha a coluna: entra no grupo.
	if err := c.Transiciona(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalDe(t, c, "A"); total != 1 || refs != 1 {
		t.Fatalf("A: total %d refs %d", total, refs)
	}
	// E perde de novo: sai.
	if err := c.Transiciona(0, payload(t, "", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalDe(t, c, "A"); total != 0 || refs != 0 {
		t.Fatalf("A depois de perder a coluna: total %d refs %d", total, refs)
	}
}

func TestSemWhereContaTodaLinhaComGrupo(t *testing.T) {
	c := abreContador(t, t.TempDir(), defPorOLT("serv", false, 100), false)
	if err := c.Transiciona(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.Transiciona(1, payload(t, "A", 1, 1, false), false); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalDe(t, c, "A"); total != 2 || refs != 2 {
		t.Fatalf("sem where: total %d refs %d, quer 2 e 2", total, refs)
	}
}

func TestTetoDeGruposRecusaEConta(t *testing.T) {
	c := abreContador(t, t.TempDir(), defPorOLT("aut", true, 1), false)
	if err := c.Transiciona(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	// A segunda OLT não cabe: a linha entra como "sem grupo", e a recusa conta.
	if err := c.Transiciona(1, payload(t, "B", 1, 1, true), false); err != nil {
		t.Fatalf("o teto não pode virar erro de escrita: %v", err)
	}
	est := c.Estatisticas()
	if est.Recusados != 1 || est.Grupos != 1 {
		t.Fatalf("estatísticas = %+v, quer Recusados 1 e Grupos 1", est)
	}
	if total, refs := totalDe(t, c, "B"); total != 0 || refs != 0 {
		t.Fatalf("B não pode ter sido contada: %d %d", total, refs)
	}
	// A linha recusada, ao ser reescrita numa OLT que cabe, entra normalmente.
	if err := c.Transiciona(1, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalDe(t, c, "A"); total != 2 || refs != 2 {
		t.Fatalf("A: total %d refs %d, quer 2 e 2", total, refs)
	}
}

// TestDobraReproduzOsTotais é o teste que sustenta a consistência através de
// crash: os totais NÃO são persistidos, são dobra do vetor na abertura. Uma
// segunda instância aberta sobre os mesmos arquivos tem de chegar exatamente
// aos mesmos números.
func TestDobraReproduzOsTotais(t *testing.T) {
	dir := t.TempDir()
	def := defPorPON("pon", 1000)
	c := abreContador(t, dir, def, false)
	rnd := rand.New(rand.NewSource(7))
	olts := []string{"A", "B", "C", "D"}
	for i := 0; i < 1000; i++ {
		row := types.RowID(rnd.Intn(200))
		if rnd.Intn(10) == 0 {
			if err := c.Transiciona(row, nil, true); err != nil {
				t.Fatal(err)
			}
			continue
		}
		p := payload(t, olts[rnd.Intn(len(olts))], int64(rnd.Intn(3)), int64(rnd.Intn(4)), rnd.Intn(2) == 1)
		if err := c.Transiciona(row, p, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.ApplyOverlay(); err != nil {
		t.Fatal(err)
	}
	if err := c.Msync(); err != nil {
		t.Fatal(err)
	}
	quero, refsQ, limite := c.Congela()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	c2 := abreContador(t, dir, def, false)
	if err := c2.Dobra(); err != nil {
		t.Fatal(err)
	}
	if c2.Limite() != limite {
		t.Fatalf("limite %d, quer %d", c2.Limite(), limite)
	}
	for id := uint32(1); id < limite; id++ {
		_, _, total, refs, _ := c2.Grupo(id)
		if total != quero[id] || refs != refsQ[id] {
			t.Fatalf("grupo %d: dobra deu total %d refs %d, quer %d e %d", id, total, refs, quero[id], refsQ[id])
		}
	}
	if c2.Estatisticas().Orfas != 0 {
		t.Fatal("nenhuma entrada órfã era esperada")
	}
}

// TestOverlayNaoVaiAoMmapAntesDoApply: o vetor em disco só muda no
// ApplyOverlay, que o checkpoint chama DEPOIS do segSync. É o que impede a
// página do vetor de ficar à frente do log durável numa cauda rasgada.
func TestOverlayNaoVaiAoMmapAntesDoApply(t *testing.T) {
	dir := t.TempDir()
	def := defPorOLT("aut", true, 100)
	c := abreContador(t, dir, def, false)
	for r := types.RowID(0); r < 10; r++ {
		if err := c.Transiciona(r, payload(t, "A", 1, 1, true), false); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Msync(); err != nil {
		t.Fatal(err)
	}
	// Segunda instância sobre os mesmos arquivos, SEM apply: dobra zero.
	// (O dicionário já tem a OLT A — ele é fsyncado por entrada.)
	espia := abreContador(t, dir, def, false)
	if err := espia.Dobra(); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalDe(t, espia, "A"); total != 0 || refs != 0 {
		t.Fatalf("antes do ApplyOverlay o mmap tem de estar zerado: total %d refs %d", total, refs)
	}
	if err := espia.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.ApplyOverlay(); err != nil {
		t.Fatal(err)
	}
	if err := c.Msync(); err != nil {
		t.Fatal(err)
	}
	espia2 := abreContador(t, dir, def, false)
	if err := espia2.Dobra(); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalDe(t, espia2, "A"); total != 10 || refs != 10 {
		t.Fatalf("depois do ApplyOverlay: total %d refs %d, quer 10 e 10", total, refs)
	}
	// E a instância viva continua lendo o próprio overlay depois do apply.
	if err := c.Transiciona(0, payload(t, "A", 1, 1, false), false); err != nil {
		t.Fatal(err)
	}
	if total, _ := totalDe(t, c, "A"); total != 9 {
		t.Fatalf("total depois do apply e de uma transição = %d, quer 9", total)
	}
}

// TestDobraTrataIdAposentadoComoOrfa: uma entrada em disco apontando para um
// grupo aposentado (ou inexistente) não pode contar em lugar nenhum. Ela é
// reescrita para SemGrupo — o Open não tem leitores — e contada em Orfas.
func TestDobraTrataIdAposentadoComoOrfa(t *testing.T) {
	dir := t.TempDir()
	def := defPorOLT("aut", true, 100)
	c := abreContador(t, dir, def, false)
	if err := c.Transiciona(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.Transiciona(1, payload(t, "B", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.ApplyOverlay(); err != nil {
		t.Fatal(err)
	}
	// Aposenta A por baixo (o dicionário é de teste aqui) e grava uma entrada
	// apontando para um id que nunca existiu.
	if err := c.dict.Aposenta(1); err != nil {
		t.Fatal(err)
	}
	if err := c.vetor.Set(2, 99); err != nil {
		t.Fatal(err)
	}
	if err := c.Msync(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c2 := abreContador(t, dir, def, false)
	if err := c2.Dobra(); err != nil {
		t.Fatal(err)
	}
	if est := c2.Estatisticas(); est.Orfas != 2 {
		t.Fatalf("órfãs = %d, quer 2 (uma aposentada, uma inexistente)", est.Orfas)
	}
	if _, _, total, refs, vivo := c2.Grupo(1); vivo || total != 0 || refs != 0 {
		t.Fatalf("grupo aposentado não pode contar: vivo=%v total=%d refs=%d", vivo, total, refs)
	}
	if total, refs := totalDe(t, c2, "B"); total != 1 || refs != 1 {
		t.Fatalf("B: %d %d", total, refs)
	}
	if v, _ := c2.vetor.Get(0); v != SemGrupo {
		t.Fatalf("a entrada órfã tinha de ser reescrita para SemGrupo, é %#x", v)
	}
	if v, _ := c2.vetor.Get(2); v != SemGrupo {
		t.Fatalf("a entrada inexistente tinha de ser reescrita para SemGrupo, é %#x", v)
	}
}

func TestCobreMexidasAcompanhaAsTransicoes(t *testing.T) {
	c := abreContador(t, t.TempDir(), defPorOLT("aut", true, 100), false)
	if err := c.Transiciona(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.Transiciona(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if c.Transicoes() != 1 {
		t.Fatalf("transições = %d, quer 1 (reprocessar não conta)", c.Transicoes())
	}
	c.CobreMexidas(1)
	c.CobreMexidas(0) // nunca regride
	if c.MexidasCobertas() != 1 {
		t.Fatalf("cobertas = %d, quer 1", c.MexidasCobertas())
	}
}

func TestGrupoCompostoTemColunasTipadas(t *testing.T) {
	c := abreContador(t, t.TempDir(), defPorPON("pon", 100), false)
	if err := c.Transiciona(0, payload(t, "PRCTA_G1M10", 1, 9, true), false); err != nil {
		t.Fatal(err)
	}
	chave, texto, total, refs, vivo := c.Grupo(1)
	if !vivo || total != 1 || refs != 1 {
		t.Fatalf("grupo 1: vivo=%v total=%d refs=%d", vivo, total, refs)
	}
	vals, err := valuekey.DecodeComposta(chave, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 3 || string(vals[0].Bytes) != "PRCTA_G1M10" || vals[1].I64 != 1 || vals[2].I64 != 9 {
		t.Fatalf("componentes = %+v", vals)
	}
	if string(texto) != "PRCTA_G1M10|1|9" {
		t.Fatalf("texto = %q", texto)
	}
	if id, ok := c.PorTexto([]byte("PRCTA_G1M10|1|9")); !ok || id != 1 {
		t.Fatalf("PorTexto = %d, %v", id, ok)
	}
}

func TestZeraLimpaTotaisEVetorMasMantemODicionario(t *testing.T) {
	c := abreContador(t, t.TempDir(), defPorOLT("aut", true, 100), false)
	if err := c.Transiciona(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.Zera(); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalDe(t, c, "A"); total != 0 || refs != 0 {
		t.Fatalf("depois do Zera: %d %d", total, refs)
	}
	if c.Limite() != 2 {
		t.Fatal("o dicionário fica: os ids são estáveis")
	}
	// Reconstruir é transicionar de novo, do zero.
	if err := c.Transiciona(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalDe(t, c, "A"); total != 1 || refs != 1 {
		t.Fatalf("depois de reconstruir: %d %d", total, refs)
	}
}

func TestAbrirComRecriaComecaDoZero(t *testing.T) {
	dir := t.TempDir()
	def := defPorOLT("aut", true, 100)
	c := abreContador(t, dir, def, false)
	if err := c.Transiciona(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.ApplyOverlay(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c2 := abreContador(t, dir, def, true)
	if !c2.Nasceu() || c2.Limite() != 1 {
		t.Fatalf("recria tinha de nascer vazio: nasceu=%v limite=%d", c2.Nasceu(), c2.Limite())
	}
	// Os arquivos antigos ficam de lado até o dono os apagar.
	if _, err := os.Stat(filepath.Join(dir, "grupos.dict.old")); err != nil {
		t.Fatalf("o dicionário antigo tinha de ser renomeado: %v", err)
	}
	if err := c2.ApagaAntigos(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "grupos.dict.old")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ApagaAntigos tinha de remover o .old: %v", err)
	}
}

func TestCongelaCopiaOsTotais(t *testing.T) {
	c := abreContador(t, t.TempDir(), defPorOLT("aut", true, 100), false)
	if err := c.Transiciona(0, payload(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	totais, refs, limite := c.Congela()
	if limite != 2 || totais[1] != 1 || refs[1] != 1 {
		t.Fatalf("congelado = %v %v %d", totais, refs, limite)
	}
	if err := c.Transiciona(0, nil, true); err != nil {
		t.Fatal(err)
	}
	if totais[1] != 1 {
		t.Fatal("a cópia não pode acompanhar o writer")
	}
}

func TestCobreENasceu(t *testing.T) {
	dir := t.TempDir()
	def := defPorOLT("aut", true, 100)
	c := abreContador(t, dir, def, false)
	if !c.Nasceu() || !c.Cobre(1024) || c.Cobre(1<<30) {
		t.Fatalf("nasceu=%v cobre(1024)=%v cobre(2^30)=%v", c.Nasceu(), c.Cobre(1024), c.Cobre(1<<30))
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c2 := abreContador(t, dir, def, false)
	if c2.Nasceu() {
		t.Fatal("reabertura não nasce")
	}
	if c2.Def().Nome != "aut" || c2.Def().RetainEmptyGroups != time.Duration(0) {
		t.Fatalf("Def = %+v", c2.Def())
	}
}
