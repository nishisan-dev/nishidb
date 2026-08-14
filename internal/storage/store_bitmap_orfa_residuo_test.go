package storage

// O resíduo IRMÃO do de liveness: o mesmo ramo do replayRecord que deixa
// LiveRecords acima do real deixa também uma POSTING DE BITMAP órfã. Quando o
// prev é desconhecido (location durável já à frente do ponto S), o replay roda
// applyBitmapSets — que liga as postings do payload novo — e a fase de Clear
// não tem o que limpar, porque o prevPayload que ela recebe é nil. O bit do
// valor ANTIGO fica ligado para sempre.
//
// Este arquivo existe pela mesma razão do store_liveness_residuo_test.go: o
// fato precisa estar em TESTE, e não só em prosa, porque quem vai fechar a
// conta é a compaction/rebuild do M4 e precisa encontrar a medida. NÃO
// conserta o replay — isso é escopo do M4 —, apenas fixa o que ele produz
// hoje e a direção da assimetria.
//
// Direção da assimetria (é o que separa este resíduo de um bug de corretude):
// a órfã é um FALSO POSITIVO. O índice devolve um superset de candidatos e o
// executor revalida o predicado contra o payload corrente (§44), então
// nenhuma query responde errado por causa dela. O custo é candidato a mais
// varrido — desperdício, não resultado incorreto. Falso NEGATIVO seria o
// contrário, e é o que exigeBitmapCompleto guarda em todos os cenários de
// recovery.

import (
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// postingsDeStatus devolve as rows presentes nas postings de cada valor de
// status pedido, na ordem em que os valores foram pedidos.
func postingsDeStatus(t *testing.T, s *Store, valores ...string) map[string][]types.RowID {
	t.Helper()
	out := make(map[string][]types.RowID, len(valores))
	for _, v := range valores {
		it, err := s.LookupBitmap(fldStatus, strVal(v))
		if err != nil {
			t.Fatalf("LookupBitmap(status=%q): %v", v, err)
		}
		out[v] = collectRows(t, it)
	}
	return out
}

// TestPostingOrfaDoReplayNaoConverge mede a posting órfã e prova que ela NÃO
// se corrige sozinha.
//
// O cenário é exatamente o crash que o §10.2 admite: o checkpoint msynca as
// locations (passo 3) antes de gravar o manifest (passo 4), e as páginas dos
// bitmaps podem não ter chegado ao disco quando a energia cai — o que regride
// as postings ao estado do ponto S enquanto a location já aponta para o record
// do tail. É a mesma montagem do TestReplayRestauraPostingComBitmapAtrasDasLocations,
// que mede a outra metade: lá, que a posting NOVA é restaurada; aqui, que a
// ANTIGA não é apagada.
func TestPostingOrfaDoReplayNaoConverge(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, BitmapFields: []types.FieldID{fldStatus}}

	// 1. Row criada com status=Start e CHECKPOINTADA: a partir daqui ela
	// existe no ponto S, e é o bit de Start que vai virar órfão.
	s := audOpen(t, opts)
	id, _ := mustPut(t, s, "bm", 1, statusPayload(t, s, "bm", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// Estado das postings no ponto S, para regredir depois do crash.
	emS := filepath.Join(t.TempDir(), "bm-em-S")
	copiaDir(t, bitmapDir(dir, fldStatus), emS)

	// 2. Update no tail (Start→Stop) e crash entre os passos 3 e 4: as
	// locations ficam duráveis apontando para o record NOVO…
	prev := mustGetAud(t, s, "bm")
	mustPut(t, s, "bm", 2, statusPayload(t, s, "bm", "Stop", 2), &prev)
	if err := s.PartialCheckpointForTest(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// …e as páginas dos bitmaps, não.
	copiaDir(t, emS, bitmapDir(dir, fldStatus))

	// 3. Recovery: o replay reaplica o update sem conhecer o estado anterior.
	s2 := audOpen(t, opts)
	defer s2.Close()
	if rec := mustGetAud(t, s2, "bm"); rec.Sequence != 2 {
		t.Fatalf("o estado do tail deveria estar visível: %+v", rec)
	}
	depois := postingsDeStatus(t, s2, "Start", "Stop")
	t.Logf("após o replay: postings de Start=%v Stop=%v (payload corrente = status Stop)",
		depois["Start"], depois["Stop"])
	exigeOrfaDeStatus(t, "após o replay", depois, id, "Start", "Stop")

	// 4. Um TERCEIRO update com checkpoint completo — o "próximo checkpoint
	// que enxerga a row inteira". O que ele limpa é a posting do estado que a
	// location nomeia (Stop), nunca a de antes dela: Start continua ligada.
	prev2 := mustGetAud(t, s2, "bm")
	mustPut(t, s2, "bm", 3, statusPayload(t, s2, "bm", "Idle", 3), &prev2)
	if err := s2.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	final := postingsDeStatus(t, s2, "Start", "Stop", "Idle")
	t.Logf("após o terceiro update + checkpoint: postings de Start=%v Stop=%v Idle=%v",
		final["Start"], final["Stop"], final["Idle"])
	if len(final["Stop"]) != 0 {
		t.Fatalf("após o terceiro update + checkpoint: a posting de Stop deveria ter sido limpa "+
			"(esse prev o replay conhecia), obtido %v", final["Stop"])
	}
	exigeOrfaDeStatus(t, "após o terceiro update + checkpoint", final, id, "Start", "Idle")

	// 5. E o resíduo NÃO é falso negativo: o valor corrente está nas postings,
	// que é a única propriedade da qual o §44 depende.
	exigeBitmapCompleto(t, s2)

	// 6. Contraprova: a MESMA sequência de escritas sem crash nenhum fecha a
	// conta exata. O resíduo é do REPLAY, não do write path.
	semCrash := postingsSemCrash(t)
	for _, v := range []string{"Start", "Stop"} {
		if len(semCrash[v]) != 0 {
			t.Fatalf("write path sem crash deveria ter limpado a posting de %s, obtido %v", v, semCrash[v])
		}
	}
	if len(semCrash["Idle"]) != 1 || semCrash["Idle"][0] != id {
		t.Fatalf("write path sem crash deveria deixar só a posting de Idle com a row %d, obtido %v",
			id, semCrash["Idle"])
	}
}

// exigeOrfaDeStatus separa as duas afirmações que o M4 precisa encontrar:
//
//   - a INVARIANTE, que vale para sempre: o valor CORRENTE tem de estar nas
//     postings. Faltar aqui é falso negativo, e falso negativo é query
//     silenciosamente incompleta — nenhuma camada acima recupera um candidato
//     que o índice não devolveu;
//   - a MEDIDA de hoje: o excesso é EXATAMENTE a posting do valor que a row
//     tinha no ponto S, e ela permanece. Se ela sumir, o teste falha de
//     propósito, com a instrução do que fazer.
func exigeOrfaDeStatus(t *testing.T, momento string, postings map[string][]types.RowID, id types.RowID, orfao, corrente string) {
	t.Helper()
	if len(postings[corrente]) != 1 || postings[corrente][0] != id {
		t.Fatalf("%s: FALSO NEGATIVO — a row %d tem status=%s no payload corrente, mas as postings "+
			"desse valor são %v", momento, id, corrente, postings[corrente])
	}
	if len(postings[orfao]) != 1 || postings[orfao][0] != id {
		t.Fatalf("%s: as postings de %s são %v; o resíduo medido do replay é de exatamente uma "+
			"posting órfã, a row %d. Se ela deixou de existir, a correção do M4 aconteceu: "+
			"atualize esta expectativa, o comentário do replayRecord em store_recovery.go, o "+
			"§10.2 do planning/m1-design-storage.md e o débito 45 do docs/debitos.md",
			momento, orfao, postings[orfao], id)
	}
}

// postingsSemCrash roda a mesma sequência de escritas sem crash nenhum e
// devolve as postings finais de cada valor.
func postingsSemCrash(t *testing.T) map[string][]types.RowID {
	t.Helper()
	dir := t.TempDir()
	s := audOpen(t, Options{Dir: dir, BitmapFields: []types.FieldID{fldStatus}})
	defer s.Close()

	mustPut(t, s, "bm", 1, statusPayload(t, s, "bm", "Start", 1), nil)
	for i, status := range []string{"Stop", "Idle"} {
		prev := mustGetAud(t, s, "bm")
		mustPut(t, s, "bm", int64(i+2), statusPayload(t, s, "bm", status, int64(i+2)), &prev)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	return postingsDeStatus(t, s, "Start", "Stop", "Idle")
}
