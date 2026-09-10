package storage

// O resíduo IRMÃO do de liveness: o mesmo ramo do replayRecord que deixa
// LiveRecords acima do real deixa também uma POSTING órfã.
//
// A origem mudou de lugar quando o motor mudou, e o resíduo continuou. No motor
// global ele nascia de um diff cego: prev desconhecido (location durável já à
// frente do ponto S) fazia a fase de Set ligar as postings do payload novo
// enquanto a de Clear não tinha o que limpar. No índice por segmento não há fase
// de Clear em caminho nenhum (spec §5.2), então a posting do valor anterior fica
// onde está por DESENHO, e não por um ramo do replay — o que faz o write path e
// o replay convergirem para o mesmo estado, que é o que este teste passou a
// afirmar.
//
// Este arquivo existe pela mesma razão do store_liveness_residue_test.go: o
// fato precisa estar em TESTE, e não só em prosa, porque quem vai fechar a
// conta é a compaction/rebuild do M4 e precisa encontrar a medida.
//
// Direção da assimetria (é o que separa este resíduo de um bug de corretude):
// a órfã é um FALSO POSITIVO. O índice devolve um superset de candidatos e o
// executor revalida o predicado contra o payload corrente (§44), então
// nenhuma query responde errado por causa dela. O custo é candidato a mais
// varrido — desperdício, não resultado incorreto. Falso NEGATIVO seria o
// contrário, e é o que exigeBitmapCompleto guarda em todos os cenários de
// recovery.

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// postingsDeStatus devolve as rows presentes nas postings de cada valor de
// status pedido, na ordem em que os valores foram pedidos.
func postingsDeStatus(t *testing.T, s *Store, valores ...string) map[string][]types.RowID {
	t.Helper()
	out := make(map[string][]types.RowID, len(valores))
	for _, v := range valores {
		it, err := s.Lookup(fldStatus, strVal(v))
		if err != nil {
			t.Fatalf("Lookup(status=%q): %v", v, err)
		}
		out[v] = collectRows(t, it)
	}
	return out
}

// TestPostingOrfaDoReplayNaoConverge mede a posting órfã e prova que ela NÃO
// se corrige sozinha.
//
// O cenário é exatamente o crash que o §10.2 admite: o checkpoint msynca as
// locations (passo 3) antes de gravar o manifest (passo 4), então a location já
// aponta para o record do tail enquanto o ponto S recuperável ficou para trás.
// É a mesma montagem do TestReplayRestauraPostingComBitmapAtrasDasLocations,
// que mede a outra metade: lá, que a posting NOVA é restaurada; aqui, que a
// ANTIGA não é apagada.
func TestPostingOrfaDoReplayNaoConverge(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, IndexedFields: []types.FieldID{fldStatus}}

	// 1. Row criada com status=Start e CHECKPOINTADA: a partir daqui ela
	// existe no ponto S, e é o bit de Start que vai virar órfão.
	s := audOpen(t, opts)
	id, _ := mustPut(t, s, "bm", 1, statusPayload(t, s, "bm", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// 2. Update no tail (Start→Stop) e crash entre os passos 3 e 4: as
	// locations ficam duráveis apontando para o record NOVO, e o manifest fica
	// no ponto S. O índice não entra mais nessa encenação porque ele não é
	// durabilizado no checkpoint: toda abertura o reconstrói (spec §2.5).
	prev := mustGetAud(t, s, "bm")
	mustPut(t, s, "bm", 2, statusPayload(t, s, "bm", "Stop", 2), &prev)
	if err := s.PartialCheckpointForTest(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

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
	// A asserção que estava aqui — "a posting de Stop deveria ter sido limpa,
	// esse prev o replay conhecia" — media a fase de CLEAR, que o índice por
	// segmento não tem (spec §5.2). Sem Clear, Stop vira órfã pela MESMA porta
	// que Start, e não há mais "prev que o replay conhecia" a distinguir.
	exigeOrfaDeStatus(t, "após o terceiro update + checkpoint", final, id, "Start", "Idle")
	exigeOrfaDeStatus(t, "após o terceiro update + checkpoint", final, id, "Stop", "Idle")

	// 5. E o resíduo NÃO é falso negativo: o valor corrente está nas postings,
	// que é a única propriedade da qual o §44 depende.
	exigeBitmapCompleto(t, s2)

	// 6. A contraprova mudou de sentido, e a mudança é o ganho do desenho novo.
	//
	// Ela existia para provar que o resíduo era do REPLAY e não do write path —
	// uma ASSIMETRIA entre os dois caminhos, que é o que fazia o débito 45 ser
	// um débito. Com o índice por segmento não há Clear em caminho nenhum
	// (§5.2), então os dois convergem para o MESMO estado, e é essa
	// convergência que o teste passa a afirmar. Assimetria entre write path e
	// replay é que seria defeito: significaria que a recuperação não reproduz o
	// que a ingestão faz.
	//
	// E o resíduo deixou de ser eterno, que era a outra metade do débito: a
	// posting órfã mora no índice do segmento em que foi escrita e sai de disco
	// quando a compaction ou a retenção aposenta aquele segmento.
	semCrash := postingsSemCrash(t)
	for _, v := range []string{"Start", "Stop", "Idle"} {
		if len(semCrash[v]) != 1 || semCrash[v][0] != id {
			t.Fatalf("write path sem crash: postings de %s = %v, esperado [%d] — o write path e o "+
				"replay têm de convergir para o mesmo estado", v, semCrash[v], id)
		}
	}
	// A propriedade de produto, nos dois caminhos: só o valor CORRENTE
	// sobrevive à revalidação.
	if rows := lookupRevalidado(t, s2, fldStatus, strVal("Start")); len(rows) != 0 {
		t.Fatalf("pós-revalidação, status=Start deveria ser vazio: %v", rows)
	}
	if rows := lookupRevalidado(t, s2, fldStatus, strVal("Idle")); len(rows) != 1 || rows[0] != id {
		t.Fatalf("pós-revalidação, status=Idle deveria ser [%d]: %v", id, rows)
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
	s := audOpen(t, Options{Dir: dir, IndexedFields: []types.FieldID{fldStatus}})
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
