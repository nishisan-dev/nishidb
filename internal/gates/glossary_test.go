// Package gates — este arquivo confere o glossário PT-BR ↔ inglês.
package gates_test

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// O glossário é normativo (spec de identificadores em inglês, §1): comentário
// novo cita o termo PT-BR dele, identificador novo usa o termo inglês dele.
// Um glossário com célula vazia ou com o mesmo termo em duas linhas devolve
// duas respostas para a mesma pergunta, e aí cada agente escolhe a sua — que
// é o que este gate existe para impedir. Ele lê a tabela como dado, linha a
// linha, e não por grep em prosa.
func TestGlossaryHasBothColumnsAndNoDuplicateTerm(t *testing.T) {
	path := filepath.Join(raizDoRepo, "docs", "glossario.md")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("o glossário não existe: %v", err)
	}
	defer f.Close()

	seen := map[string]int{}
	lineNum, rowCount := 0, 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		lineNum++
		l := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(l, "|") || strings.HasPrefix(l, "|---") || strings.HasPrefix(l, "| PT-BR") {
			continue
		}
		cells := strings.Split(strings.Trim(l, "|"), "|")
		if len(cells) < 2 {
			t.Errorf("linha %d: tabela com menos de duas colunas: %q", lineNum, l)
			continue
		}
		termPT := strings.TrimSpace(cells[0])
		termEN := strings.TrimSpace(cells[1])
		if termPT == "" || termEN == "" {
			t.Errorf("linha %d: célula vazia: %q", lineNum, l)
		}
		if firstAt, ok := seen[termPT]; ok {
			t.Errorf("linha %d: termo %q repetido (já na linha %d)", lineNum, termPT, firstAt)
		}
		seen[termPT] = lineNum
		rowCount++
	}
	if rowCount < 40 {
		t.Fatalf("o glossário tem %d termos; o spec semeia mais de 40 — ou a tabela não foi lida, ou foi esvaziada", rowCount)
	}
}
