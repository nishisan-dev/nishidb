package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/protocol"
)

// TestDesfechoDaAuditoriaDivergenteSaiComExitDataset: divergência é corrupção
// de índice derivado, e o exit code é o que um script de operação lê. O
// caminho de ingestão nunca produz deriva por conta própria, então o desfecho
// é testado sobre o relatório — o mesmo que os dois caminhos montam.
func TestDesfechoDaAuditoriaDivergenteSaiComExitDataset(t *testing.T) {
	var stderr bytes.Buffer
	amb := Context{Stderr: &stderr, Ctx: context.Background()}
	limpo := []rowOfAudit{{name: "a"}, {name: "b"}}
	if code := auditOutcome(limpo, ExitOK, amb); code != ExitOK {
		t.Fatalf("sem divergência: %d", code)
	}
	divergente := []rowOfAudit{{name: "a"}, {name: "b", divergentes: 2}}
	if code := auditOutcome(divergente, ExitOK, amb); code != ExitDataset {
		t.Fatalf("com divergência: %d, quer %d", code, ExitDataset)
	}
	if !strings.Contains(stderr.String(), "1 contador(es) divergem") {
		t.Fatalf("stderr: %s", stderr.String())
	}
	withoutGrupo := []rowOfAudit{{name: "a", naoInternadas: 3}}
	if code := auditOutcome(withoutGrupo, ExitOK, amb); code != ExitDataset {
		t.Fatalf("linha sem grupo internado é divergência: %d", code)
	}
	// Um erro de escrita da tabela vence: quem não conseguiu mostrar o
	// relatório não pode afirmar que ele estava limpo.
	if code := auditOutcome(limpo, ExitUsageError, amb); code != ExitUsageError {
		t.Fatalf("erro de saída tinha de prevalecer: %d", code)
	}
}

func TestAuditoriaDoWireEDoStoragePreenchemAMesmaLinha(t *testing.T) {
	w := auditOfWire(protocol.CounterAudit{
		Name: "x", Groups: 2, Maintained: 5, Recomputed: 4, DivergentGroups: 1, NotInterned: 0, RowsScanned: 9, DurationMs: 7,
		Sample: []protocol.GroupDivergence{{GroupKey: "A", Maintained: 5, Recomputed: 4, RefsMaintained: 5, RefsRecomputed: 4}},
	})
	if w.name != "x" || w.groups != 2 || w.mantido != 5 || w.recalculado != 4 || w.divergentes != 1 ||
		w.rowsVarridas != 9 || w.durationMs != 7 || len(w.amostra) != 1 || w.amostra[0].groupKey != "A" {
		t.Fatalf("linha do wire = %+v", w)
	}
	var buf bytes.Buffer
	writeAudit(&buf, "d", []rowOfAudit{w})
	for _, trecho := range []string{"DIVERGÊNCIA", "divergências de x", "REFS RECALCULADO", "  A"} {
		if !strings.Contains(buf.String(), trecho) {
			t.Fatalf("tabela sem %q:\n%s", trecho, buf.String())
		}
	}
}
