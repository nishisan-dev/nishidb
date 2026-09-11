package main

import (
	"fmt"
	"io"
)

// ImprimeRelatorio escreve o relatório final em texto legível por operador:
// totais, buckets, percentis exatos e a cauda dos N maiores — exatamente o
// que o brief da issue #15 pede para decidir entre elevar MaxKeySize, derivar
// a chave por hash ou aceitar.
//
// Os erros de escrita são ignorados de propósito (`_, _ =`): é saída de
// operador para stdout/stderr, no mesmo padrão de internal/cli/cli.go:147 —
// não há política de retry nem exit code que caiba aqui para um pipe quebrado
// no meio de um relatório.
func ImprimeRelatorio(w io.Writer, field string, h *Histograma, n int) {
	_, _ = fmt.Fprintf(w, "campo medido:     %s\n", field)
	_, _ = fmt.Fprintf(w, "total lido:       %d\n", h.TotalLido())
	_, _ = fmt.Fprintf(w, "com o campo:      %d\n", h.WithField())
	_, _ = fmt.Fprintf(w, "sem_campo:        %d\n", h.WithoutField())
	_, _ = fmt.Fprintf(w, "invalido:         %d\n", h.Invalidos())

	_, _ = fmt.Fprintln(w, "\nbuckets de tamanho (% sobre o total com o campo):")
	for _, b := range h.Buckets() {
		_, _ = fmt.Fprintf(w, "  %-6s %12d  (%5.1f%%)\n", b.Rotulo, b.Count, b.Percentual)
	}

	_, _ = fmt.Fprintln(w, "\npercentis exatos (bytes):")
	_, _ = fmt.Fprintf(w, "  p50 %d   p95 %d   p99 %d   máximo %d\n",
		h.Percentil(50), h.Percentil(95), h.Percentil(99), h.Maximo())

	maiores := h.TopN(n)
	_, _ = fmt.Fprintf(w, "\n%d maiores (bytes, um por mensagem):\n", len(maiores))
	for i, c := range maiores {
		_, _ = fmt.Fprintf(w, "  %2d. %d\n", i+1, c)
	}
}
