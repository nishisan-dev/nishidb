// Command nishidb é o binário do engine: materializa eventos e consulta o
// resultado. A lógica dos comandos mora em internal/cli, para ser testável
// como função — o teste fim-a-fim não deveria precisar de um binário
// compilado.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/nishisan-dev/nishidb/internal/cli"
)

func main() {
	// O sinal CANCELA o contexto em vez de matar o processo: a ingestão
	// precisa gravar o checkpoint do que já aplicou antes de sair, senão o
	// Ctrl-C joga fora o trabalho desde o último checkpoint.
	ctx, parar := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer parar()

	os.Exit(cli.Run(os.Args[1:], cli.Ambiente{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
		Ctx:    ctx,
	}))
}
