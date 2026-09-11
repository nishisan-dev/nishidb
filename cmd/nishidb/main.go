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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	// `parar` é chamado ANTES do os.Exit, e não por defer: o os.Exit não
	// desempilha nada, então um `defer parar()` aqui é linha que jamais roda.
	// O efeito prático é nulo — o processo está morrendo e o SO recolhe o
	// handler —, mas defer que não executa é o tipo de linha que a próxima
	// leitura toma por garantia de limpeza que não existe.
	code := cli.Run(os.Args[1:], cli.Context{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
		Ctx:    ctx,
	})
	stop()
	os.Exit(code)
}
