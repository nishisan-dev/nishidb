package cli

import (
	"flag"
	"fmt"
	"log/slog"

	nishiconfig "github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/server"
)

// runServer sobe o processo de longa duração do §96: consultas e ingestão
// Kafka no MESMO processo.
//
// O motivo de serem o mesmo processo é o flock: ele é exclusivo por processo,
// então um consumer separado tornaria o dataset inconsultável enquanto
// rodasse. Ver o godoc do internal/server.
func runServer(args []string, amb Context) int {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	fs.SetOutput(amb.Stderr)
	var (
		argConfig    = fs.String("config", "", "caminho do nishidb.yaml")
		argLogFormat = fs.String("log-format", "json", "json|texto")
		argLogLevel  = fs.String("log-level", "info", "debug|info|warn|error")
		pprofAddr    = fs.String("pprof", "", "endereço dos endpoints de debug (§112); vazio desliga")
	)
	if err := fs.Parse(args); err != nil {
		return ExitUsageError
	}
	if *argConfig == "" {
		fmt.Fprintln(amb.Stderr, "server: --config é obrigatório")
		return ExitUsageError
	}

	formato, err := logFormat(*argLogFormat)
	if err != nil {
		fmt.Fprintf(amb.Stderr, "server: %v\n", err)
		return ExitUsageError
	}
	nivel, err := logLevel(*argLogLevel)
	if err != nil {
		fmt.Fprintf(amb.Stderr, "server: %v\n", err)
		return ExitUsageError
	}
	log := logging.New(amb.Stderr, formato, nivel)

	cfg, err := nishiconfig.Load(*argConfig)
	if err != nil {
		fmt.Fprintf(amb.Stderr, "server: %v\n", err)
		return ExitUsageError
	}

	srv, err := server.Novo(cfg, server.Options{Log: log})
	if err != nil {
		fmt.Fprintf(amb.Stderr, "server: %v\n", err)
		return errorCode(err)
	}

	// Roda bloqueia até o contexto ser cancelado pelo sinal. O encerramento é
	// gracioso: drena a ingestão, checkpointa e devolve o flock.
	if err := srv.Run(amb.Ctx, *pprofAddr); err != nil {
		fmt.Fprintf(amb.Stderr, "server: %v\n", err)
		return errorCode(err)
	}

	// Um dataset com ingestão parada não derruba o processo (§51), mas o
	// operador precisa saber disso pelo $? de um `systemctl stop` seguido de
	// inspeção — senão o único sinal seria o /v1/health, que ninguém lê depois
	// que o processo já morreu.
	if pausas := srv.Pausas(); len(pausas) > 0 {
		for name, motivo := range pausas {
			fmt.Fprintf(amb.Stderr, "server: dataset %q com ingestão parada: %v\n", name, motivo)
		}
		return ExitPolicyStopped
	}
	return ExitOK
}

func logFormat(v string) (logging.Format, error) {
	switch v {
	case "json":
		return logging.FormatJSON, nil
	case "texto":
		return logging.FormatText, nil
	}
	return 0, fmt.Errorf("--log-format %q inválido (json|texto)", v)
}

func logLevel(v string) (slog.Level, error) {
	switch v {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return 0, fmt.Errorf("--log-level %q inválido (debug|info|warn|error)", v)
}
