package main

import (
	"log/slog"
	"os"

	"github.com/ManusaRivi/money-laundering-analysis/src/client/config"
	client "github.com/ManusaRivi/money-laundering-analysis/src/client/internal"
)

func run() int {
	config, err := config.LoadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := client.NewClient(config)
	if err != nil {
		slog.Error("While connecting to server", "err", err)
		return 1
	}

	if err := server.Start(); err != nil {
		slog.Error("Client stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
