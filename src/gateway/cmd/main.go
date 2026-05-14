package main

import (
	"log/slog"
	"os"

	"github.com/ManusaRivi/money-laundering-analysis/src/gateway/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/gateway/internal/gateway"
)

func run() int {
	config, err := config.LoadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		os.Exit(1)
	}

	server, err := gateway.NewGateway(config)
	if err != nil {
		slog.Error("While creating gateway", "err", err)
		os.Exit(1)
	}

	if err := server.Run(); err != nil {
		slog.Error("Gateway stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
