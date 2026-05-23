package main

import (
	"log/slog"
	"os"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/logger"
	"github.com/ManusaRivi/money-laundering-analysis/src/core"
)

func main() {
	logger.SetupLogger()
	configPath := os.Getenv("CONFIG_PATH")
	cfg, err := config.Load(configPath)
	if err != nil {
		slog.Error("Failed to load config: %v", "error", err)
	}

	if err := core.RunManager(cfg); err != nil {
		slog.Error("Error running manager: %v", "error", err)
	}
}
