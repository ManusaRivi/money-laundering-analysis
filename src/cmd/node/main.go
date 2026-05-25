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
		return
	}

	manager, err := core.NewManager(cfg)
	if err != nil {
		slog.Error("Failed to create manager: %v", "error", err)
		return
	}

	if err := manager.Run(); err != nil {
		slog.Error("Error running manager: %v", "error", err)
	}
}
