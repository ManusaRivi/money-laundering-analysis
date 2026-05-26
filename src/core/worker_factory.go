package core

import (
	"fmt"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/workers/cleaner"
	"github.com/ManusaRivi/money-laundering-analysis/src/workers/filter"
)

// TODO: Define worker types as constants
func workerFactory(cfg config.WorkerConfig, communicationBroker broker.Broker) (Worker, error) {
	switch cfg.Type {
	case "SyncFilter":
		worker, err := filter.NewSyncFilter(cfg, communicationBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create SyncFilter: %w", err)
		}
		return worker, nil
	case "Cleaner":
		worker := cleaner.NewCleaner(cfg, communicationBroker)
		return worker, nil
	default:
		return nil, fmt.Errorf("unknown worker type: %s", cfg.Type)
	}
}
