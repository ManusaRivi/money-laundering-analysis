package core

import (
	"fmt"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/workers/filter"
)

func workerFactory(cfg config.WorkerConfig, communicationBroker broker.Broker) (Worker, error) {
	switch cfg.Type {
	case "SyncAmountFilter":
		worker, err := filter.NewSyncAmountFilter(cfg.Params, communicationBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create SyncAmountFilter: %w", err)
		}
		return worker, nil

	default:
		return nil, fmt.Errorf("unknown worker type: %s", cfg.Type)
	}
}
