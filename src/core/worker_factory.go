package core

import (
	"fmt"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/workers/aggregator"
	"github.com/ManusaRivi/money-laundering-analysis/src/workers/cleaner"
	"github.com/ManusaRivi/money-laundering-analysis/src/workers/filter"
	"github.com/ManusaRivi/money-laundering-analysis/src/workers/join"
	"github.com/ManusaRivi/money-laundering-analysis/src/workers/router"
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
	case "DateRangeFilter":
		worker, err := filter.NewDateRange(cfg, communicationBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create DateRangeFilter: %w", err)
		}
		return worker, nil
	case "Cleaner":
		worker := cleaner.NewCleaner(cfg, communicationBroker)
		return worker, nil
	case "Join":
		worker, err := join.NewJoin(cfg, communicationBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create Join: %w", err)
		}
		return worker, nil
	case "Router":
		worker, err := router.NewRouter(cfg, communicationBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create Router: %w", err)
		}
		return worker, nil
	case "Spliter":
		worker, err := router.NewSpliter(cfg, communicationBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create Spliter: %w", err)
		}
		return worker, nil
	case "Aggregator":
		worker, err := aggregator.NewAggregator(cfg, communicationBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create Aggregator: %w", err)
		}
		return worker, nil
	case "ScatterAndGather":
		worker, err := aggregator.NewScatterAndGather(cfg, communicationBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create ScatterAndGather: %w", err)
		}
		return worker, nil
	default:
		return nil, fmt.Errorf("unknown worker type: %s", cfg.Type)
	}
}
