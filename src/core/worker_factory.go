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
func workerFactory(cfg *config.Config, communicationBroker broker.Broker) (Worker, error) {
	switch cfg.Worker.Type {
	case "SyncFilter":
		worker, err := filter.NewSyncFilter(cfg.Worker, communicationBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create SyncFilter: %w", err)
		}
		return worker, nil
	case "DateRangeFilter":
		worker, err := filter.NewDateRange(cfg.Worker, communicationBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create DateRangeFilter: %w", err)
		}
		return worker, nil
	case "AvgFormatFilter":
		if cfg.AvgBroker == nil {
			return nil, fmt.Errorf("avg_broker config is required for AvgFormatFilter")
		}
		avgBroker, err := broker.NewBroker(*cfg.AvgBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create avg broker: %w", err)
		}
		worker, err := filter.NewAvgFormatFilter(cfg.Worker, communicationBroker, avgBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create AvgFormatFilter: %w", err)
		}
		return worker, nil
	case "Cleaner":
		worker := cleaner.NewCleaner(cfg.Worker, communicationBroker)
		return worker, nil
	case "Join":
		worker, err := join.NewJoin(cfg.Worker, communicationBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create Join: %w", err)
		}
		return worker, nil
	case "Router":
		worker, err := router.NewRouter(cfg.Worker, communicationBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create Router: %w", err)
		}
		return worker, nil
	case "Aggregator":
		worker, err := aggregator.NewAggregator(cfg.Worker, communicationBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create Aggregator: %w", err)
		}
		return worker, nil
	default:
		return nil, fmt.Errorf("unknown worker type: %s", cfg.Worker.Type)
	}
}
