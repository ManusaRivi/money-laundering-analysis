package core

import (
	"fmt"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/workers/aggregator"
	"github.com/ManusaRivi/money-laundering-analysis/src/workers/cleaner"
	"github.com/ManusaRivi/money-laundering-analysis/src/workers/converter"
	"github.com/ManusaRivi/money-laundering-analysis/src/workers/filter"
	"github.com/ManusaRivi/money-laundering-analysis/src/workers/join"
	"github.com/ManusaRivi/money-laundering-analysis/src/workers/router"
)

const (
	WorkerTypeFilter          = "SyncFilter"
	WorkerTypeQ5Filter        = "Q5Filter"
	WorkerTypeCleaner         = "Cleaner"
	WorkerTypeJoin            = "Join"
	WorkerTypeRouter          = "Router"
	WorkerTypeAggregator      = "Aggregator"
	WorkerTypeConverter       = "Converter"
	WorkerTypeDateRangeFilter = "DateRangeFilter"
	WorkerTypeAvgFormatFilter = "AvgFormatFilter"
)

// TODO: Define worker types as constants
func workerFactory(cfg *config.Config, communicationBroker broker.Broker) (Worker, error) {
	workerCfg := cfg.Worker
	switch workerCfg.Type {
	case WorkerTypeFilter:
		worker, err := filter.NewSyncFilter(workerCfg, communicationBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create SyncFilter: %w", err)
		}
		return worker, nil
	case WorkerTypeQ5Filter:
		worker, err := filter.NewQ5Filter(workerCfg, communicationBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create Q5Filter: %w", err)
		}
		return worker, nil
	case WorkerTypeDateRangeFilter:
		worker, err := filter.NewDateRange(workerCfg, communicationBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create DateRangeFilter: %w", err)
		}
		return worker, nil
	case WorkerTypeCleaner:
		worker := cleaner.NewCleaner(workerCfg, communicationBroker)
		return worker, nil
	case WorkerTypeJoin:
		worker, err := join.NewJoin(workerCfg, communicationBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create Join: %w", err)
		}
		return worker, nil
	case WorkerTypeAvgFormatFilter:
		if cfg.AvgBroker == nil {
			return nil, fmt.Errorf("avg_broker config is required for AvgFormatFilter")
		}
		avgBroker, err := broker.NewBroker(*cfg.AvgBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create avg broker: %w", err)
		}
		worker, err := filter.NewAvgFormatFilter(workerCfg, communicationBroker, avgBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create AvgFormatFilter: %w", err)
		}
		return worker, nil
	case WorkerTypeRouter:
		worker, err := router.NewRouter(workerCfg, communicationBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create Router: %w", err)
		}
		return worker, nil
	case WorkerTypeAggregator:
		worker, err := aggregator.NewAggregator(workerCfg, communicationBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create Aggregator: %w", err)
		}
		return worker, nil
	case WorkerTypeConverter:
		worker := converter.NewConverter(workerCfg, communicationBroker)
		return worker, nil
	default:
		return nil, fmt.Errorf("unknown worker type: %s", workerCfg.Type)
	}
}
