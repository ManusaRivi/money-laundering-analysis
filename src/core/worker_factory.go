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
	"github.com/ManusaRivi/money-laundering-analysis/src/workers/monitor"
	"github.com/ManusaRivi/money-laundering-analysis/src/workers/router"
)

const (
	WorkerTypeFilter              = "SyncFilter"
	WorkerTypeQ5Filter            = "Q5Filter"
	WorkerTypeCleaner             = "Cleaner"
	WorkerTypeJoin                = "Join"
	WorkerTypeRouter              = "Router"
	WorkerTypeAggregator          = "Aggregator"
	WorkerTypeConverter           = "Converter"
	WorkerTypeDateRangeFilter     = "DateRangeFilter"
	WorkerTypeAvgFormatFilter     = "AvgFormatFilter"
	WorkerTypeSpliter             = "Spliter"
	WorkerTypeScatterAndGather    = "ScatterAndGather"
	WorkerTypeScatterGather       = "ScatterGather"
	WorkerTypeScatterGatherFilter = "ScatterGatherFilter"
	WorkerTypeJoinQuery4          = "JoinQuery4"
	WorkerTypeMonitor             = "Monitor"
)

// TODO: Define worker types as constants
func workerFactory(workercfg *config.Config, communicationBroker broker.Broker) (Worker, error) {
	workerCfg := workercfg.Worker
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
	case WorkerTypeScatterGatherFilter:
		worker, err := filter.NewScatterGather(workerCfg, communicationBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create ScatterGatherFilter: %w", err)
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
	case WorkerTypeJoinQuery4:
		worker, err := join.NewQuery4(workerCfg, communicationBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create JoinQuery4: %w", err)
		}
		return worker, nil
	case WorkerTypeAvgFormatFilter:
		if workercfg.AvgBroker == nil {
			return nil, fmt.Errorf("avg_broker config is required for AvgFormatFilter")
		}
		avgBroker, err := broker.NewBroker(*workercfg.AvgBroker)
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
	case WorkerTypeSpliter:
		worker, err := router.NewSpliter(workerCfg, communicationBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create Spliter: %w", err)
		}
		return worker, nil
	case WorkerTypeAggregator:
		worker, err := aggregator.NewAggregator(workerCfg, communicationBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create Aggregator: %w", err)
		}
		return worker, nil
	case WorkerTypeScatterAndGather:
		worker, err := aggregator.NewScatterAndGather(workerCfg, communicationBroker, workercfg.Broker.RabbitURL)
		if err != nil {
			return nil, fmt.Errorf("failed to create ScatterAndGather: %w", err)
		}
		return worker, nil
	case WorkerTypeScatterGather:
		worker, err := aggregator.NewScatterGather(workerCfg, communicationBroker)
		if err != nil {
			return nil, fmt.Errorf("failed to create ScatterGather: %w", err)
		}
		return worker, nil
	case WorkerTypeConverter:
		worker := converter.NewConverter(workerCfg, communicationBroker)
		return worker, nil
	case WorkerTypeMonitor:
		if workercfg.Monitor == nil {
			return nil, fmt.Errorf("monitor config required for Monitor worker type")
		}
		selfKey := fmt.Sprintf("%s_%d", workerCfg.WorkerPrefix, workerCfg.WorkerID)

		worker, err := monitor.New(workercfg.Monitor, selfKey, workerCfg.WorkerID, workercfg.Heartbeat.MonitorHosts)
		if err != nil {
			return nil, fmt.Errorf("failed to create Monitor: %w", err)
		}
		return worker, nil
	default:
		return nil, fmt.Errorf("unknown worker type: %s", workerCfg.Type)
	}
}
