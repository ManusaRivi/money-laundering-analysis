package core

import (
	"fmt"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/workers/filter"
)

// Worker is the interface that all workers must implement.
type Worker interface {
	Run() error
	Stop()
}

func RunManager(cfg *config.Config) error {
	communicationBroker, err := broker.NewBroker(cfg.Broker)
	if err != nil {
		return err
	}

	workerConfig := cfg.Worker
	var w Worker
	switch workerConfig.Type {
	case "SyncFilter":
		w, err = filter.NewSyncFilter(workerConfig.Params, communicationBroker)
		if err != nil {
			return fmt.Errorf("Failed to create SyncFilter: %v", err)
		}
	// case "MaxAggregator":
	// 	w, err = aggregator.NewMaxAggregator(workerConfig.Params, communicationBroker)
	// 	if err != nil {
	// 		return fmt.Errorf("Failed to create MaxAggregator: %v", err)
	// 	}

	// TODO: Agregar todos los tipos de workers que haya

	default:
		return fmt.Errorf("Unknown WorkerType : %s", workerConfig.Type)
	}

	return w.Run()
}
