package core

import (
	"fmt"
	"os"
	"strconv"

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
	brokerConfig, err := buildBrokerConfig(cfg.Broker)
	if err != nil {
		return err
	}
	communicationBroker, err := broker.NewBroker(brokerConfig)
	if err != nil {
		return err
	}

	workerConfig := cfg.Worker
	var w Worker
	switch workerConfig.Type {
	case "SyncAmountFilter":
		w, err = filter.NewSyncAmountFilter(workerConfig.Params, communicationBroker)
		if err != nil {
			return fmt.Errorf("Failed to create SyncAmountFilter: %v", err)
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

func buildBrokerConfig(cfg config.BrokerConfig) (config.BrokerConfig, error) {
	if err := readWorkerEnv(&cfg); err != nil {
		return config.BrokerConfig{}, err
	}

	if broker.IsInputExchangeType(cfg.Type) {
		if cfg.Input == "" {
			if cfg.WorkerPrefix == "" {
				return config.BrokerConfig{}, fmt.Errorf("WORKER_PREFIX environment variable is required for input exchange")
			}
			cfg.Input = cfg.WorkerPrefix
		}
		if len(cfg.InputKeys) == 0 {
			if cfg.WorkerPrefix == "" {
				return config.BrokerConfig{}, fmt.Errorf("WORKER_PREFIX environment variable is required for input keys")
			}
			if cfg.WorkerID == 0 {
				return config.BrokerConfig{}, fmt.Errorf("ID environment variable is required for input keys")
			}
			cfg.InputKeys = []string{fmt.Sprintf("%s_%d", cfg.WorkerPrefix, cfg.WorkerID)}
		}
	} else if cfg.Input == "" {
		if cfg.WorkerPrefix == "" {
			return config.BrokerConfig{}, fmt.Errorf("WORKER_PREFIX environment variable is required for input queue")
		}
		cfg.Input = cfg.WorkerPrefix
	}

	if broker.IsOutputExchangeType(cfg.Type) {
		if cfg.Output == "" {
			if cfg.NextWorkerPrefix == "" {
				return config.BrokerConfig{}, fmt.Errorf("NEXT_WORKER_PREFIX environment variable is required for output exchange")
			}
			cfg.Output = cfg.NextWorkerPrefix
		}
		if len(cfg.OutputKeys) == 0 {
			if cfg.NextWorkerPrefix == "" {
				return config.BrokerConfig{}, fmt.Errorf("NEXT_WORKER_PREFIX environment variable is required for output keys")
			}
			if cfg.NextWorkerAmount <= 0 {
				return config.BrokerConfig{}, fmt.Errorf("NEXT_WORKER_AMOUNT environment variable is required for output keys")
			}
			cfg.OutputKeys = buildRoutingKeys(cfg.NextWorkerPrefix, cfg.NextWorkerAmount)
		}
	} else if cfg.Output == "" {
		if cfg.NextWorkerPrefix == "" {
			return config.BrokerConfig{}, fmt.Errorf("NEXT_WORKER_PREFIX environment variable is required for output queue")
		}
		cfg.Output = cfg.NextWorkerPrefix
	}

	return cfg, nil
}

func readWorkerEnv(cfg *config.BrokerConfig) error {
	if value := os.Getenv("ID"); value != "" {
		id, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid ID: %w", err)
		}
		cfg.WorkerID = id
	}

	if value := os.Getenv("WORKER_AMOUNT"); value != "" {
		amount, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid WORKER_AMOUNT: %w", err)
		}
		cfg.WorkerAmount = amount
	}

	if value := os.Getenv("PREV_WORKER_AMOUNT"); value != "" {
		amount, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid PREV_WORKER_AMOUNT: %w", err)
		}
		cfg.PrevWorkerAmount = amount
	}

	if value := os.Getenv("NEXT_WORKER_AMOUNT"); value != "" {
		amount, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid NEXT_WORKER_AMOUNT: %w", err)
		}
		cfg.NextWorkerAmount = amount
	}

	cfg.WorkerPrefix = os.Getenv("WORKER_PREFIX")
	cfg.PrevWorkerPrefix = os.Getenv("PREV_WORKER_PREFIX")
	cfg.NextWorkerPrefix = os.Getenv("NEXT_WORKER_PREFIX")

	return nil
}

func buildRoutingKeys(prefix string, amount int) []string {
	keys := make([]string, amount)
	for i := 0; i < amount; i++ {
		keys[i] = fmt.Sprintf("%s_%d", prefix, i)
	}
	return keys
}
