package config

import (
	"fmt"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Broker BrokerConfig `yaml:"broker"`
	Worker WorkerConfig `yaml:"worker"`
}

type BrokerConfig struct {
	Type         string   `yaml:"type"`
	RabbitURL    string   `yaml:"url"`
	Input        string   `yaml:"input"`
	Output       string   `yaml:"output"`
	InputKeys    []string `yaml:"input_keys"`
	OutputKeys   []string `yaml:"output_keys"`
	ExchangeType string   `yaml:"exchange_type"`
	Prefetch     int      `yaml:"prefetch"`
	Durable      bool     `yaml:"durable"`
	AutoDelete   bool     `yaml:"auto_delete"`
	Exclusive    bool     `yaml:"exclusive"`
	NoWait       bool     `yaml:"no_wait"`
	Internal     bool     `yaml:"internal"`

	WorkerID         int    `yaml:"-"`
	WorkerPrefix     string `yaml:"-"`
	WorkerAmount     int    `yaml:"-"`
	PrevWorkerAmount int    `yaml:"-"`
	PrevWorkerPrefix string `yaml:"-"`
	NextWorkerAmount int    `yaml:"-"`
	NextWorkerPrefix string `yaml:"-"`
}

type WorkerConfig struct {
	Type   string         `yaml:"type"`
	Params map[string]any `yaml:"params"`
}

func Load(filepath string) (*Config, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	if err := applyEnv(&cfg.Broker); err != nil {
		return nil, err
	}
	if err := applyBrokerDefaults(&cfg.Broker); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func applyEnv(cfg *BrokerConfig) error {
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

func applyBrokerDefaults(cfg *BrokerConfig) error {
	if cfg.ExchangeType == "" {
		cfg.ExchangeType = "direct"
	}
	if cfg.Prefetch == 0 {
		cfg.Prefetch = 30
	}

	if isInputExchangeType(cfg.Type) {
		if cfg.Input == "" {
			if cfg.WorkerPrefix == "" {
				return fmt.Errorf("WORKER_PREFIX environment variable is required for input exchange")
			}
			cfg.Input = cfg.WorkerPrefix
		}
		if len(cfg.InputKeys) == 0 {
			if cfg.WorkerPrefix == "" {
				return fmt.Errorf("WORKER_PREFIX environment variable is required for input keys")
			}
			if cfg.WorkerID == 0 {
				return fmt.Errorf("ID environment variable is required for input keys")
			}
			cfg.InputKeys = []string{fmt.Sprintf("%s_%d", cfg.WorkerPrefix, cfg.WorkerID)}
		}
	} else if cfg.Input == "" {
		if cfg.WorkerPrefix == "" {
			return fmt.Errorf("WORKER_PREFIX environment variable is required for input queue")
		}
		cfg.Input = cfg.WorkerPrefix
	}

	if isOutputExchangeType(cfg.Type) {
		if cfg.Output == "" {
			if cfg.NextWorkerPrefix == "" {
				return fmt.Errorf("NEXT_WORKER_PREFIX environment variable is required for output exchange")
			}
			cfg.Output = cfg.NextWorkerPrefix
		}
		if len(cfg.OutputKeys) == 0 {
			if cfg.NextWorkerPrefix == "" {
				return fmt.Errorf("NEXT_WORKER_PREFIX environment variable is required for output keys")
			}
			if cfg.NextWorkerAmount <= 0 {
				return fmt.Errorf("NEXT_WORKER_AMOUNT environment variable is required for output keys")
			}
			cfg.OutputKeys = buildRoutingKeys(cfg.NextWorkerPrefix, cfg.NextWorkerAmount)
		}
	} else if cfg.Output == "" {
		if cfg.NextWorkerPrefix == "" {
			return fmt.Errorf("NEXT_WORKER_PREFIX environment variable is required for output queue")
		}
		cfg.Output = cfg.NextWorkerPrefix
	}

	return nil
}

func buildRoutingKeys(prefix string, amount int) []string {
	keys := make([]string, amount)
	for i := 0; i < amount; i++ {
		keys[i] = fmt.Sprintf("%s_%d", prefix, i)
	}
	return keys
}

func isInputExchangeType(brokerType string) bool {
	return brokerType == "e-q" || brokerType == "e-e"
}

func isOutputExchangeType(brokerType string) bool {
	return brokerType == "q-e" || brokerType == "e-e"
}

