package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Broker BrokerConfig `yaml:"rabbitmq"`
	Worker WorkerConfig `yaml:"worker"`
}

type BrokerConfig struct {
	Type           string   `yaml:"type"`
	RabbitURL      string   `yaml:"url"`
	InputQueue     string   `yaml:"input_queue"`
	OutputQueue    string   `yaml:"output_queue"`
	OutputExchange string   `yaml:"output_exchange"`
	ExchangeType   string   `yaml:"exchange_type"`
	RoutingKeys    []string `yaml:"routing_keys"`
	Prefetch       int      `yaml:"prefetch"`
	Durable        bool     `yaml:"durable"`
	AutoDelete     bool     `yaml:"auto_delete"`
	Exclusive      bool     `yaml:"exclusive"`
	NoWait         bool     `yaml:"no_wait"`
	Internal       bool     `yaml:"internal"`
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

	return &cfg, nil
}
