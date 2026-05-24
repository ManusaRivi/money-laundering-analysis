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
	Type           string `yaml:"type"`
	RabbitURL      string `yaml:"url"`
	InputQueue     string `yaml:"input"`
	OutputExchange string `yaml:"output"`
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
