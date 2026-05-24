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

	return &cfg, nil
}
