package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

type MonitorConfig struct {
	UdpHost          string `yaml:"udp_host"`
	UdpPort          int    `yaml:"udp_port"`
	HeartbeatTimeout string `yaml:"heartbeat_timeout"`
	CheckInterval    string `yaml:"check_interval"`
	FailureThreshold int    `yaml:"failure_threshold"`
	RestartCooldown  string `yaml:"restart_cooldown"`
	RabbitMQHost     string `yaml:"rabbitmq_host"`
	RabbitMQPort     int    `yaml:"rabbitmq_port"`
	TcpHost          string `yaml:"tcp_host"`
	TcpPort          int    `yaml:"tcp_port"`
	PingInterval     string `yaml:"ping_interval"`
	PingTimeout      string `yaml:"ping_timeout"`
}

type HeartbeatConfig struct {
	MonitorHosts []string `yaml:"monitor_hosts"`
	MonitorPort  int      `yaml:"monitor_port"`
	Interval     int      `yaml:"interval"`
}

type Config struct {
	Broker    BrokerConfig    `yaml:"broker,omitempty"`
	AvgBroker *BrokerConfig   `yaml:"avg_broker,omitempty"`
	Worker    WorkerConfig    `yaml:"worker"`
	Monitor   *MonitorConfig  `yaml:"monitor,omitempty"`
	Heartbeat *HeartbeatConfig `yaml:"heartbeat,omitempty"`
}

type BrokerConfig struct {
	Type         string   `yaml:"type"`
	RabbitURL    string   `yaml:"url"`
	Input        string   `yaml:"input"`
	InputQueue   string   `yaml:"input_queue"`
	Output       string   `yaml:"output"`
	InputKeys    []string `yaml:"input_keys"`
	ExchangeType string   `yaml:"exchange_type"`
	OutputExchangeType string   `yaml:"output_exchange_type"`
	Prefetch     int      `yaml:"prefetch"`
	Durable      bool     `yaml:"durable"`
	AutoDelete   bool     `yaml:"auto_delete"`
	Exclusive    bool     `yaml:"exclusive"`
	NoWait       bool     `yaml:"no_wait"`
	Internal     bool     `yaml:"internal"`
	Lazy         bool     `yaml:"lazy"`
	Persistent   bool     `yaml:"persistent"`

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
	Query  int            `yaml:"query"`

	WorkerID         int                     `yaml:"-"`
	WorkerPrefix     string                  `yaml:"-"`
	WorkerAmount     int                     `yaml:"-"`
	PrevWorkerAmount int                     `yaml:"-"`
	PrevWorkerPrefix string                  `yaml:"-"`
	NextWorkerAmount int                     `yaml:"-"`
	NextWorkerPrefix string                  `yaml:"-"`
	Threshold        int                     `yaml:"-"` // from SCATTER_GATHER_THRESHOLD; shared Q4 threshold
	SyncEOFConfig    SyncEOFControllerConfig `yaml:"-"`
}

type SyncEOFControllerConfig struct {
	RetryBaseDelay float64 `yaml:"retries_base_delay"`
	RetryStepDelay float64 `yaml:"retries_step_delay"`
	MaxRetries     int     `yaml:"max_retries"`
	
	RabbitURL         string `yaml:"-"`
	WorkerID          int    `yaml:"-"`
	EOFPrefix         string `yaml:"-"`
	WorkerAmount      int    `yaml:"-"`
	BroadcastExchange string `yaml:"-"`
	InputKeys	[]string `yaml:"-"`
}

func Load(filepath string) (*Config, error) {
	cfg := &Config{}

	if err := loadSystemDefaults(cfg); err != nil {
		slog.Warn("failed to load system defaults", "error", err)
	}

	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	if err := applyEnv(cfg); err != nil {
		return nil, err
	}
	if err := verifyConfig(cfg); err != nil {
		return nil, err
	}

	if cfg.Worker.Type == "Monitor" {
		return cfg, nil
	}

	if err := applyBrokerDefaults(&cfg.Broker); err != nil {
		return nil, err
	}
	if cfg.AvgBroker != nil {
		if err := applyBrokerDefaults(cfg.AvgBroker); err != nil {
			return nil, err
		}
	}
	applyEOFDefaults(cfg)

	return cfg, nil
}

func LoadAccountConfig(filepath string) (*BrokerConfig, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}

	var cfg struct {
		AccountsBroker BrokerConfig `yaml:"accounts_broker"`
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	if err := applyBrokerDefaults(&cfg.AccountsBroker); err != nil {
		return nil, err
	}

	return &cfg.AccountsBroker, nil
}

func loadSystemDefaults(cfg *Config) error {
	sysPath := os.Getenv("SYSTEM_CONFIG_PATH")
	if sysPath == "" {
		sysPath = "/app/system.yaml"
	}

	data, err := os.ReadFile(sysPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	return yaml.Unmarshal(data, cfg)
}

func verifyConfig(cfg *Config) error {
	if cfg.Worker.Type == "" {
		return fmt.Errorf("worker type is required")
	}

	if cfg.Worker.Type == "Monitor" {
		if cfg.Monitor == nil {
			return fmt.Errorf("monitor config section is required for Monitor worker type")
		}
		if cfg.Monitor.UdpHost == "" {
			return fmt.Errorf("monitor.udp_host is required")
		}
		if cfg.Monitor.UdpPort == 0 {
			return fmt.Errorf("monitor.udp_port is required")
		}
		if cfg.Monitor.HeartbeatTimeout == "" {
			return fmt.Errorf("monitor.heartbeat_timeout is required")
		}
		if _, err := time.ParseDuration(cfg.Monitor.HeartbeatTimeout); err != nil {
			return fmt.Errorf("monitor.heartbeat_timeout: %w", err)
		}
		if cfg.Monitor.CheckInterval == "" {
			return fmt.Errorf("monitor.check_interval is required")
		}
		if _, err := time.ParseDuration(cfg.Monitor.CheckInterval); err != nil {
			return fmt.Errorf("monitor.check_interval: %w", err)
		}
		if cfg.Monitor.FailureThreshold == 0 {
			cfg.Monitor.FailureThreshold = 1
		}
		if cfg.Monitor.RestartCooldown == "" {
			cfg.Monitor.RestartCooldown = "1s"
		}
		if _, err := time.ParseDuration(cfg.Monitor.RestartCooldown); err != nil {
			return fmt.Errorf("monitor.restart_cooldown: %w", err)
		}
		if cfg.Monitor.RabbitMQHost == "" {
			cfg.Monitor.RabbitMQHost = "rabbitmq"
		}
		if cfg.Monitor.RabbitMQPort == 0 {
			cfg.Monitor.RabbitMQPort = 5672
		}
		if cfg.Monitor.TcpHost == "" {
			cfg.Monitor.TcpHost = "0.0.0.0"
		}
		if cfg.Monitor.TcpPort == 0 {
			cfg.Monitor.TcpPort = 9001
		}
		if cfg.Monitor.PingInterval == "" {
			cfg.Monitor.PingInterval = "1.5s"
		}
		if _, err := time.ParseDuration(cfg.Monitor.PingInterval); err != nil {
			return fmt.Errorf("monitor.ping_interval: %w", err)
		}
		if cfg.Monitor.PingTimeout == "" {
			cfg.Monitor.PingTimeout = "500ms"
		}
		if _, err := time.ParseDuration(cfg.Monitor.PingTimeout); err != nil {
			return fmt.Errorf("monitor.ping_timeout: %w", err)
		}
		return nil
	}

	if cfg.Broker.Type == "" {
		return fmt.Errorf("broker type is required")
	}
	if cfg.Broker.RabbitURL == "" {
		return fmt.Errorf("broker url is required")
	}
	if cfg.Broker.Input == "" {
		return fmt.Errorf("broker input is required")
	}
	if cfg.Broker.Output == "" {
		return fmt.Errorf("broker output is required")
	}
	return nil
}

func applyEnv(cfg *Config) error {
	brokerConfig := &cfg.Broker
	workerConfig := &cfg.Worker
	if value := os.Getenv("ID"); value != "" {
		id, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid ID: %w", err)
		}
		brokerConfig.WorkerID = id
		workerConfig.WorkerID = id
	}

	if value := os.Getenv("WORKER_AMOUNT"); value != "" {
		amount, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid WORKER_AMOUNT: %w", err)
		}
		brokerConfig.WorkerAmount = amount
		workerConfig.WorkerAmount = amount
	}

	if value := os.Getenv("PREV_WORKER_AMOUNT"); value != "" {
		amount, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid PREV_WORKER_AMOUNT: %w", err)
		}
		brokerConfig.PrevWorkerAmount = amount
		workerConfig.PrevWorkerAmount = amount
	}

	if value := os.Getenv("NEXT_WORKER_AMOUNT"); value != "" {
		amount, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid NEXT_WORKER_AMOUNT: %w", err)
		}
		brokerConfig.NextWorkerAmount = amount
		workerConfig.NextWorkerAmount = amount
	}

	if value := os.Getenv("SCATTER_GATHER_THRESHOLD"); value != "" {
		threshold, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid SCATTER_GATHER_THRESHOLD: %w", err)
		}
		workerConfig.Threshold = threshold
	}

	prefix := os.Getenv("WORKER_PREFIX")
	brokerConfig.WorkerPrefix = prefix
	workerConfig.WorkerPrefix = prefix

	prefix = os.Getenv("PREV_WORKER_PREFIX")
	brokerConfig.PrevWorkerPrefix = prefix
	workerConfig.PrevWorkerPrefix = prefix

	prefix = os.Getenv("NEXT_WORKER_PREFIX")
	brokerConfig.NextWorkerPrefix = prefix
	workerConfig.NextWorkerPrefix = prefix

	return nil
}

func applyBrokerDefaults(cfg *BrokerConfig) error {
	if cfg.ExchangeType == "" {
		cfg.ExchangeType = "direct"
	}
	if cfg.Prefetch == 0 {
		cfg.Prefetch = 30
	}

	// A "queue" broker is a single point-to-point queue. Only one of
	// input/output needs to be set — whichever is set names the queue.
	if cfg.Type == "queue" {
		if cfg.Input == "" && cfg.Output == "" {
			return fmt.Errorf("queue broker requires either input or output")
		}
		return nil
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
	} else if cfg.Output == "" {
		if cfg.NextWorkerPrefix == "" {
			return fmt.Errorf("NEXT_WORKER_PREFIX environment variable is required for output queue")
		}
		cfg.Output = cfg.NextWorkerPrefix
	}

	if cfg.OutputExchangeType == "" {
		cfg.OutputExchangeType = cfg.ExchangeType
	}

	return nil
}

func applyEOFDefaults(cfg *Config) {
	brokerConfig := &cfg.Broker
	workerConfig := &cfg.Worker
	eofConfig := &workerConfig.SyncEOFConfig

	eofConfig.RabbitURL = brokerConfig.RabbitURL
	eofConfig.WorkerID = brokerConfig.WorkerID
	eofConfig.EOFPrefix = fmt.Sprintf("%s_eof", brokerConfig.WorkerPrefix)
	eofConfig.WorkerAmount = brokerConfig.WorkerAmount
	eofConfig.BroadcastExchange = fmt.Sprintf("%s_eof_broadcast", brokerConfig.WorkerPrefix)
	eofConfig.InputKeys = cfg.Broker.InputKeys
}

func isInputExchangeType(brokerType string) bool {
	return brokerType == "e-q" || brokerType == "e-e"
}

func isOutputExchangeType(brokerType string) bool {
	return brokerType == "q-e" || brokerType == "e-e"
}
