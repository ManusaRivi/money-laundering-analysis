package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

type MonitoringConfig struct {
	Port int `yaml:"port"`
}

type BullyParams struct {
	TcpHost      string `yaml:"tcp_host"`
	TcpPort      int    `yaml:"tcp_port"`
	PingInterval string `yaml:"ping_interval"`
	PingTimeout  string `yaml:"ping_timeout"`
}

type MonitoringParams struct {
	UdpPort          int    `yaml:"udp_port"`
	PingInterval     string `yaml:"ping_interval"`
	PingTimeout      string `yaml:"ping_timeout"`
	FailureThreshold int    `yaml:"failure_threshold"`
}

type MonitorWorkerParams struct {
	Bully      BullyParams      `yaml:"bully"`
	Monitoring MonitoringParams `yaml:"monitoring"`
}

type QueueEndpoint struct {
	Name       string `yaml:"name"`
	Prefetch   int    `yaml:"prefetch,omitempty"`
	Durable    *bool  `yaml:"durable,omitempty"`
	AutoDelete *bool  `yaml:"auto_delete,omitempty"`
	Exclusive  *bool  `yaml:"exclusive,omitempty"`
	NoWait     *bool  `yaml:"no_wait,omitempty"`
	Lazy       *bool  `yaml:"lazy,omitempty"`
	Persistent *bool  `yaml:"persistent,omitempty"`
}

type ExchangeEndpoint struct {
	Name       string   `yaml:"name"`
	Type       string   `yaml:"type,omitempty"`
	Keys       []string `yaml:"keys,omitempty"`
	Durable    *bool    `yaml:"durable,omitempty"`
	AutoDelete *bool    `yaml:"auto_delete,omitempty"`
	Internal   *bool    `yaml:"internal,omitempty"`
	NoWait     *bool    `yaml:"no_wait,omitempty"`
	Persistent *bool    `yaml:"persistent,omitempty"`
}

type Endpoint struct {
	Queue    *QueueEndpoint    `yaml:"queue,omitempty"`
	Exchange *ExchangeEndpoint `yaml:"exchange,omitempty"`
}

type SystemBrokerQueueDefaults struct {
	Prefetch   int  `yaml:"prefetch"`
	Durable    bool `yaml:"durable"`
	AutoDelete bool `yaml:"auto_delete"`
	Exclusive  bool `yaml:"exclusive"`
	NoWait     bool `yaml:"no_wait"`
	Internal   bool `yaml:"internal"`
	Lazy       bool `yaml:"lazy"`
}

type SystemBrokerEndpointDefaults struct {
	Queue *SystemBrokerQueueDefaults `yaml:"queue"`
}

type SystemBrokerDefaults struct {
	URL    string                        `yaml:"url"`
	Input  *SystemBrokerEndpointDefaults `yaml:"input"`
	Output *SystemBrokerEndpointDefaults `yaml:"output"`
}

type BrokerConfig struct {
	Type      string    `yaml:"type"`
	RabbitURL string    `yaml:"url"`
	Input     *Endpoint `yaml:"input,omitempty"`
	Output    *Endpoint `yaml:"output,omitempty"`

	WorkerID         int    `yaml:"-"`
	WorkerPrefix     string `yaml:"-"`
	WorkerAmount     int    `yaml:"-"`
	PrevWorkerAmount int    `yaml:"-"`
	PrevWorkerPrefix string `yaml:"-"`
	NextWorkerAmount int    `yaml:"-"`
	NextWorkerPrefix string `yaml:"-"`
}

type Config struct {
	Broker        BrokerConfig      `yaml:"broker,omitempty"`
	AvgBroker     *BrokerConfig     `yaml:"avg_broker,omitempty"`
	Worker        WorkerConfig      `yaml:"worker"`
	CheckpointDir string            `yaml:"-"`
	Monitoring    *MonitoringConfig `yaml:"monitoring,omitempty"`
}

type SystemWorkerDefaults struct {
	CheckpointInterval int `yaml:"checkpoint_interval"`
}

var systemDefaults *SystemBrokerDefaults
var systemWorkerDefaults *SystemWorkerDefaults

type SystemConfig struct {
	Monitoring *MonitoringConfig     `yaml:"monitoring"`
	Broker     *SystemBrokerDefaults `yaml:"broker"`
	Worker     *SystemWorkerDefaults `yaml:"worker"`
}

func InitSystemDefaults() {
	cfg := &Config{}
	if err := loadSystemDefaults(cfg); err != nil {
		slog.Warn("failed to load system defaults", "error", err)
	}
}

func ParseMonitorParams(params map[string]any) (*MonitorWorkerParams, error) {
	data, err := yaml.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("marshal monitor params: %w", err)
	}
	var p MonitorWorkerParams
	if err := yaml.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("unmarshal monitor params: %w", err)
	}
	return &p, nil
}

func LoadSystemDefaultsForBroker(cfg *BrokerConfig) {
	applyBrokerDefaults(cfg)
}

type WorkerConfig struct {
	Type   string         `yaml:"type"`
	Params map[string]any `yaml:"params"`
	Query  int            `yaml:"query"`

	WorkerID         int    `yaml:"-"`
	WorkerPrefix     string `yaml:"-"`
	WorkerAmount     int    `yaml:"-"`
	PrevWorkerAmount int    `yaml:"-"`
	PrevWorkerPrefix string `yaml:"-"`
	NextWorkerAmount int    `yaml:"-"`
	NextWorkerPrefix string `yaml:"-"`
	Threshold        int    `yaml:"-"` // from SCATTER_GATHER_THRESHOLD; shared Q4 threshold

	SyncEOFConfig      SyncEOFControllerConfig `yaml:"sync_eof"`
	CheckpointDir      string                  `yaml:"-"`
	CheckpointInterval int                     `yaml:"checkpoint_interval"`
}

type SyncEOFControllerConfig struct {
	RetryBaseDelay float64 `yaml:"retries_base_delay"`
	RetryStepDelay float64 `yaml:"retries_step_delay"`
	MaxRetries     int     `yaml:"max_retries"`

	RabbitURL         string   `yaml:"-"`
	WorkerID          int      `yaml:"-"`
	EOFPrefix         string   `yaml:"-"`
	WorkerAmount      int      `yaml:"-"`
	BroadcastExchange string   `yaml:"-"`
	InputKeys         []string `yaml:"-"`
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

	applyWorkerDefaults(cfg)

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

	if cfg.Broker.Input != nil && cfg.Broker.Input.Queue != nil && cfg.Worker.CheckpointInterval > cfg.Broker.Input.Queue.Prefetch {
		return nil, fmt.Errorf("worker checkpoint_interval cannot be greater than input queue prefetch")
	}

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

	var sysCfg SystemConfig
	if err := yaml.Unmarshal(data, &sysCfg); err != nil {
		return err
	}
	if sysCfg.Monitoring != nil {
		cfg.Monitoring = sysCfg.Monitoring
	}
	if sysCfg.Broker != nil {
		systemDefaults = sysCfg.Broker
	}
	if sysCfg.Worker != nil {
		systemWorkerDefaults = sysCfg.Worker
	}
	return nil
}

func verifyConfig(cfg *Config) error {
	if cfg.Worker.Type == "" {
		return fmt.Errorf("worker type is required")
	}

	if cfg.Worker.Type == "Monitor" {
		if cfg.Worker.Params == nil {
			return fmt.Errorf("worker.params is required for Monitor worker type")
		}
		return nil
	}

	if cfg.Monitoring == nil || cfg.Monitoring.Port == 0 {
		cfg.Monitoring = &MonitoringConfig{Port: 9000}
	}

	if cfg.Broker.Type == "" {
		return fmt.Errorf("broker type is required")
	}
	if cfg.Broker.Input == nil {
		return fmt.Errorf("broker input is required")
	}
	if cfg.Broker.Output == nil {
		return fmt.Errorf("broker output is required")
	}
	if cfg.Worker.Type == "" {
		return fmt.Errorf("worker type is required")
	}
	if cfg.Worker.CheckpointInterval == 0 {
		return fmt.Errorf("worker checkpoint_interval is required (set in config yaml or system.yaml)")
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

	if cfg.AvgBroker != nil {
		cfg.AvgBroker.WorkerID = brokerConfig.WorkerID
		cfg.AvgBroker.WorkerAmount = brokerConfig.WorkerAmount
		cfg.AvgBroker.PrevWorkerAmount = brokerConfig.PrevWorkerAmount
		cfg.AvgBroker.NextWorkerAmount = brokerConfig.NextWorkerAmount
		cfg.AvgBroker.WorkerPrefix = brokerConfig.WorkerPrefix
		cfg.AvgBroker.PrevWorkerPrefix = brokerConfig.PrevWorkerPrefix
		cfg.AvgBroker.NextWorkerPrefix = brokerConfig.NextWorkerPrefix
	}

	cfg.Worker.CheckpointDir = os.Getenv("CHECKPOINT_DIR")
	if cfg.Worker.CheckpointDir == "" {
		return fmt.Errorf("CHECKPOINT_DIR environment variable is required")
	}

	return nil
}

func applyWorkerDefaults(cfg *Config) {
	if systemWorkerDefaults != nil && cfg.Worker.CheckpointInterval == 0 {
		cfg.Worker.CheckpointInterval = systemWorkerDefaults.CheckpointInterval
	}
}

func systemQueueInputDefaults(sys *SystemBrokerDefaults) *SystemBrokerQueueDefaults {
	if sys != nil && sys.Input != nil {
		return sys.Input.Queue
	}
	return nil
}

func systemQueueOutputDefaults(sys *SystemBrokerDefaults) *SystemBrokerQueueDefaults {
	if sys != nil && sys.Output != nil {
		return sys.Output.Queue
	}
	return nil
}

func boolPtr(v bool) *bool { return &v }

func applyQueueDefaults(cfg *BrokerConfig, q **QueueEndpoint, sys *SystemBrokerQueueDefaults) {
	if *q == nil {
		*q = &QueueEndpoint{}
	}
	if sys != nil {
		if (*q).Prefetch == 0 {
			(*q).Prefetch = sys.Prefetch
		}
		if (*q).Durable == nil {
			(*q).Durable = boolPtr(sys.Durable)
		}
		if (*q).AutoDelete == nil {
			(*q).AutoDelete = boolPtr(sys.AutoDelete)
		}
		if (*q).Exclusive == nil {
			(*q).Exclusive = boolPtr(sys.Exclusive)
		}
		if (*q).NoWait == nil {
			(*q).NoWait = boolPtr(sys.NoWait)
		}
		if (*q).Lazy == nil {
			(*q).Lazy = boolPtr(sys.Lazy)
		}
	}
	if (*q).Durable == nil {
		(*q).Durable = boolPtr(true)
	}
	if (*q).AutoDelete == nil {
		(*q).AutoDelete = boolPtr(false)
	}
	if (*q).Exclusive == nil {
		(*q).Exclusive = boolPtr(false)
	}
	if (*q).NoWait == nil {
		(*q).NoWait = boolPtr(false)
	}
	if (*q).Lazy == nil {
		(*q).Lazy = boolPtr(false)
	}
	if (*q).Persistent == nil {
		(*q).Persistent = boolPtr(false)
	}
}

func applyExchangeDefaults(ep *Endpoint) {
	if ep.Exchange == nil {
		return
	}
	var sys *SystemBrokerQueueDefaults
	if systemDefaults != nil && systemDefaults.Input != nil {
		sys = systemDefaults.Input.Queue
	}
	if sys != nil {
		if ep.Exchange.Durable == nil {
			ep.Exchange.Durable = boolPtr(sys.Durable)
		}
		if ep.Exchange.AutoDelete == nil {
			ep.Exchange.AutoDelete = boolPtr(sys.AutoDelete)
		}
		if ep.Exchange.Internal == nil {
			ep.Exchange.Internal = boolPtr(sys.Internal)
		}
		if ep.Exchange.NoWait == nil {
			ep.Exchange.NoWait = boolPtr(sys.NoWait)
		}
	}
	if ep.Exchange.Durable == nil {
		ep.Exchange.Durable = boolPtr(true)
	}
	if ep.Exchange.AutoDelete == nil {
		ep.Exchange.AutoDelete = boolPtr(false)
	}
	if ep.Exchange.Internal == nil {
		ep.Exchange.Internal = boolPtr(false)
	}
	if ep.Exchange.NoWait == nil {
		ep.Exchange.NoWait = boolPtr(false)
	}
	if ep.Exchange.Persistent == nil {
		ep.Exchange.Persistent = boolPtr(false)
	}
}

func applyBrokerDefaults(cfg *BrokerConfig) error {
	if systemDefaults != nil && cfg.RabbitURL == "" {
		cfg.RabbitURL = systemDefaults.URL
	}
	if cfg.RabbitURL == "" {
		cfg.RabbitURL = "amqp://guest:guest@rabbitmq:5672/"
	}

	if cfg.Input == nil {
		cfg.Input = &Endpoint{}
	}
	if cfg.Output == nil {
		cfg.Output = &Endpoint{}
	}

	applyQueueDefaults(cfg, &cfg.Input.Queue, systemQueueInputDefaults(systemDefaults))
	applyQueueDefaults(cfg, &cfg.Output.Queue, systemQueueOutputDefaults(systemDefaults))

	applyExchangeDefaults(cfg.Input)
	applyExchangeDefaults(cfg.Output)

	if cfg.Type == "queue" {
		if cfg.Input.Queue == nil && cfg.Output.Queue == nil {
			return fmt.Errorf("queue broker requires input.queue or output.queue")
		}
		if cfg.Input.Queue.Name == "" && cfg.Output.Queue.Name == "" {
			return fmt.Errorf("queue broker requires input.queue.name or output.queue.name")
		}
		return nil
	}

	if isInputExchangeType(cfg.Type) {
		if cfg.Input.Exchange == nil {
			cfg.Input.Exchange = &ExchangeEndpoint{}
		}
		if cfg.Input.Exchange.Name == "" {
			if cfg.WorkerPrefix == "" {
				return fmt.Errorf("WORKER_PREFIX environment variable is required for input exchange")
			}
			cfg.Input.Exchange.Name = cfg.WorkerPrefix
		}
		if cfg.Input.Exchange.Type == "" {
			return fmt.Errorf("input exchange type is required for exchange input")
		}
		if len(cfg.Input.Exchange.Keys) == 0 && cfg.Input.Exchange.Type != "fanout" {
			if cfg.WorkerPrefix == "" {
				return fmt.Errorf("WORKER_PREFIX environment variable is required for input keys")
			}
			cfg.Input.Exchange.Keys = []string{fmt.Sprintf("%s_%d", cfg.WorkerPrefix, cfg.WorkerID)}
			if cfg.Input.Queue == nil {
				cfg.Input.Queue = &QueueEndpoint{}
			}
			cfg.Input.Queue.Name = fmt.Sprintf("%s_%d", cfg.WorkerPrefix, cfg.WorkerID)
		}
		if len(cfg.Input.Exchange.Keys) == 0 && cfg.Input.Exchange.Type == "fanout" {
			if cfg.WorkerPrefix == "" {
				return fmt.Errorf("WORKER_PREFIX environment variable is required for input keys")
			}
			if cfg.Input.Queue == nil {
				cfg.Input.Queue = &QueueEndpoint{}
			}
			cfg.Input.Queue.Name = fmt.Sprintf("%s_%d", cfg.WorkerPrefix, cfg.WorkerID)
		}
		if cfg.Input.Queue == nil {
			cfg.Input.Queue = &QueueEndpoint{}
		}
	} else {
		if cfg.Input.Queue == nil {
			cfg.Input.Queue = &QueueEndpoint{}
		}
		if cfg.Input.Queue.Name == "" {
			if cfg.WorkerPrefix == "" {
				return fmt.Errorf("WORKER_PREFIX environment variable is required for input queue")
			}
			cfg.Input.Queue.Name = cfg.WorkerPrefix
		}
	}

	if isOutputExchangeType(cfg.Type) {
		if cfg.Output.Exchange == nil {
			cfg.Output.Exchange = &ExchangeEndpoint{}
		}
		if cfg.Output.Exchange.Name == "" {
			if cfg.NextWorkerPrefix == "" {
				return fmt.Errorf("NEXT_WORKER_PREFIX environment variable is required for output exchange")
			}
			cfg.Output.Exchange.Name = cfg.NextWorkerPrefix
		}
		if cfg.Output.Exchange.Type == "" {
			return fmt.Errorf("output exchange type is required for exchange output")
		}
	} else {
		if cfg.Output.Queue == nil {
			cfg.Output.Queue = &QueueEndpoint{}
		}
		if cfg.Output.Queue.Name == "" {
			if cfg.NextWorkerPrefix == "" {
				return fmt.Errorf("NEXT_WORKER_PREFIX environment variable is required for output queue")
			}
			cfg.Output.Queue.Name = cfg.NextWorkerPrefix
		}
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
	if isInputExchangeType(brokerConfig.Type) && brokerConfig.Input != nil && brokerConfig.Input.Exchange != nil {
		eofConfig.InputKeys = brokerConfig.Input.Exchange.Keys
	}
}

func isInputExchangeType(brokerType string) bool {
	return brokerType == "e-q" || brokerType == "e-e"
}

func isOutputExchangeType(brokerType string) bool {
	return brokerType == "q-e" || brokerType == "e-e"
}
