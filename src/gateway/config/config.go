package config

import (
	"os"
	"strconv"

	commonConfig "github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"gopkg.in/yaml.v3"
)

type GatewayConfig struct {
	BrokerConfig         commonConfig.BrokerConfig `yaml:"broker"`
	AccountsBrokerConfig *commonConfig.BrokerConfig
	ServerHost           string `yaml:"server_host"`
	ServerPort           string `yaml:"server_port"`
	CheckpointInterval   int                           `yaml:"checkpoint_interval"`
	CheckpointDir        string                        `yaml:"-"`
	Monitoring           *commonConfig.MonitoringConfig `yaml:"monitoring,omitempty"`
}

const CONFIG_PATH = "../config.yaml"

func LoadConfig() (*GatewayConfig, error) {
	commonConfig.InitSystemDefaults()

	data, err := os.ReadFile(CONFIG_PATH)
	if err != nil {
		return nil, err
	}

	var cfg GatewayConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	if err := loadSystemDefaults(&cfg); err != nil {
		return nil, err
	}

	commonConfig.LoadSystemDefaultsForBroker(&cfg.BrokerConfig)

	if accountsConfigPath := os.Getenv("ACCOUNTS_CONFIG_PATH"); accountsConfigPath != "" {
		accountsCfg, err := commonConfig.LoadAccountConfig(accountsConfigPath)
		if err != nil {
			return nil, err
		}
		cfg.AccountsBrokerConfig = accountsCfg
	}

	if checkpointDir := os.Getenv("CHECKPOINT_DIR"); checkpointDir != "" {
		cfg.CheckpointDir = checkpointDir
	}
	if cfg.CheckpointDir == "" {
		cfg.CheckpointDir = "/app/checkpoints"
	}

	if value := os.Getenv("CHECKPOINT_INTERVAL"); value != "" {
		interval, err := strconv.Atoi(value)
		if err != nil {
			return nil, err
		}
		cfg.CheckpointInterval = interval
	}
	if cfg.CheckpointInterval < 1 {
		cfg.CheckpointInterval = 1
	}

	if cfg.Monitoring == nil || cfg.Monitoring.Port == 0 {
		cfg.Monitoring = &commonConfig.MonitoringConfig{Port: 9000}
	}

	return &cfg, nil
}

func loadSystemDefaults(cfg *GatewayConfig) error {
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

	var sysCfg struct {
		Monitoring *commonConfig.MonitoringConfig `yaml:"monitoring"`
		Worker     *struct {
			CheckpointInterval int `yaml:"checkpoint_interval"`
		} `yaml:"worker"`
	}
	if err := yaml.Unmarshal(data, &sysCfg); err != nil {
		return err
	}

	if cfg.Monitoring == nil && sysCfg.Monitoring != nil {
		cfg.Monitoring = sysCfg.Monitoring
	}
	if cfg.CheckpointInterval == 0 && sysCfg.Worker != nil {
		cfg.CheckpointInterval = sysCfg.Worker.CheckpointInterval
	}
	return nil
}
