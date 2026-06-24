package config

import (
	"os"

	commonConfig "github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"gopkg.in/yaml.v3"
)

type GatewayConfig struct {
	BrokerConfig         commonConfig.BrokerConfig `yaml:"broker"`
	AccountsBrokerConfig *commonConfig.BrokerConfig
	ServerHost           string `yaml:"server_host"`
	ServerPort           string `yaml:"server_port"`
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

	commonConfig.LoadSystemDefaultsForBroker(&cfg.BrokerConfig)

	if accountsConfigPath := os.Getenv("ACCOUNTS_CONFIG_PATH"); accountsConfigPath != "" {
		accountsCfg, err := commonConfig.LoadAccountConfig(accountsConfigPath)
		if err != nil {
			return nil, err
		}
		cfg.AccountsBrokerConfig = accountsCfg
	}

	return &cfg, nil
}
