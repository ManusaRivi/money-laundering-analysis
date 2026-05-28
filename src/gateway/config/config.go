package config

import (
	"os"

	commonConfig "github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/spf13/viper"
)

type GatewayConfig struct {
	BrokerConfig         commonConfig.BrokerConfig
	AccountsBrokerConfig *commonConfig.BrokerConfig
	ServerHost           string
	ServerPort           string
}

const CONFIG_PATH = "../config.yaml"

func LoadConfig() (*GatewayConfig, error) {
	v := viper.New()
	v.SetConfigFile(CONFIG_PATH)
	err := v.ReadInConfig()
	if err != nil {
		return nil, err
	}
	config := &GatewayConfig{
		BrokerConfig: commonConfig.BrokerConfig{
			Type:         v.GetString("type"),
			RabbitURL:    v.GetString("url"),
			Input:        v.GetString("input"),
			Output:       v.GetString("output"),
			ExchangeType: v.GetString("exchange_type"),
			OutputExchangeType: v.GetString("output_exchange_type"),
			Prefetch:     v.GetInt("prefetch"),
			Durable:      v.GetBool("durable"),
			AutoDelete:   v.GetBool("auto_delete"),
			Exclusive:    v.GetBool("exclusive"),
			NoWait:       v.GetBool("no_wait"),
			Internal:     v.GetBool("internal"),
		},
		ServerHost: v.GetString("server_host"),
		ServerPort: v.GetString("server_port"),
	}

	if config.BrokerConfig.OutputExchangeType == "" {
		config.BrokerConfig.OutputExchangeType = config.BrokerConfig.ExchangeType
	}

	if accountsConfigPath := os.Getenv("ACCOUNTS_CONFIG_PATH"); accountsConfigPath != "" {
		accountsCfg, err := commonConfig.LoadAccountConfig(accountsConfigPath)
		if err != nil {
			return nil, err
		}
		config.AccountsBrokerConfig = accountsCfg
	}

	return config, nil
}
