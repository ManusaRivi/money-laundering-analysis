package config

import (
	commonConfig "github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/spf13/viper"
)

type GatewayConfig struct {
	BrokerConfig commonConfig.BrokerConfig
	ServerHost   string
	ServerPort   string
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
	return config, nil
}
