package config

import "github.com/spf13/viper"

type GatewayConfig struct {
	InputQueueName  string
	OutputQueueName string
	ServerHost      string
	ServerPort      string
	MomHost         string
	MomPort         int
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
		InputQueueName:  v.GetString("input_queue_name"),
		OutputQueueName: v.GetString("output_queue_name"),
		ServerHost:      v.GetString("server_host"),
		ServerPort:      v.GetString("server_port"),
		MomHost:         v.GetString("mom_host"),
		MomPort:         v.GetInt("mom_port"),
	}
	return config, nil
}
