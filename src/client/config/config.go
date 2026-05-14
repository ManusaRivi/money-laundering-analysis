package config

import (
	"github.com/spf13/viper"
)

type ServerConfig struct {
	Host                     string `yaml:"host"`
	Port                     string `yaml:"port"`
	ConnectionAttempts       int    `yaml:"connection_attempts"`
	ConnectionAttemptDelayMs int    `yaml:"connection_attempt_delay_ms"`
}

type ClientConfig struct {
	DatasetPath string `yaml:"dataset_path"`
	OutputPath  string `yaml:"output_path"`

	Server ServerConfig
}

const CONFIG_PATH = "../config.yaml"

func LoadConfig() (*ClientConfig, error) {
	v := viper.New()
	v.SetConfigFile(CONFIG_PATH)
	err := v.ReadInConfig()
	if err != nil {
		return nil, err
	}
	config := &ClientConfig{
		DatasetPath: v.GetString("dataset_path"),
		OutputPath:  v.GetString("output_path"),
		Server: ServerConfig{
			Host:                     v.GetString("server.host"),
			Port:                     v.GetString("server.port"),
			ConnectionAttempts:       v.GetInt("server.connection_attempts"),
			ConnectionAttemptDelayMs: v.GetInt("server.connection_attempt_delay_ms"),
		},
	}
	return config, nil
}
