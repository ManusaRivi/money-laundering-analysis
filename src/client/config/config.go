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
	TransactionsDatasetPath string `yaml:"transactions_dataset_path"`
	AccountsDatasetPath     string `yaml:"accounts_dataset_path"`
	OutputPath              string `yaml:"output_path"`
	TransactionBatchSize    int    `yaml:"transaction_batch_size"`
	AccountBatchSize        int    `yaml:"account_batch_size"`

	Server ServerConfig
}

const CONFIG_PATH = "../config.yaml"

func LoadConfig() (*ClientConfig, error) {
	v := viper.New()
	v.SetConfigFile(CONFIG_PATH)
	v.AutomaticEnv()
	err := v.ReadInConfig()
	if err != nil {
		return nil, err
	}
	config := &ClientConfig{
		TransactionsDatasetPath: v.GetString("transactions_dataset_path"),
		AccountsDatasetPath:     v.GetString("accounts_dataset_path"),
		OutputPath:              v.GetString("output_path"),
		TransactionBatchSize:    v.GetInt("transaction_batch_size"),
		AccountBatchSize:        v.GetInt("account_batch_size"),
		Server: ServerConfig{
			Host:                     v.GetString("server.host"),
			Port:                     v.GetString("server.port"),
			ConnectionAttempts:       v.GetInt("server.connection_attempts"),
			ConnectionAttemptDelayMs: v.GetInt("server.connection_attempt_delay_ms"),
		},
	}
	return config, nil
}
