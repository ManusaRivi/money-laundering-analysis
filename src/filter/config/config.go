package config

import (
    "os"

    "gopkg.in/yaml.v3"
)

type Config struct {
    RabbitMQ RabbitMQConfig `yaml:"rabbitmq"`
    Filter   FilterConfig   `yaml:"filter"`
}

type RabbitMQConfig struct {
    RabbitURL      string `yaml:"url"`
    InputQueue     string `yaml:"input_queue"`
    OutputExchange string `yaml:"output_exchange"`
}

type FilterConfig struct {
    Type        string  `yaml:"type"`          // Tipo de filtro: "amount", "date_range", etc.
    Field       string  `yaml:"field"`         // Campo a filtrar: "Amount", "Timestamp"

    // Campos para filtros simples (amount, string)
    Operator    string  `yaml:"operator"`      
    ValueFloat  float64 `yaml:"value_float"`   
    ValueString string  `yaml:"value_string"`  

    // Campos para filtro por rango de fechas
    FromDate    string  `yaml:"from_date"`
    ToDate      string  `yaml:"to_date"`
}

func LoadConfig(filepath string) (*Config, error) {
    data, err := os.ReadFile(filepath)
    if err != nil {
        return nil, err
    }

    var cfg Config
    err = yaml.Unmarshal(data, &cfg)
    if err != nil {
        return nil, err
    }

    return &cfg, nil
}