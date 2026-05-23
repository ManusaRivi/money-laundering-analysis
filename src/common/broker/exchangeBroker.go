package broker

import (
	"fmt"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
)

type ExchangeBroker struct{}

func CreateExchangeBroker(exchange string, keys []string, connectionSettings ConnSettings) (Broker, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	return createExchangeBroker(exchange, keys, url, config.BrokerConfig{})
}

func createExchangeBroker(exchange string, keys []string, rabbitURL string, cfg config.BrokerConfig) (Broker, error) {
	conn, channel, err := connectRabbit(rabbitURL)
	if err != nil {
		return nil, err
	}

	if cfg.ExchangeType == "" {
		cfg.ExchangeType = "direct"
	}
	if cfg.Prefetch == 0 {
		cfg.Prefetch = 30
	}

	if err := channel.ExchangeDeclare(
		exchange,
		cfg.ExchangeType,
		cfg.Durable,
		cfg.AutoDelete,
		cfg.Internal,
		cfg.NoWait,
		nil,
	); err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare exchange: %w", err)
	}

	if cfg.Prefetch > 0 {
		if err := channel.Qos(cfg.Prefetch, 0, false); err != nil {
			channel.Close()
			conn.Close()
			return nil, fmt.Errorf("failed to set qos: %w", err)
		}
	}

	return &ExchangeBroker{}, nil
}
