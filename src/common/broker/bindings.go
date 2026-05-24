package broker

import (
	"errors"
	"fmt"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	amqp "github.com/rabbitmq/amqp091-go"
)

func bindInputQueue(channel *amqp.Channel, cfg config.BrokerConfig, queueName string) error {
	if cfg.Input == "" {
		return nil
	}
	if len(cfg.InputKeys) == 0 {
		return errors.New("input_keys is required when input is exchange")
	}

	if err := channel.ExchangeDeclare(
		cfg.Input,
		cfg.ExchangeType,
		cfg.Durable,
		cfg.AutoDelete,
		cfg.Internal,
		cfg.NoWait,
		nil,
	); err != nil {
		return fmt.Errorf("failed to declare input exchange: %w", err)
	}

	for _, key := range cfg.InputKeys {
		if err := channel.QueueBind(
			queueName,
			key,
			cfg.Input,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("failed to bind input queue: %w", err)
		}
	}

	return nil
}
