package broker

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrBrokerMessage      = errors.New("Broker: message error")
	ErrBrokerDisconnected = errors.New("Broker: disconnected")
	ErrBrokerClose        = errors.New("Broker: close error")
)

type Message struct {
	Body []byte
}

type ConnSettings struct {
	Hostname string
	Port     int
}

type Broker interface {

	//StartConsuming begins consuming from the configured input. For q-* types it
	//consumes the named queue. For e-* types it consumes from an anonymous queue
	//bound to the input exchange using input keys.
	//callbackFunc receives the message and ack/nack callbacks.
	//If the broker disconnects, returns ErrMessageBrokerDisconnected.
	//If an internal error occurs, returns ErrMessageBrokerMessage.
	StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error

	//StopConsuming stops consumption if active. If not consuming, it's a no-op.
	//If the broker disconnects, returns ErrMessageBrokerDisconnected.
	StopConsuming() error

	//Send publishes a message to the configured output.
	//For *-q types it publishes to a queue. For *-e types it publishes to an exchange
	//using output keys.
	//If the broker disconnects, returns ErrMessageBrokerDisconnected.
	//If an internal error occurs, returns ErrMessageBrokerMessage.
	Send(msg Message) error

	//Close closes the broker connection and releases resources.
	//If an internal error occurs, returns ErrMessageBrokerClose.
	Close() error
}

func NewBroker(cfg config.BrokerConfig) (Broker, error) {
	cfg = parseBrokerDefaults(cfg)
	switch cfg.Type {
	case TypeQueueToQueue:
		return newQueueToQueueBroker(cfg)
	case TypeQueueToExchange:
		return newQueueToExchangeBroker(cfg)
	case TypeExchangeToQueue:
		return newExchangeToQueueBroker(cfg)
	case TypeExchangeToExchange:
		return newExchangeToExchangeBroker(cfg)
	default:
		return nil, errors.New("unsupported broker type: " + cfg.Type)
	}
}

func parseBrokerDefaults(cfg config.BrokerConfig) config.BrokerConfig {
	if cfg.ExchangeType == "" {
		cfg.ExchangeType = "direct"
	}
	if cfg.Prefetch == 0 {
		cfg.Prefetch = 30
	}
	return cfg
}

func connectRabbit(rawURL string) (*amqp.Connection, *amqp.Channel, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid rabbitmq url: %w", err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, nil, fmt.Errorf("invalid rabbitmq url: %s", rawURL)
	}

	conn, err := amqp.Dial(rawURL)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to rabbitmq: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("failed to open channel: %w", err)
	}

	return conn, channel, nil
}


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
