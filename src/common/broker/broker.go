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
	RoutingKey KeyType // Para ruteo dinámico (opcional)
	Body       []byte
}

type ConnSettings struct {
	Hostname string
	Port     int
}

type Broker interface {

	//StartConsuming comienza a consumir desde el input configurado. Para tipos q-*
	//consume de la cola con nombre. Para tipos e-* consume de una cola anónima
	//bindeada al exchange de entrada usando input_keys.
	//callbackFunc recibe el mensaje y callbacks de ack/nack.
	//Si el broker se desconecta, devuelve ErrBrokerDisconnected.
	//Si ocurre un error interno, devuelve ErrBrokerMessage.
	StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error

	//StopConsuming detiene el consumo si está activo. Si no estaba consumiendo,
	//no tiene efecto. Si el broker se desconecta, devuelve ErrBrokerDisconnected.
	StopConsuming() error

	//Send publica un mensaje al output configurado. Para tipos *-q publica a cola.
	//Para tipos *-e publica a exchange usando routing key dinámica o output_keys.
	//Si el broker se desconecta, devuelve ErrBrokerDisconnected.
	//Si ocurre un error interno, devuelve ErrBrokerMessage.
	Send(msg Message) error

	//Close cierra la conexión del broker y libera recursos.
	//Si ocurre un error interno, devuelve ErrBrokerClose.
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

func bindInputQueue(channel *amqp.Channel, cfg config.BrokerConfig, routingKeys []KeyType, queueName string) error {
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

	inputKeysWithControlEOF := append(routingKeys, KeyControlEOF)

	for _, key := range inputKeysWithControlEOF {
		if err := channel.QueueBind(
			queueName,
			string(key),
			cfg.Input,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("failed to bind input queue: %w", err)
		}
	}

	return nil
}
