package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	connectMaxAttempts = 10
	connectBaseDelay   = 500 * time.Millisecond
	connectMaxDelay    = 5 * time.Second

	backoffExponent = 2
)

var (
	ErrBrokerMessage      = errors.New("Broker: message error")
	ErrBrokerDisconnected = errors.New("Broker: disconnected")
	ErrBrokerClose        = errors.New("Broker: close error")
)

const (
	// ContentTypeJSON marca mensajes del protocolo inner legado (JSON).
	ContentTypeJSON = "application/json"
	// ContentTypeBinary marca mensajes con framing binario
	// ([16B client UUID][external envelope]) que el gateway reenvía sin decodificar.
	ContentTypeBinary = "application/octet-stream"
)

type Message struct {
	RoutingKey  KeyType // Para ruteo dinámico (opcional)
	Body        []byte
	ContentType string // Vacío => ContentTypeJSON (legado)
}

// contentTypeOrDefault resuelve el content type AMQP a publicar, manteniendo
// retrocompatibilidad con los productores que no setean el campo.
func (m Message) contentTypeOrDefault() string {
	if m.ContentType == "" {
		return ContentTypeJSON
	}
	return m.ContentType
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
	case TypeQueue:
		return newQueueBroker(cfg)
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

	var conn *amqp.Connection
	delay := connectBaseDelay
	for attempt := 1; ; attempt++ {
		conn, err = amqp.Dial(rawURL)
		if err == nil {
			break
		}
		if attempt >= connectMaxAttempts {
			return nil, nil, fmt.Errorf("failed to connect to rabbitmq after %d attempts: %w", attempt, err)
		}
		slog.Warn("rabbitmq dial failed, retrying", "attempt", attempt, "delay", delay, "err", err)
		time.Sleep(delay)
		if delay < connectMaxDelay {
			delay *= backoffExponent
			if delay > connectMaxDelay {
				delay = connectMaxDelay
			}
		}
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

func classifyPublishErr(err error) error {
	if errors.Is(err, amqp.ErrClosed) {
		return ErrBrokerDisconnected
	}
	return ErrBrokerMessage
}

func publishMessage(mu *sync.Mutex, ch *amqp.Channel, persistent bool, exchange, routingKey string, msg Message) error {
	mu.Lock()
	defer mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	publishing := amqp.Publishing{
		ContentType: msg.contentTypeOrDefault(),
		Body:        msg.Body,
	}

	if !persistent {
		if err := ch.PublishWithContext(ctx, exchange, routingKey, false, false, publishing); err != nil {
			return classifyPublishErr(err)
		}
		return nil
	}

	publishing.DeliveryMode = amqp.Persistent
	dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, false, false, publishing)
	if err != nil {
		return classifyPublishErr(err)
	}
	select {
	case <-dc.Done():
		if !dc.Acked() {
			return ErrBrokerMessage
		}
		return nil
	case <-ctx.Done():
		return ErrBrokerDisconnected
	}
}
