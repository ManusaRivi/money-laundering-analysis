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
	ContentTypeJSON   = "application/json"
	ContentTypeBinary = "application/octet-stream"
)

type Message struct {
	RoutingKey  KeyType
	Body        []byte
	ContentType string
}

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
	StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error
	StopConsuming() error
	Send(msg Message) error
	Close() error
}

func NewBroker(cfg config.BrokerConfig) (Broker, error) {
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
	if cfg.Input == nil || cfg.Input.Exchange == nil || cfg.Input.Exchange.Name == "" {
		return nil
	}

	if len(cfg.Input.Exchange.Keys) == 0 && cfg.Input.Exchange.Type != "fanout" {
		return errors.New("exchange keys are required when input is exchange")
	}

	if err := channel.ExchangeDeclare(
		cfg.Input.Exchange.Name,
		cfg.Input.Exchange.Type,
		*cfg.Input.Exchange.Durable,
		*cfg.Input.Exchange.AutoDelete,
		*cfg.Input.Exchange.Internal,
		*cfg.Input.Exchange.NoWait,
		nil,
	); err != nil {
		return fmt.Errorf("failed to declare input exchange: %w", err)
	}

	inputKeysWithControlEOF := append(routingKeys, KeyControlEOF)

	for _, key := range inputKeysWithControlEOF {
		if err := channel.QueueBind(
			queueName,
			string(key),
			cfg.Input.Exchange.Name,
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

func persistentFromOutput(cfg config.BrokerConfig) bool {
	if cfg.Output != nil && cfg.Output.Queue != nil && cfg.Output.Queue.Persistent != nil {
		return *cfg.Output.Queue.Persistent
	}
	if cfg.Output != nil && cfg.Output.Exchange != nil && cfg.Output.Exchange.Persistent != nil {
		return *cfg.Output.Exchange.Persistent
	}
	if cfg.Input != nil && cfg.Input.Queue != nil && cfg.Input.Queue.Persistent != nil {
		return *cfg.Input.Queue.Persistent
	}
	return false
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
