package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	amqp "github.com/rabbitmq/amqp091-go"
)

// queueToExchangeBroker consumes from a named queue and publishes to an
// exchange with one or more routing keys. It owns a single AMQP connection
// but separates the consumer and publisher onto two dedicated channels so
// that publisher flow control cannot stall consumer acks, and a channel-level
// error on one side does not tear down the other.
type queueToExchangeBroker struct {
	conn           *amqp.Connection
	produceChannel *amqp.Channel
	consumeChannel *amqp.Channel
	inputQueue     amqp.Queue
	outputExchange string
	state          consumerState
	consumerTag    string
	mu             sync.Mutex
	config         config.BrokerConfig
}

func newQueueToExchangeBroker(cfg config.BrokerConfig) (Broker, error) {
	if cfg.Input == "" {
		return nil, errors.New("input is required for q-e broker")
	}
	if cfg.Output == "" {
		return nil, errors.New("output is required for q-e broker")
	}
	if cfg.RabbitURL == "" {
		return nil, errors.New("url is required for q-e broker")
	}
	if cfg.ExchangeType == "" {
		cfg.ExchangeType = "direct"
	}

	return buildQueueToExchangeBroker(cfg, cfg.RabbitURL)
}

func buildQueueToExchangeBroker(cfg config.BrokerConfig, rabbitURL string) (Broker, error) {
	conn, consumeChannel, err := connectRabbit(rabbitURL)
	if err != nil {
		return nil, err
	}

	produceChannel, err := conn.Channel()
	if err != nil {
		consumeChannel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to open producer channel: %w", err)
	}

	if cfg.Prefetch == 0 {
		cfg.Prefetch = 30
	}

	queueArgs := amqp.Table{}

	inputQueue, err := consumeChannel.QueueDeclare(
		cfg.Input,
		cfg.Durable,
		cfg.AutoDelete,
		cfg.Exclusive,
		cfg.NoWait,
		queueArgs,
	)
	if err != nil {
		produceChannel.Close()
		consumeChannel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	if cfg.Prefetch > 0 {
		if err := consumeChannel.Qos(cfg.Prefetch, 0, false); err != nil {
			produceChannel.Close()
			consumeChannel.Close()
			conn.Close()
			return nil, fmt.Errorf("failed to set qos: %w", err)
		}
	}

	if err := consumeChannel.ExchangeDeclare(
		cfg.Output,
		cfg.ExchangeType,
		cfg.Durable,
		cfg.AutoDelete,
		cfg.Internal,
		cfg.NoWait,
		nil,
	); err != nil {
		produceChannel.Close()
		consumeChannel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare exchange: %w", err)
	}

	return &queueToExchangeBroker{
		conn:           conn,
		produceChannel: produceChannel,
		consumeChannel: consumeChannel,
		inputQueue:     inputQueue,
		outputExchange: cfg.Output,
		state:          idle,
		config:         cfg,
	}, nil
}

func (qb *queueToExchangeBroker) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	qb.mu.Lock()
	if qb.state == closed {
		qb.mu.Unlock()
		return ErrBrokerMessage
	}
	if qb.state == consuming {
		qb.mu.Unlock()
		return nil
	}
	qb.mu.Unlock()

	queueName := qb.inputQueue.Name
	tag := queueName + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	msgs, err := qb.consumeChannel.Consume(
		queueName,
		tag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return ErrBrokerDisconnected
		}
		return ErrBrokerMessage
	}

	qb.mu.Lock()
	qb.consumerTag = tag
	qb.state = consuming
	qb.mu.Unlock()

	for d := range msgs {
		callbackFunc(Message{Body: d.Body}, func() { d.Ack(false) }, func() { d.Nack(false, true) })
	}

	qb.mu.Lock()
	defer qb.mu.Unlock()
	if qb.state == consuming {
		qb.state = closed
		return ErrBrokerDisconnected
	}

	return nil
}

func (qb *queueToExchangeBroker) StopConsuming() error {
	qb.mu.Lock()
	if qb.state != consuming {
		qb.mu.Unlock()
		return nil
	}
	consumerTag := qb.consumerTag
	qb.mu.Unlock()

	if err := qb.consumeChannel.Cancel(consumerTag, false); err != nil {
		return ErrBrokerDisconnected
	}

	qb.mu.Lock()
	qb.state = idle
	qb.consumerTag = ""
	qb.mu.Unlock()
	return nil
}

func (qb *queueToExchangeBroker) Send(msg Message) error {
	qb.mu.Lock()
	if qb.state == closed {
		qb.mu.Unlock()
		return ErrBrokerMessage
	}
	qb.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if msg.RoutingKey == KeyNil {
		slog.Error("Message missing routing key", "message", msg)
		return ErrBrokerMessage
	}

	if err := qb.produceChannel.PublishWithContext(
		ctx,
		qb.outputExchange,
		string(msg.RoutingKey),
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        msg.Body,
		},
	); err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return ErrBrokerDisconnected
		}
		return ErrBrokerMessage
	}
	return nil
}

func (qb *queueToExchangeBroker) Close() error {
	errStop := qb.StopConsuming()
	errConsumeChannel := qb.consumeChannel.Close()
	errProduceChannel := qb.produceChannel.Close()
	errConn := qb.conn.Close()

	qb.mu.Lock()
	qb.state = closed
	qb.consumerTag = ""
	qb.mu.Unlock()

	if errStop != nil || errConsumeChannel != nil || errProduceChannel != nil || errConn != nil {
		return ErrBrokerClose
	}
	return nil
}
