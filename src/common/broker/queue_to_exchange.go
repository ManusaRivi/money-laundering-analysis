package broker

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	amqp "github.com/rabbitmq/amqp091-go"
)

type queueToExchangeBroker struct {
	conn           *amqp.Connection
	produceChannel *amqp.Channel
	consumeChannel *amqp.Channel
	inputQueue     amqp.Queue
	outputExchange string
	state          consumerState
	consumerTag    string
	mu             sync.Mutex
	publishMu      sync.Mutex
	config         config.BrokerConfig
	consumeDone    chan struct{}
}

func newQueueToExchangeBroker(cfg config.BrokerConfig) (Broker, error) {
	if cfg.Input == nil || cfg.Input.Queue == nil || cfg.Input.Queue.Name == "" {
		return nil, errors.New("input.queue.name is required for q-e broker")
	}
	if cfg.Output == nil || cfg.Output.Exchange == nil || cfg.Output.Exchange.Name == "" {
		return nil, errors.New("output.exchange.name is required for q-e broker")
	}
	if cfg.RabbitURL == "" {
		return nil, errors.New("url is required for q-e broker")
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

	if persistent := persistentFromOutput(cfg); persistent {
		if err := produceChannel.Confirm(false); err != nil {
			produceChannel.Close()
			consumeChannel.Close()
			conn.Close()
			return nil, fmt.Errorf("failed to enable publisher confirms: %w", err)
		}
	}

	queueArgs := amqp.Table{}

	inputQueue, err := consumeChannel.QueueDeclare(
		cfg.Input.Queue.Name,
		*cfg.Input.Queue.Durable,
		*cfg.Input.Queue.AutoDelete,
		*cfg.Input.Queue.Exclusive,
		*cfg.Input.Queue.NoWait,
		queueArgs,
	)
	if err != nil {
		produceChannel.Close()
		consumeChannel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	if cfg.Input.Queue.Prefetch > 0 {
		if err := consumeChannel.Qos(cfg.Input.Queue.Prefetch, 0, false); err != nil {
			produceChannel.Close()
			consumeChannel.Close()
			conn.Close()
			return nil, fmt.Errorf("failed to set qos: %w", err)
		}
	}

	if err := consumeChannel.ExchangeDeclare(
		cfg.Output.Exchange.Name,
		cfg.Output.Exchange.Type,
		*cfg.Output.Exchange.Durable,
		*cfg.Output.Exchange.AutoDelete,
		*cfg.Output.Exchange.Internal,
		*cfg.Output.Exchange.NoWait,
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
		outputExchange: cfg.Output.Exchange.Name,
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
	qb.consumeDone = make(chan struct{})
	qb.mu.Unlock()

	for {
		select {
		case d, ok := <-msgs:
			if !ok {
				qb.mu.Lock()
				if qb.state == consuming {
					qb.state = closed
				}
				qb.mu.Unlock()
				return ErrBrokerDisconnected
			}
			callbackFunc(Message{Body: d.Body, ContentType: d.ContentType}, func() { d.Ack(false) }, func() { d.Nack(false, true) })
		case <-qb.consumeDone:
			return nil
		}
	}
}

func (qb *queueToExchangeBroker) StopConsuming() error {
	qb.mu.Lock()
	if qb.state != consuming {
		qb.mu.Unlock()
		return nil
	}
	consumerTag := qb.consumerTag
	close(qb.consumeDone)
	qb.state = idle
	qb.consumerTag = ""
	qb.mu.Unlock()

	if err := qb.consumeChannel.Cancel(consumerTag, false); err != nil {
		return ErrBrokerDisconnected
	}

	return nil
}

func (qb *queueToExchangeBroker) Send(msg Message) error {
	qb.mu.Lock()
	if qb.state == closed {
		qb.mu.Unlock()
		return ErrBrokerMessage
	}
	qb.mu.Unlock()

	if msg.RoutingKey == KeyNil && qb.config.Output.Exchange.Type != "fanout" {
		slog.Error("Message missing routing key", "message", msg)
		return ErrBrokerMessage
	}

	return publishMessage(&qb.publishMu, qb.produceChannel, persistentFromOutput(qb.config), qb.outputExchange, string(msg.RoutingKey), msg)
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
