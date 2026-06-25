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

type exchangeToExchangeBroker struct {
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
}

func newExchangeToExchangeBroker(cfg config.BrokerConfig) (Broker, error) {
	if cfg.Input == nil || cfg.Input.Exchange == nil || cfg.Input.Exchange.Name == "" {
		return nil, errors.New("input.exchange.name is required for e-e broker")
	}
	if cfg.Output == nil || cfg.Output.Exchange == nil || cfg.Output.Exchange.Name == "" {
		return nil, errors.New("output.exchange.name is required for e-e broker")
	}
	if cfg.RabbitURL == "" {
		return nil, errors.New("url is required for e-e broker")
	}
	if len(cfg.Input.Exchange.Keys) == 0 {
		return nil, errors.New("input.exchange.keys is required for e-e broker")
	}
	return buildExchangeToExchangeBroker(cfg, cfg.RabbitURL)
}

func buildExchangeToExchangeBroker(cfg config.BrokerConfig, rabbitURL string) (Broker, error) {
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
	
	var queueName string
	hasNamedQueue := cfg.Input.Queue != nil && cfg.Input.Queue.Name != ""
	if hasNamedQueue {
		queueName = cfg.Input.Queue.Name
	}

	exclusive := !hasNamedQueue
	if hasNamedQueue && cfg.Input.Queue.Exclusive != nil {
		exclusive = *cfg.Input.Queue.Exclusive
	}

	inputQueue, err := consumeChannel.QueueDeclare(
		queueName,
		cfg.Input.Queue.Durable != nil && *cfg.Input.Queue.Durable,
		cfg.Input.Queue.AutoDelete != nil && *cfg.Input.Queue.AutoDelete,
		exclusive,
		cfg.Input.Queue.NoWait != nil && *cfg.Input.Queue.NoWait,
		nil,
	)
	if err != nil {
		produceChannel.Close()
		consumeChannel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare input queue: %w", err)
	}

	routingKeys := StringsToKeyType(cfg.Input.Exchange.Keys)

	if err := bindInputQueue(consumeChannel, cfg, routingKeys, inputQueue.Name); err != nil {
		produceChannel.Close()
		consumeChannel.Close()
		conn.Close()
		return nil, err
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
		return nil, fmt.Errorf("failed to declare output exchange: %w", err)
	}

	return &exchangeToExchangeBroker{
		conn:           conn,
		produceChannel: produceChannel,
		consumeChannel: consumeChannel,
		inputQueue:     inputQueue,
		outputExchange: cfg.Output.Exchange.Name,
		state:          idle,
		config:         cfg,
	}, nil
}

func (qb *exchangeToExchangeBroker) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
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
		callbackFunc(Message{Body: d.Body, ContentType: d.ContentType}, func() { d.Ack(false) }, func() { d.Nack(false, true) })
	}

	qb.mu.Lock()
	defer qb.mu.Unlock()
	if qb.state == consuming {
		qb.state = closed
		return ErrBrokerDisconnected
	}

	return nil
}

func (qb *exchangeToExchangeBroker) StopConsuming() error {
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

func (qb *exchangeToExchangeBroker) Send(msg Message) error {
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

func (qb *exchangeToExchangeBroker) Close() error {
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
