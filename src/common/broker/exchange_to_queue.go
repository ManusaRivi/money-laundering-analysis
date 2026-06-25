package broker

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	amqp "github.com/rabbitmq/amqp091-go"
)

type exchangeToQueueBroker struct {
	conn           *amqp.Connection
	produceChannel *amqp.Channel
	consumeChannel *amqp.Channel
	inputQueue     amqp.Queue
	outputQueue    string
	state          consumerState
	consumerTag    string
	mu             sync.Mutex
	publishMu      sync.Mutex
	config         config.BrokerConfig
}

func newExchangeToQueueBroker(cfg config.BrokerConfig) (Broker, error) {
	if cfg.Input == nil || cfg.Input.Exchange == nil || cfg.Input.Exchange.Name == "" {
		return nil, errors.New("input.exchange.name is required for e-q broker")
	}
	if cfg.Output == nil || cfg.Output.Queue == nil || cfg.Output.Queue.Name == "" {
		return nil, errors.New("output.queue.name is required for e-q broker")
	}
	if cfg.RabbitURL == "" {
		return nil, errors.New("url is required for e-q broker")
	}
	if len(cfg.Input.Exchange.Keys) == 0 && cfg.Input.Exchange.Type != "fanout" {
		return nil, errors.New("input.exchange.keys is required for e-q broker")
	}

	return buildExchangeToQueueBroker(cfg, cfg.RabbitURL)
}

func buildExchangeToQueueBroker(cfg config.BrokerConfig, rabbitURL string) (Broker, error) {
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
	if cfg.Input.Queue != nil && cfg.Input.Queue.Name != "" {
		queueName = cfg.Input.Queue.Name
	}
	isExclusive := cfg.Input.Queue != nil && cfg.Input.Queue.Exclusive != nil && *cfg.Input.Queue.Exclusive

	inputQueue, err := consumeChannel.QueueDeclare(
		queueName,
		*cfg.Input.Queue.Durable,
		*cfg.Input.Queue.AutoDelete,
		isExclusive,
		*cfg.Input.Queue.NoWait,
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

	return &exchangeToQueueBroker{
		conn:           conn,
		produceChannel: produceChannel,
		consumeChannel: consumeChannel,
		inputQueue:     inputQueue,
		outputQueue:    cfg.Output.Queue.Name,
		state:          idle,
		config:         cfg,
	}, nil
}

func (qb *exchangeToQueueBroker) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
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

func (qb *exchangeToQueueBroker) StopConsuming() error {
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

func (qb *exchangeToQueueBroker) Send(msg Message) error {
	qb.mu.Lock()
	if qb.state == closed {
		qb.mu.Unlock()
		return ErrBrokerMessage
	}
	qb.mu.Unlock()

	return publishMessage(&qb.publishMu, qb.produceChannel, persistentFromOutput(qb.config), "", qb.outputQueue, msg)
}

func (qb *exchangeToQueueBroker) Close() error {
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
