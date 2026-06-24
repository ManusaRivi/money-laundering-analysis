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

type queueBroker struct {
	conn           *amqp.Connection
	produceChannel *amqp.Channel
	consumeChannel *amqp.Channel
	queue          amqp.Queue
	state          consumerState
	consumerTag    string
	mu             sync.Mutex
	publishMu      sync.Mutex
	config         config.BrokerConfig
}

func newQueueBroker(cfg config.BrokerConfig) (Broker, error) {
	var queueName string
	if cfg.Input != nil && cfg.Input.Queue != nil && cfg.Input.Queue.Name != "" {
		queueName = cfg.Input.Queue.Name
	} else if cfg.Output != nil && cfg.Output.Queue != nil && cfg.Output.Queue.Name != "" {
		queueName = cfg.Output.Queue.Name
	}
	if queueName == "" {
		return nil, errors.New("input.queue or output.queue is required for queue broker")
	}
	if cfg.RabbitURL == "" {
		return nil, errors.New("url is required for queue broker")
	}

	conn, consumeChannel, err := connectRabbit(cfg.RabbitURL)
	if err != nil {
		return nil, err
	}

	produceChannel, err := conn.Channel()
	if err != nil {
		consumeChannel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to open producer channel: %w", err)
	}

	if *cfg.Persistent {
		if err := produceChannel.Confirm(false); err != nil {
			produceChannel.Close()
			consumeChannel.Close()
			conn.Close()
			return nil, fmt.Errorf("failed to enable publisher confirms: %w", err)
		}
	}

	queue, err := consumeChannel.QueueDeclare(
		queueName,
		*cfg.Durable,
		*cfg.AutoDelete,
		*cfg.Exclusive,
		*cfg.NoWait,
		amqp.Table{},
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

	return &queueBroker{
		conn:           conn,
		produceChannel: produceChannel,
		consumeChannel: consumeChannel,
		queue:          queue,
		state:          idle,
		config:         cfg,
	}, nil
}

func (qb *queueBroker) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
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

	tag := qb.queue.Name + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	msgs, err := qb.consumeChannel.Consume(
		qb.queue.Name,
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

func (qb *queueBroker) StopConsuming() error {
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

func (qb *queueBroker) Send(msg Message) error {
	qb.mu.Lock()
	if qb.state == closed {
		qb.mu.Unlock()
		return ErrBrokerMessage
	}
	qb.mu.Unlock()

	return publishMessage(&qb.publishMu, qb.produceChannel, *qb.config.Persistent, "", qb.queue.Name, msg)
}

func (qb *queueBroker) Close() error {
	errStop := qb.StopConsuming()
	errConsume := qb.consumeChannel.Close()
	errProduce := qb.produceChannel.Close()
	errConn := qb.conn.Close()

	qb.mu.Lock()
	qb.state = closed
	qb.consumerTag = ""
	qb.mu.Unlock()

	if errStop != nil || errConsume != nil || errProduce != nil || errConn != nil {
		return ErrBrokerClose
	}
	return nil
}
