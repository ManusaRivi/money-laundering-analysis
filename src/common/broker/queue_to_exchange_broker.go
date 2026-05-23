package broker

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueToExchangeBroker struct {
	conn           *amqp.Connection
	channel        *amqp.Channel
	inputQueue     amqp.Queue
	outputExchange string
	routingKeys    []string
	state          consumerState
	consumerTag    string
	mu             sync.Mutex
	config         config.BrokerConfig
}

func NewQueueToExchangeBroker(cfg config.BrokerConfig) (Broker, error) {
	if cfg.InputQueue == "" {
		return nil, errors.New("input_queue is required for q_e broker")
	}
	if cfg.OutputExchange == "" {
		return nil, errors.New("output_exchange is required for q_e broker")
	}
	if cfg.RabbitURL == "" {
		return nil, errors.New("url is required for q_e broker")
	}
	if len(cfg.RoutingKeys) == 0 {
		return nil, errors.New("routing_keys is required for q_e broker")
	}
	if cfg.ExchangeType == "" {
		cfg.ExchangeType = "direct"
	}

	return createQueueToExchangeBroker(cfg, cfg.RabbitURL)
}

func createQueueToExchangeBroker(cfg config.BrokerConfig, rabbitURL string) (Broker, error) {
	conn, channel, err := connectRabbit(rabbitURL)
	if err != nil {
		return nil, err
	}

	if cfg.Prefetch == 0 {
		cfg.Prefetch = 30
	}

	queueArgs := amqp.Table{}
	if cfg.Durable {
		queueArgs[amqp.QueueTypeArg] = amqp.QueueTypeQuorum
	}

	inputQueue, err := channel.QueueDeclare(
		cfg.InputQueue,
		cfg.Durable,
		cfg.AutoDelete,
		cfg.Exclusive,
		cfg.NoWait,
		queueArgs,
	)
	if err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	if cfg.OutputExchange == "" {
		channel.Close()
		conn.Close()
		return nil, errors.New("output_exchange is required for q_e broker")
	}

	if err := channel.ExchangeDeclare(
		cfg.OutputExchange,
		cfg.ExchangeType,
		cfg.Durable,
		cfg.AutoDelete,
		cfg.Internal,
		cfg.NoWait,
		nil,
	); err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare exchange: %w", err)
	}

	if cfg.Prefetch > 0 {
		if err := channel.Qos(cfg.Prefetch, 0, false); err != nil {
			channel.Close()
			conn.Close()
			return nil, fmt.Errorf("failed to set qos: %w", err)
		}
	}

	return &QueueToExchangeBroker{
		conn:           conn,
		channel:        channel,
		inputQueue:     inputQueue,
		outputExchange: cfg.OutputExchange,
		routingKeys:    cfg.RoutingKeys,
		state:          idle,
		config:         cfg,
	}, nil
}

func (qb *QueueToExchangeBroker) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	qb.mu.Lock()
	if qb.state == closed {
		qb.mu.Unlock()
		return ErrMessageBrokerMessage
	}
	if qb.state == consuming {
		qb.mu.Unlock()
		return nil
	}
	qb.mu.Unlock()

	queueName := qb.inputQueue.Name
	tag := queueName + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	msgs, err := qb.channel.Consume(
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
			return ErrMessageBrokerDisconnected
		}
		return ErrMessageBrokerMessage
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
		return ErrMessageBrokerDisconnected
	}

	return nil
}

func (qb *QueueToExchangeBroker) StopConsuming() error {
	qb.mu.Lock()
	if qb.state != consuming {
		qb.mu.Unlock()
		return nil
	}
	consumerTag := qb.consumerTag
	qb.mu.Unlock()

	if err := qb.channel.Cancel(consumerTag, false); err != nil {
		return ErrMessageBrokerDisconnected
	}

	qb.mu.Lock()
	qb.state = idle
	qb.consumerTag = ""
	qb.mu.Unlock()
	return nil
}

func (qb *QueueToExchangeBroker) Send(msg Message) error {
	qb.mu.Lock()
	if qb.state == closed {
		qb.mu.Unlock()
		return ErrMessageBrokerMessage
	}
	qb.mu.Unlock()

	if len(qb.routingKeys) == 0 {
		return ErrMessageBrokerMessage
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, key := range qb.routingKeys {
		if err := qb.channel.PublishWithContext(
			ctx,
			qb.outputExchange,
			key,
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        msg.Body,
			},
		); err != nil {
			if errors.Is(err, amqp.ErrClosed) {
				return ErrMessageBrokerDisconnected
			}
			return ErrMessageBrokerMessage
		}
	}
	return nil
}

func (qb *QueueToExchangeBroker) Close() error {
	errStop := qb.StopConsuming()
	errChannel := qb.channel.Close()
	errConn := qb.conn.Close()

	qb.mu.Lock()
	qb.state = closed
	qb.consumerTag = ""
	qb.mu.Unlock()

	if errStop != nil || errChannel != nil || errConn != nil {
		return ErrMessageBrokerClose
	}
	return nil
}
