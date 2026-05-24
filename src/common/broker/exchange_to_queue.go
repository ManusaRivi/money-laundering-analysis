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

type exchangeToQueueBroker struct {
	conn        *amqp.Connection
	channel     *amqp.Channel
	inputQueue  amqp.Queue
	outputQueue string
	state       consumerState
	consumerTag string
	mu          sync.Mutex
	config      config.BrokerConfig
}

func newExchangeToQueueBroker(cfg config.BrokerConfig) (Broker, error) {
	if cfg.Input == "" {
		return nil, errors.New("input is required for e-q broker")
	}
	if cfg.Output == "" {
		return nil, errors.New("output is required for e-q broker")
	}
	if cfg.RabbitURL == "" {
		return nil, errors.New("url is required for e-q broker")
	}
	if len(cfg.InputKeys) == 0 {
		return nil, errors.New("input_keys is required for e-q broker")
	}

	return buildExchangeToQueueBroker(cfg, cfg.RabbitURL)
}

func buildExchangeToQueueBroker(cfg config.BrokerConfig, rabbitURL string) (Broker, error) {
	conn, channel, err := connectRabbit(rabbitURL)
	if err != nil {
		return nil, err
	}

	if cfg.Prefetch == 0 {
		cfg.Prefetch = 30
	}

	inputQueue, err := channel.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare input queue: %w", err)
	}

	if err := bindInputQueue(channel, cfg, inputQueue.Name); err != nil {
		channel.Close()
		conn.Close()
		return nil, err
	}

	if cfg.Prefetch > 0 {
		if err := channel.Qos(cfg.Prefetch, 0, false); err != nil {
			channel.Close()
			conn.Close()
			return nil, fmt.Errorf("failed to set qos: %w", err)
		}
	}

	queueArgs := amqp.Table{}
	if cfg.Durable {
		queueArgs[amqp.QueueTypeArg] = amqp.QueueTypeQuorum
	}

	_, err = channel.QueueDeclare(
		cfg.Output,
		cfg.Durable,
		cfg.AutoDelete,
		cfg.Exclusive,
		cfg.NoWait,
		queueArgs,
	)
	if err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare output queue: %w", err)
	}

	return &exchangeToQueueBroker{
		conn:        conn,
		channel:     channel,
		inputQueue:  inputQueue,
		outputQueue: cfg.Output,
		state:       idle,
		config:      cfg,
	}, nil
}

func (qb *exchangeToQueueBroker) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
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

func (qb *exchangeToQueueBroker) StopConsuming() error {
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

func (qb *exchangeToQueueBroker) Send(msg Message) error {
	qb.mu.Lock()
	if qb.state == closed {
		qb.mu.Unlock()
		return ErrMessageBrokerMessage
	}
	qb.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := qb.channel.PublishWithContext(
		ctx,
		"",
		qb.outputQueue,
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
	return nil
}

func (qb *exchangeToQueueBroker) Close() error {
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
