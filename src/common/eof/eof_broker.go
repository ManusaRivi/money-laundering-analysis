package eof

import (
	"fmt"
	"sync"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	amqp "github.com/rabbitmq/amqp091-go"
)

// EOFBroker implementa broker.Broker y enruta los mensajes de control
// entre broadcast (fanout) y envio directo por nodo.
type EOFBroker struct {
	conn *amqp.Connection
	ch   *amqp.Channel

	broadcastExchange string
	inputQueueName    string
	EOFPrefix         string

	mu          sync.Mutex
	consuming   bool
	consumeDone chan struct{}
	consumerTagInput string
}

const eofPrefetchCount = 3

// NewEOFBroker inicializa el broker compuesto para el EOF
func NewEOFBroker(rabbitURL string, broadcastExchange string, nodeID int, EOFPrefix string ) (*EOFBroker, error) {
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		return nil, fmt.Errorf("EOFBroker: dial rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("EOFBroker: open channel: %w", err)
	}

	if err := ch.Qos(eofPrefetchCount, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("EOFBroker: set qos: %w", err)
	}

	if err := ch.ExchangeDeclare(
		broadcastExchange,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("EOFBroker: declare broadcast exchange: %w", err)
	}

	queueName := fmt.Sprintf("%s_%d", EOFPrefix, nodeID)
	if _, err := ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("EOFBroker: declare input queue: %w", err)
	}

	if err := ch.QueueBind(
		queueName,
		"",
		broadcastExchange,
		false,
		nil,
	); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("EOFBroker: bind input queue: %w", err)
	}

	return &EOFBroker{
		conn:              conn,
		ch:                ch,
		broadcastExchange: broadcastExchange,
		inputQueueName:    queueName,
		EOFPrefix:         EOFPrefix,
	}, nil
}

func (eb *EOFBroker) StartConsuming(callbackFunc func(msg broker.Message, ack func(), nack func())) error {
	eb.mu.Lock()
	if eb.consuming {
		eb.mu.Unlock()
		return nil
	}
	inputTag := fmt.Sprintf("eof_input_%s", eb.inputQueueName)
	eb.consumerTagInput = inputTag
	eb.consumeDone = make(chan struct{})
	eb.consuming = true
	eb.mu.Unlock()

	inputDeliveries, err := eb.ch.Consume(
		eb.inputQueueName,
		inputTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		eb.setConsuming(false)
		return fmt.Errorf("EOFBroker: consume input queue: %w", err)
	}

	handle := func(deliveries <-chan amqp.Delivery) {
		for {
			select {
			case d, ok := <-deliveries:
				if !ok {
					return
				}
				msg := broker.Message{Body: d.Body}
				ack := func() { _ = d.Ack(false) }
				nack := func() { _ = d.Nack(false, true) }
				callbackFunc(msg, ack, nack)
			case <-eb.consumeDone:
				return
			}
		}
	}

	handle(inputDeliveries)
	return nil
}

func (eb *EOFBroker) StopConsuming() error {
	eb.mu.Lock()
	if !eb.consuming {
		eb.mu.Unlock()
		return nil
	}
	close(eb.consumeDone)
	inputTag := eb.consumerTagInput
	eb.consuming = false
	eb.mu.Unlock()

	if err := eb.ch.Cancel(inputTag, false); err != nil {
		return fmt.Errorf("EOFBroker: cancel input consumer: %w", err)
	}
	return nil
}

// Send inspecciona el mensaje para decidir si va por broadcast o directo
func (eb *EOFBroker) Send(msg broker.Message) error {
	ctrlMsg, err := UnmarshalControlMessage(msg)
	if err != nil {
		return fmt.Errorf("EOFBroker: fail to unmarshal control message: %w", err)
	}

	switch ctrlMsg.Type {
	case MsgTypeAmountResponse:
		queueName := fmt.Sprintf("%s_%d", eb.EOFPrefix, ctrlMsg.RequesterID)
		return eb.ch.Publish(
			"",
			queueName,
			false,
			false,
			amqp.Publishing{Body: msg.Body},
		)
	case MsgTypeFlushAck:
		queueName := fmt.Sprintf("%s_%d", eb.EOFPrefix, ctrlMsg.RequesterID)
		return eb.ch.Publish(
			"",
			queueName,
			false,
			false,
			amqp.Publishing{Body: msg.Body},
		)
	case MsgTypeAmountRequest, MsgTypeFlush, MsgTypeRetryExceeded:
		return eb.ch.Publish(
			eb.broadcastExchange,
			"",
			false,
			false,
			amqp.Publishing{Body: msg.Body},
		)
	default:
		return fmt.Errorf("EOFBroker: unsupported message type %s", ctrlMsg.Type)
	}
}

func (eb *EOFBroker) Close() error {
	var errs []error
	if err := eb.StopConsuming(); err != nil {
		errs = append(errs, err)
	}
	if eb.ch != nil {
		if err := eb.ch.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if eb.conn != nil {
		if err := eb.conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("EOFBroker: multiple close errors: %v", errs)
	}
	return nil
}

func (eb *EOFBroker) setConsuming(value bool) {
	eb.mu.Lock()
	eb.consuming = value
	eb.mu.Unlock()
}

