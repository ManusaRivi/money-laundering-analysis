package scattergather

import (
	"fmt"
	"sync"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// heavyBatchSize caps how many accounts go in one heavy-batch message, so a
// replica with many heavy accounts never publishes one oversized message.
const heavyBatchSize = 1000

const heavyExchangePrefetch = 16

// HeavyAccountsExchange is the all-to-all fanout among ScatterAndGather replicas
// used for the Q4 degree exchange. Each replica publishes its heavy sources/sinks
// (batched, binary-encoded, tagged with its node id) plus a "done" marker, and
// consumes every replica's publishes from its own queue bound to the fanout.
type HeavyAccountsExchange struct {
	conn  *amqp.Connection
	ch    *amqp.Channel
	codec codec.Codec

	exchangeName string
	queueName    string
	nodeID       int

	mu          sync.Mutex
	consuming   bool
	consumeDone chan struct{}
	consumerTag string
}

func NewHeavyAccountsExchange(rabbitURL, exchangeName string, nodeID int) (*HeavyAccountsExchange, error) {
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		return nil, fmt.Errorf("HeavyAccountsExchange: dial rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("HeavyAccountsExchange: open channel: %w", err)
	}

	if err := ch.Qos(heavyExchangePrefetch, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("HeavyAccountsExchange: set qos: %w", err)
	}

	if err := ch.ExchangeDeclare(exchangeName, "fanout", true, false, false, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("HeavyAccountsExchange: declare exchange: %w", err)
	}

	queueName := fmt.Sprintf("%s_%d", exchangeName, nodeID)
	if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("HeavyAccountsExchange: declare input queue: %w", err)
	}

	if err := ch.QueueBind(queueName, "", exchangeName, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("HeavyAccountsExchange: bind input queue: %w", err)
	}

	return &HeavyAccountsExchange{
		conn:         conn,
		ch:           ch,
		codec:        codec.New(),
		exchangeName: exchangeName,
		queueName:    queueName,
		nodeID:       nodeID,
	}, nil
}

// PublishHeavy broadcasts this replica's heavy sets for a client — sources and
// sinks in bounded batches — followed by a done marker. The done is published
// last, so (AMQP preserving per-publisher order) peers merge all batches before
// they see this replica's done.
func (e *HeavyAccountsExchange) PublishHeavy(clientID uuid.UUID, heavySrcs, heavySinks map[domain.Account]struct{}) error {
	if err := e.publishBatches(clientID, protocol.Q4HeavyRoleSource, heavySrcs); err != nil {
		return err
	}
	if err := e.publishBatches(clientID, protocol.Q4HeavyRoleSink, heavySinks); err != nil {
		return err
	}
	done, err := e.codec.EncodeQ4HeavyDoneEnvelope(clientID, e.nodeID)
	if err != nil {
		return fmt.Errorf("HeavyAccountsExchange: encode done: %w", err)
	}
	return e.publish(done)
}

func (e *HeavyAccountsExchange) publishBatches(clientID uuid.UUID, role uint8, accounts map[domain.Account]struct{}) error {
	batch := make([]domain.Account, 0, heavyBatchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		env, err := e.codec.EncodeQ4HeavyBatchEnvelope(clientID, e.nodeID, role, batch)
		if err != nil {
			return fmt.Errorf("HeavyAccountsExchange: encode heavy batch: %w", err)
		}
		if err := e.publish(env); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}
	for acc := range accounts {
		batch = append(batch, acc)
		if len(batch) >= heavyBatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func (e *HeavyAccountsExchange) publish(body []byte) error {
	if err := e.ch.Publish(e.exchangeName, "", false, false, amqp.Publishing{
		ContentType: broker.ContentTypeBinary,
		Body:        body,
	}); err != nil {
		return fmt.Errorf("HeavyAccountsExchange: publish: %w", err)
	}
	return nil
}

// StartConsuming blocks, delivering each peer message to onBatch / onDone until
// StopConsuming/Close is called. Messages are acked after the callback runs;
// since merges are idempotent (set unions, done dedup by sender), at-least-once
// redelivery is safe.
func (e *HeavyAccountsExchange) StartConsuming(
	onBatch func(clientID uuid.UUID, senderID int, role uint8, accounts []domain.Account, ack, nack func()),
	onDone func(clientID uuid.UUID, senderID int, ack, nack func()),
) error {
	e.mu.Lock()
	if e.consuming {
		e.mu.Unlock()
		return nil
	}
	tag := fmt.Sprintf("heavy_%s", e.queueName)
	e.consumerTag = tag
	e.consumeDone = make(chan struct{})
	e.consuming = true
	e.mu.Unlock()

	deliveries, err := e.ch.Consume(e.queueName, tag, false, false, false, false, nil)
	if err != nil {
		e.mu.Lock()
		e.consuming = false
		e.mu.Unlock()
		return fmt.Errorf("HeavyAccountsExchange: consume: %w", err)
	}

	for {
		select {
		case d, ok := <-deliveries:
			if !ok {
				return nil
			}
			e.dispatch(d.Body, onBatch, onDone, d.Ack(), d.Nack(false, true))
		case <-e.consumeDone:
			return nil
		}
	}
}

func (e *HeavyAccountsExchange) dispatch(
	body []byte,
	onBatch func(uuid.UUID, int, uint8, []domain.Account, ack, nack func()),
	onDone func(uuid.UUID, int, ack, nack func()),
) {
	envelope, err := e.codec.DecodeInternalEnvelope(body)
	if err != nil {
		return
	}
	switch envelope.MsgType {
	case protocol.MsgQ4HeavyBatch:
		senderID, role, accounts, err := e.codec.DecodeQ4HeavyBatch(envelope.Payload)
		if err != nil {
			return
		}
		onBatch(envelope.ClientId, senderID, role, accounts)
	case protocol.MsgQ4HeavyDone:
		senderID, err := e.codec.DecodeQ4HeavyDone(envelope.Payload)
		if err != nil {
			return
		}
		onDone(envelope.ClientId, senderID)
	}
}

func (e *HeavyAccountsExchange) StopConsuming() error {
	e.mu.Lock()
	if !e.consuming {
		e.mu.Unlock()
		return nil
	}
	close(e.consumeDone)
	tag := e.consumerTag
	e.consuming = false
	e.mu.Unlock()

	if err := e.ch.Cancel(tag, false); err != nil {
		return fmt.Errorf("HeavyAccountsExchange: cancel consumer: %w", err)
	}
	return nil
}

func (e *HeavyAccountsExchange) Close() {
	_ = e.StopConsuming()
	if e.ch != nil {
		e.ch.Close()
	}
	if e.conn != nil {
		e.conn.Close()
	}
}
