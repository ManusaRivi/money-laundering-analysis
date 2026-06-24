package messaging

import (
	"fmt"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
	"github.com/google/uuid"
)

type Handler func(envelope protocol.InternalEnvelope) error

type Publisher struct {
	codec.Codec
	broker broker.Broker
	dedup  *dedupState
}

func New(c codec.Codec, b broker.Broker) *Publisher {
	return &Publisher{Codec: c, broker: b, dedup: newDedupState()}
}

func (p *Publisher) PublishInternal(clientID uuid.UUID, msgType protocol.MsgType, key broker.KeyType, payload []byte) error {
	return p.PublishInternalWithID(clientID, msgType, key, payload, protocol.MsgID{})
}

func (p *Publisher) PublishInternalWithID(clientID uuid.UUID, msgType protocol.MsgType, key broker.KeyType, payload []byte, id protocol.MsgID) error {
	envelope, err := p.EncodeInternalEnvelope(protocol.InternalEnvelope{
		MsgType:  msgType,
		ClientId: clientID,
		MsgID:    id,
		Payload:  payload,
	})
	if err != nil {
		return fmt.Errorf("encoding internal envelope: %w", err)
	}
	return p.send(key, envelope)
}

func (p *Publisher) PublishRaw(key broker.KeyType, envelope []byte) error {
	return p.send(key, envelope)
}

func (p *Publisher) PublishRawWithID(key broker.KeyType, envelope []byte, id protocol.MsgID) error {
	codec.SetEnvelopeMsgID(envelope, id)
	return p.send(key, envelope)
}

func (p *Publisher) send(key broker.KeyType, envelope []byte) error {
	if err := p.broker.Send(broker.Message{
		RoutingKey:  key,
		ContentType: broker.ContentTypeBinary,
		Body:        envelope,
	}); err != nil {
		return fmt.Errorf("sending message to broker: %w", err)
	}
	return nil
}

func (p *Publisher) Dispatch(msg broker.Message, handlers map[protocol.MsgType]Handler) (uuid.UUID, error) {
	envelope, err := p.DecodeInternalEnvelope(msg.Body)
	if err != nil {
		return uuid.Nil, fmt.Errorf("decoding internal envelope: %w", err)
	}
	handler, ok := handlers[envelope.MsgType]
	if !ok {
		return uuid.Nil, fmt.Errorf("unexpected inbound packet type: %v", envelope.MsgType)
	}

	deduped := envelope.MsgID != (protocol.MsgID{})
	if deduped && p.dedup.alreadySeen(envelope.ClientId, envelope.MsgID) {
		slog.Debug("Dropping already-seen message", "clientID", envelope.ClientId, "msgType", envelope.MsgType)
		return uuid.Nil, nil
	}
	if err := handler(envelope); err != nil {
		return uuid.Nil, err
	}
	if deduped {
		p.dedup.markSeen(envelope.ClientId, envelope.MsgID)
	}
	return envelope.ClientId, nil
}

func (p *Publisher) Forget(clientID uuid.UUID) {
	p.dedup.forget(clientID)
}

func (p *Publisher) DrainClient(clientID uuid.UUID) ([]byte, error) {
	return p.dedup.drainClient(clientID), nil
}

func (p *Publisher) CommitClient(clientID uuid.UUID) error {
	p.dedup.commitDrained(clientID)
	return nil
}

func (p *Publisher) ReplayClient(clientID uuid.UUID, record []byte) error {
	p.dedup.replayClient(clientID, record)
	return nil
}
