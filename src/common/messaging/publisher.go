// Package messaging is the seam between the byte-level codec and the broker.
// Workers describe *what* to ship (msg type, client, routing key, payload) and
// Publisher owns the repetitive *how*: wrapping the payload in an internal
// envelope and putting it on the wire as a binary broker message. It also owns
// the inbound mirror: decoding the envelope once and dispatching by message
// type. The byte-level (de)serialisation still belongs to the embedded codec —
// Publisher only removes the transport boilerplate that used to be copied into
// every worker.
package messaging

import (
	"fmt"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
	"github.com/google/uuid"
)

// Handler processes a decoded inbound envelope. Workers register one per
// message type they accept.
type Handler func(envelope protocol.InternalEnvelope) error

// Publisher embeds the codec so workers keep a single messaging dependency:
// payload-level helpers (EncodeTransactionBatch, EncodeEOFCounts, ...) are
// promoted from the codec, while PublishInternal/Dispatch add the transport.
type Publisher struct {
	codec.Codec
	broker broker.Broker
	dedup  *dedupState
}

func New(c codec.Codec, b broker.Broker) *Publisher {
	return &Publisher{Codec: c, broker: b, dedup: newDedupState()}
}

// PublishInternal frames payload in an internal envelope (with a zero MsgID) and
// sends it on key. Prefer PublishInternalWithID on the fault-tolerant path so the
// message carries a deterministic id for deduplication.
func (p *Publisher) PublishInternal(clientID uuid.UUID, msgType protocol.MsgType, key broker.KeyType, payload []byte) error {
	return p.PublishInternalWithID(clientID, msgType, key, payload, protocol.MsgID{})
}

// PublishInternalWithID frames payload in an internal envelope stamped with id
// and sends it on key.
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

// PublishRaw sends an already-framed envelope on key as a binary message. Use
// it with the codec's Encode*Envelope helpers, which return a full envelope
// rather than a bare payload; use PublishInternal when you hold only a payload.
// Prefer PublishRawWithID on the fault-tolerant path.
func (p *Publisher) PublishRaw(key broker.KeyType, envelope []byte) error {
	return p.send(key, envelope)
}

// PublishRawWithID stamps id into the already-framed envelope (in place) and
// sends it on key.
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

// Dispatch decodes msg's internal envelope and routes it to the handler
// registered for its message type, erroring on an unregistered type.
func (p *Publisher) Dispatch(msg broker.Message, handlers map[protocol.MsgType]Handler) (uuid.UUID, protocol.MsgType, error) {
	envelope, err := p.DecodeInternalEnvelope(msg.Body)
	if err != nil {
		return uuid.Nil, 0, fmt.Errorf("decoding internal envelope: %w", err)
	}
	handler, ok := handlers[envelope.MsgType]
	if !ok {
		return uuid.Nil, envelope.MsgType, fmt.Errorf("unexpected inbound packet type: %v", envelope.MsgType)
	}

	deduped := envelope.MsgID != (protocol.MsgID{})
	if deduped && p.dedup.alreadySeen(envelope.ClientId, envelope.MsgID) {
		slog.Debug("Dropping already-seen message", "clientID", envelope.ClientId, "msgType", envelope.MsgType)
		return uuid.Nil, envelope.MsgType, nil
	}
	if err := handler(envelope); err != nil {
		return uuid.Nil, envelope.MsgType, err
	}
	if deduped {
		p.dedup.markSeen(envelope.ClientId, envelope.MsgID)
	}
	return envelope.ClientId, envelope.MsgType, nil
}

func (p *Publisher) Forget(clientID uuid.UUID) {
	p.dedup.forget(clientID)
}

func (p *Publisher) MarkSent(clientID uuid.UUID, key broker.KeyType, id protocol.MsgID) {
	p.dedup.markSent(clientID, key, id)
}

func (p *Publisher) GetSeen(clientID uuid.UUID) map[protocol.MsgID]struct{} {
	return p.dedup.getSeen(clientID)
}

func (p *Publisher) GetSent(clientID uuid.UUID) map[broker.KeyType]map[protocol.MsgID]struct{} {
	return p.dedup.getSent(clientID)
}

func (p *Publisher) SnapshotClient(clientID uuid.UUID) ([]byte, error) {
	return p.dedup.snapshotClient(clientID), nil
}

func (p *Publisher) RestoreClient(clientID uuid.UUID, data []byte) error {
	p.dedup.restoreClient(clientID, data)
	return nil
}
