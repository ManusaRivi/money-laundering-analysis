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
}

func New(c codec.Codec, b broker.Broker) *Publisher {
	return &Publisher{Codec: c, broker: b}
}

// PublishInternal frames payload in an internal envelope and sends it on key.
func (p *Publisher) PublishInternal(clientID uuid.UUID, msgType protocol.MsgType, key broker.KeyType, payload []byte) error {
	envelope, err := p.EncodeInternalEnvelope(protocol.InternalEnvelope{
		MsgType:  msgType,
		ClientId: clientID,
		Payload:  payload,
	})
	if err != nil {
		return fmt.Errorf("encoding internal envelope: %w", err)
	}
	if err := p.broker.Send(broker.Message{
		RoutingKey:  key,
		ContentType: broker.ContentTypeBinary,
		Body:        envelope,
	}); err != nil {
		return fmt.Errorf("sending message to broker: %w", err)
	}
	return nil
}

// PublishRaw sends an already-framed envelope on key as a binary message. Use
// it with the codec's Encode*Envelope helpers, which return a full envelope
// rather than a bare payload; use PublishInternal when you hold only a payload.
func (p *Publisher) PublishRaw(key broker.KeyType, envelope []byte) error {
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
func (p *Publisher) Dispatch(msg broker.Message, handlers map[protocol.MsgType]Handler) error {
	envelope, err := p.DecodeInternalEnvelope(msg.Body)
	if err != nil {
		return fmt.Errorf("decoding internal envelope: %w", err)
	}
	handler, ok := handlers[envelope.MsgType]
	if !ok {
		return fmt.Errorf("unexpected inbound packet type: %v", envelope.MsgType)
	}
	return handler(envelope)
}
