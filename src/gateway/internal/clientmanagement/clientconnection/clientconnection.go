package clientconnection

import (
	"fmt"
	"log/slog"
	"net"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/network"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external/codec"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
	"github.com/ManusaRivi/money-laundering-analysis/src/gateway/internal/messagehandler"
)

// ClientConnection owns a single client connection and the business logic for
// dispatching its inbound messages. For now it just consumes batches until both
// dataset EOFs arrive, then sends a result EOF back. Middleware routing will
// hook in here later.
type ClientConnection struct {
	ClientId uuid.UUID
	Conn     network.Connection
	Handler  *messagehandler.MessageHandler
	codec    codec.Codec
	EOFSent  map[external.MsgType]bool
}

func NewClientConnection(clientId uuid.UUID, conn net.Conn, codec codec.Codec) *ClientConnection {
	handler := messagehandler.NewMessageHandler()
	EOFSent := make(map[external.MsgType]bool)
	EOFSent[external.MsgQuery1ResultEOF] = false
	EOFSent[external.MsgQuery2ResultEOF] = false
	EOFSent[external.MsgQuery3ResultEOF] = false
	EOFSent[external.MsgQuery4ResultEOF] = false
	EOFSent[external.MsgQuery5ResultEOF] = false
	return &ClientConnection{
		ClientId: clientId,
		Conn:     network.NewConnection(conn),
		Handler:  &handler,
		codec:    codec,
		EOFSent:  EOFSent,
	}
}

// Dispatches msg type and sends appropriate query result or EOF to client.
func (c *ClientConnection) HandleResponseMessage(pkt *inner.Packet) error {
	switch pkt.Type {
	case inner.TypeQuery1Result:
		result := &external.Query1Result{}
		if err := pkt.UnmarshalData(&result); err != nil {
			return fmt.Errorf("unmarshalling query 1 result: %w", err)
		}

		if err := c.sendQuery1Result(result); err != nil {
			return fmt.Errorf("sending query 1 result: %w", err)
		}
	case inner.TypeQuery1EOF:
		return c.flagAndSendQueryEOF(external.MsgQuery1ResultEOF)
	case inner.TypeQuery2Result:
		// Handle query 2 result response
	case inner.TypeQuery2EOF:
		return c.flagAndSendQueryEOF(external.MsgQuery2ResultEOF)
	case inner.TypeQuery3Result:
		// Handle query 3 result response
	case inner.TypeQuery3EOF:
		return c.flagAndSendQueryEOF(external.MsgQuery3ResultEOF)
	case inner.TypeQuery4Result:
		// Handle query 4 result response
	case inner.TypeQuery4EOF:
		return c.flagAndSendQueryEOF(external.MsgQuery4ResultEOF)
	case inner.TypeQuery5Result:
		// Handle query 5 result response
	case inner.TypeQuery5EOF:
		return c.flagAndSendQueryEOF(external.MsgQuery5ResultEOF)
	default:
		slog.Warn("Unknown packet type received", "type", pkt.Type)
	}
	return nil
}

// Private methods

func (c *ClientConnection) flagAndSendQueryEOF(msgType external.MsgType) error {
	if msgType != external.MsgQuery1ResultEOF && msgType != external.MsgQuery2ResultEOF && msgType != external.MsgQuery3ResultEOF && msgType != external.MsgQuery4ResultEOF && msgType != external.MsgQuery5ResultEOF {
		return fmt.Errorf("invalid EOF message type: %v", msgType)
	}
	c.EOFSent[msgType] = true
	return c.sendEnvelope(msgType, nil)
}

func (c *ClientConnection) sendEnvelope(msgType external.MsgType, payload []byte) error {
	envelope, err := c.codec.EncodeEnvelope(external.Envelope{
		MsgType: msgType,
		Payload: payload,
	})
	if err != nil {
		return fmt.Errorf("encoding envelope of type %v: %w", msgType, err)
	}
	if err := c.Conn.Send(envelope); err != nil {
		return fmt.Errorf("sending envelope of type %v: %w", msgType, err)
	}
	return nil
}

// Method for sending a query result received from middleware
func (c *ClientConnection) sendQuery1Result(result *external.Query1Result) error {
	payload, err := c.codec.EncodeQuery1ResultBatch([]external.Query1Result{(*result)})
	if err != nil {
		return fmt.Errorf("encoding query 1 result: %w", err)
	}
	return c.sendEnvelope(external.MsgQuery1Result, payload)
}

// Soon to be deprecated method
func (c *ClientConnection) sendQuery1Batch(transactions []external.Transaction) error {
	results := make([]external.Query1Result, len(transactions))
	for i, t := range transactions {
		results[i] = external.Query1Result{
			FromBank:    t.FromBank,
			FromAccount: t.FromAccount,
			ToBank:      t.ToBank,
			ToAccount:   t.ToAccount,
			AmountPaid:  t.AmountPaid,
		}
	}

	payload, err := c.codec.EncodeQuery1ResultBatch(results)
	if err != nil {
		return fmt.Errorf("encoding query 1 batch: %w", err)
	}
	return c.sendEnvelope(external.MsgQuery1Result, payload)
}
