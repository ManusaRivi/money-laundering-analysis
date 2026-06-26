package clientconnection

import (
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/network"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
	"github.com/ManusaRivi/money-laundering-analysis/src/gateway/internal/messagehandler"
)

// ClientConnection owns a single client connection and the business logic for
// dispatching its inbound messages. For now it just consumes batches until both
// dataset EOFs arrive, then sends a result EOF back. Middleware routing will
// hook in here later.
type ClientConnection struct {
	ClientId      uuid.UUID
	Conn          network.Connection
	Handler       *messagehandler.MessageHandler
	codec         codec.Codec
	EOFSent       map[protocol.MsgType]bool
	done          chan struct{}
	closeDoneOnce sync.Once
}

func NewClientConnection(clientId uuid.UUID, conn net.Conn, codec codec.Codec) *ClientConnection {
	handler := messagehandler.NewMessageHandler()
	EOFSent := make(map[protocol.MsgType]bool)
	EOFSent[protocol.MsgQuery1ResultEOF] = false
	EOFSent[protocol.MsgQuery2ResultEOF] = false
	EOFSent[protocol.MsgQuery3ResultEOF] = false
	EOFSent[protocol.MsgQuery4ResultEOF] = false
	EOFSent[protocol.MsgQuery5ResultEOF] = false
	return &ClientConnection{
		ClientId: clientId,
		Conn:     network.NewConnection(conn),
		Handler:  &handler,
		codec:    codec,
		EOFSent:  EOFSent,
		done:     make(chan struct{}),
	}
}

func (c *ClientConnection) Done() <-chan struct{} {
	return c.done
}

func (c *ClientConnection) AllEOFSent() bool {
	return c.allEOFSent()
}

// Without decoding the actual payload, creates an external envelope with the given
// Msg type and forwards it to the client.
func (c *ClientConnection) ForwardEnvelope(envelope protocol.ExternalEnvelope) error {
	envelopeBytes, err := c.codec.EncodeExternalEnvelope(envelope)
	if err != nil {
		return fmt.Errorf("encoding envelope of type %v: %w", envelope.MsgType, err)
	}
	if err := c.Conn.Send(envelopeBytes); err != nil {
		return fmt.Errorf("forwarding envelope of type %v: %w", envelope.MsgType, err)
	}
	if _, isQueryEOF := c.EOFSent[envelope.MsgType]; isQueryEOF {
		c.EOFSent[envelope.MsgType] = true
	}
	// Assumption: no further result messages for a particular query will be received.
	if c.allEOFSent() {
		slog.Debug("All EOFs sent, closing client connection")
		c.closeDoneOnce.Do(func() {
			close(c.done)
		})
	}
	return nil
}

// Private methods

func (c *ClientConnection) allEOFSent() bool {
	for _, sent := range c.EOFSent {
		if !sent {
			return false
		}
	}
	return true
}
