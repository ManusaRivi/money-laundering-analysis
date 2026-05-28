package clientconnection

import (
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
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
	ClientId      uuid.UUID
	Conn          network.Connection
	Handler       *messagehandler.MessageHandler
	codec         codec.Codec
	EOFSent       map[external.MsgType]bool
	done          chan struct{}
	closeDoneOnce sync.Once
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
		done:     make(chan struct{}),
	}
}

func (c *ClientConnection) Done() <-chan struct{} {
	return c.done
}

// Dispatches msg type and sends appropriate query result or EOF to client.
func (c *ClientConnection) HandleResponseMessage(pkt *inner.Packet) error {
	switch pkt.Type {
	case inner.TypeQuery1Result:
		var result domain.Query1Result
		if err := pkt.UnmarshalData(&result); err != nil {
			return fmt.Errorf("unmarshalling query 1 result: %w", err)
		}

		externalResult := external.Query1Result{
			FromBank:    result.FromBank,
			FromAccount: result.FromAccount,
			ToBank:      result.ToBank,
			ToAccount:   result.ToAccount,
			AmountPaid:  result.AmountPaid,
		}
		slog.Debug("Received Query Result", "amount_paid", externalResult.AmountPaid)

		if err := c.sendQuery1Result(&externalResult); err != nil {
			return fmt.Errorf("sending query 1 result: %w", err)
		}
	case inner.TypeQuery1EOF:
		return c.flagAndSendQueryEOF(external.MsgQuery1ResultEOF)
	case inner.TypeQuery2Result:
		var result domain.Query2Result
		if err := pkt.UnmarshalData(&result); err != nil {
			return fmt.Errorf("unmarshalling query 2 result: %w", err)
		}

		externalResult := external.Query2Result{
			FromBank:    result.FromBank,
			FromAccount: result.FromAccount,
			BankName:    result.BankName,
			AmountPaid:  result.AmountPaid,
		}
		slog.Debug("Received Query Result", "amount_paid", externalResult.AmountPaid)

		if err := c.sendQuery2Result(&externalResult); err != nil {
			return fmt.Errorf("sending query 2 result: %w", err)
		}
	case inner.TypeQuery2EOF:
		return c.flagAndSendQueryEOF(external.MsgQuery2ResultEOF)
	case inner.TypeQuery3Result:
		var result domain.Query3Result
		if err := pkt.UnmarshalData(&result); err != nil {
			return fmt.Errorf("unmarshalling query 3 result: %w", err)
		}

		externalResult := external.Query3Result{
			FromBank:      result.FromBank,
			FromAccount:   result.FromAccount,
			PaymentFormat: result.PaymentFormat,
			AmountPaid:    result.AmountPaid,
		}
		slog.Debug("Received Query Result", "amount_paid", externalResult.AmountPaid)

		if err := c.sendQuery3Result(&externalResult); err != nil {
			return fmt.Errorf("sending query 3 result: %w", err)
		}
	case inner.TypeQuery3EOF:
		return c.flagAndSendQueryEOF(external.MsgQuery3ResultEOF)
	case inner.TypeQuery4Result:
		var result domain.Query4Result
		if err := pkt.UnmarshalData(&result); err != nil {
			return fmt.Errorf("unmarshalling query 4 result: %w", err)
		}
		slog.Debug("Received Query4 Result", "num_accounts", len(result.Accounts))

		extResults := make([]external.Query4Result, 0, len(result.Accounts))
		for _, acc := range result.Accounts {
			extResults = append(extResults, external.Query4Result{
				BankID: acc.BankID,
				ID:     acc.ID,
			})
		}
		if err := c.sendQuery4Result(&extResults); err != nil {
			return fmt.Errorf("sending query 4 result: %w", err)
		}
	case inner.TypeQuery4EOF:
		return c.flagAndSendQueryEOF(external.MsgQuery4ResultEOF)
	case inner.TypeQuery5Result:
		var result domain.Query5Result
		if err := pkt.UnmarshalData(&result); err != nil {
			return fmt.Errorf("unmarshalling query 5 result: %w", err)
		}

		externalResult := external.Query5Result{Count: int64(result.Count)}
		slog.Debug("Received Query 5 Result", "count", externalResult.Count)

		if err := c.sendQuery5Result(&externalResult); err != nil {
			return fmt.Errorf("sending query 5 result: %w", err)
		}
	case inner.TypeQuery5EOF:
		return c.flagAndSendQueryEOF(external.MsgQuery5ResultEOF)
	default:
		slog.Warn("Unknown packet type received", "type", pkt.Type)
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

func (c *ClientConnection) flagAndSendQueryEOF(msgType external.MsgType) error {
	if msgType != external.MsgQuery1ResultEOF && msgType != external.MsgQuery2ResultEOF && msgType != external.MsgQuery3ResultEOF && msgType != external.MsgQuery4ResultEOF && msgType != external.MsgQuery5ResultEOF {
		return fmt.Errorf("invalid EOF message type: %v", msgType)
	}
	c.EOFSent[msgType] = true
	err := c.sendEnvelope(msgType, nil)
	if err != nil {
		return fmt.Errorf("sending EOF message of type %v: %w", msgType, err)
	}
	if c.allEOFSent() {
		slog.Debug("All EOFs sent, closing client connection")
		c.closeDoneOnce.Do(func() {
			close(c.done)
		})
	}
	return nil
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

func (c *ClientConnection) sendQuery2Result(result *external.Query2Result) error {
	payload, err := c.codec.EncodeQuery2ResultBatch([]external.Query2Result{(*result)})
	if err != nil {
		return fmt.Errorf("encoding query 2 result: %w", err)
	}
	return c.sendEnvelope(external.MsgQuery2Result, payload)
}

func (c *ClientConnection) sendQuery3Result(result *external.Query3Result) error {
	payload, err := c.codec.EncodeQuery3ResultBatch([]external.Query3Result{(*result)})
	if err != nil {
		return fmt.Errorf("encoding query 3 result: %w", err)
	}
	return c.sendEnvelope(external.MsgQuery3Result, payload)
}

func (c *ClientConnection) sendQuery4Result(results *[]external.Query4Result) error {
	payload, err := c.codec.EncodeQuery4ResultBatch(*results)
	if err != nil {
		return fmt.Errorf("encoding query 4 result: %w", err)
	}
	return c.sendEnvelope(external.MsgQuery4Result, payload)
}

func (c *ClientConnection) sendQuery5Result(result *external.Query5Result) error {
	payload, err := c.codec.EncodeQuery5ResultBatch([]external.Query5Result{(*result)})
	if err != nil {
		return fmt.Errorf("encoding query 5 result: %w", err)
	}
	return c.sendEnvelope(external.MsgQuery5Result, payload)
}
