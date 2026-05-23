package clientconnection

import (
	"fmt"
	"log/slog"
	"net"

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
	ClientId uuid.UUID
	Conn     network.Connection
	Handler  *messagehandler.MessageHandler
	codec    codec.Codec
}

func NewClientConnection(clientId uuid.UUID, conn net.Conn, codec codec.Codec) *ClientConnection {
	handler := messagehandler.NewMessageHandler()
	return &ClientConnection{
		ClientId: clientId,
		Conn:     network.NewConnection(conn),
		Handler:  &handler,
		codec:    codec,
	}
}

// Run drives the client connection until both dataset EOFs are received (or the
// connection breaks), then sends the result EOF back to the client.
// TODO: Refactor to have separate goroutines for receiving and sending, and a
// channel for the results batches. This will allow sending results to the client
// as soon as they are ready, instead of waiting for all input to be received.
func (c *ClientConnection) Run() {
	defer c.Conn.Close()

	if err := c.receiveUntilEOF(); err != nil {
		slog.Error("Error handling client", "err", err)
		return
	}

	if err := c.sendResults(); err != nil {
		slog.Error("Error sending results", "err", err)
	}
}

// Receives from both accounts and transactions datasets until both EOF are received.
// TODO: Have the sending and receiving be done in parallel instead of sequentially.
//
// Message is dispatched based on msgType. Transaction batches are stored if they pass the filter
// demanded by query 1.
func (c *ClientConnection) receiveUntilEOF() error {
	accountsDone, transactionsDone := false, false

	for !(accountsDone && transactionsDone) {
		header, err := c.Conn.Receive(codec.HeaderSize)
		if err != nil {
			return fmt.Errorf("receiving header: %w", err)
		}

		msgType, payloadSize := codec.DecodeHeader(header)

		payload, err := c.Conn.Receive(int(payloadSize))
		if err != nil {
			return fmt.Errorf("receiving payload: %w", err)
		}

		switch msgType {
		case protocol.MsgAccountsBatch:
			accounts, err := c.codec.DecodeAccountBatch(payload)
			if err != nil {
				return fmt.Errorf("decoding account batch: %w", err)
			}
			c.Handler.HandleAccountsBatch(accounts)
		case protocol.MsgAccountsEOF:
			slog.Debug("Received accounts EOF")
			c.Handler.HandleAccountsEOF()
			accountsDone = true
		case protocol.MsgTransactionsBatch:
			transactions, err := c.codec.DecodeTransactionBatch(payload)
			if err != nil {
				return fmt.Errorf("decoding transaction batch: %w", err)
			}
			c.Handler.HandleTransactionsBatch(transactions)
		case protocol.MsgTransactionsEOF:
			slog.Debug("Received transactions EOF")
			c.Handler.HandleTransactionsEOF()
			transactionsDone = true
		default:
			slog.Warn("Unknown message type received", "msgType", msgType)
		}
	}
	return nil
}

func (c *ClientConnection) sendEnvelope(msgType protocol.MsgType, payload []byte) error {
	envelope, err := c.codec.EncodeEnvelope(protocol.Envelope{
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

func (c *ClientConnection) sendResults() error {
	for {
		batch := c.Handler.GetTransactionResultBatch()
		if batch == nil {
			break
		}
		if err := c.sendQuery1Batch(batch); err != nil {
			return err
		}
	}

	return c.sendEnvelope(protocol.MsgQuery1ResultEOF, nil)
}

func (c *ClientConnection) sendQuery1Batch(transactions []protocol.Transaction) error {
	results := make([]protocol.Query1Result, len(transactions))
	for i, t := range transactions {
		results[i] = protocol.Query1Result{
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
	return c.sendEnvelope(protocol.MsgQuery1Result, payload)
}
