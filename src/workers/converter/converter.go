package converter

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external/codec"
	"github.com/google/uuid"
)

const Dollar = "US Dollar"

// Converter consumes non-USD transactions and republishes them with Paid
// converted to USD using historical FX rates from the Frankfurter API.
// Transactions in unsupported currencies (e.g. Bitcoin) are dropped with a
// log entry — they're counted as received but not as sent, which is what the
// downstream EOF synchronisation expects.
type Converter struct {
	cfg                       config.WorkerConfig
	Broker                    broker.Broker
	codec                     codec.Codec
	txProcessedCountForClient map[uuid.UUID]int

	rates *rateClient
}

func NewConverter(cfg config.WorkerConfig, broker broker.Broker) *Converter {
	return &Converter{
		cfg:                       cfg,
		Broker:                    broker,
		codec:                     codec.New(),
		txProcessedCountForClient: make(map[uuid.UUID]int),
		rates:                     newRateClient(),
	}
}

func (c *Converter) Run() error {
	defer c.Broker.StopConsuming()

	return c.Broker.StartConsuming(func(msg broker.Message, ack, nack func()) {
		if err := c.handleMessage(msg); err != nil {
			nack()
			return
		}
		ack()
	})
}

func (c *Converter) Stop() {}

// Private methods

func (c *Converter) sendTransactionBatch(clientID uuid.UUID, transactions []external.Transaction) error {
	transactionsBytes, err := c.codec.EncodeTransactionBatch(transactions)
	if err != nil {
		slog.Error("Error encoding converted transactions batch", "error", err)
		return err
	}
	envelope, err := c.codec.EncodeInternalEnvelope(external.InternalEnvelope{
		MsgType:  external.MsgTransactionsBatch,
		ClientId: clientID,
		Payload:  transactionsBytes,
	})
	if err != nil {
		slog.Error("Error encoding converted transactions envelope", "error", err)
		return err
	}
	if err := c.Broker.Send(broker.Message{
		RoutingKey:  broker.KeyDollarTransaction,
		Body:        envelope,
		ContentType: broker.ContentTypeBinary,
	}); err != nil {
		slog.Error("Error sending converted transactions batch to broker", "error", err)
		return err
	}
	c.txProcessedCountForClient[clientID] += len(transactions)
	return nil
}

func (c *Converter) handleTransactionMessage(envelope external.InternalEnvelope) error {
	transactions, err := c.codec.DecodeTransactionBatch(envelope.Payload)
	clientID := envelope.ClientId
	if err != nil {
		slog.Error("Error decoding transactions batch", "error", err)
		return err
	}

	results := make([]external.Transaction, 0, len(transactions))

	for _, tx := range transactions {
		if tx.PaymentCurrency == Dollar {
			results = append(results, tx)
			continue
		}
		date, err := transactionDate(tx.Timestamp)
		if err != nil {
			slog.Warn("Skipping transaction with unparseable timestamp",
				"clientID", clientID, "timestamp", tx.Timestamp, "error", err)
			return nil
		}

		usdAmount, err := c.rates.convertToUSD(date, tx.PaymentCurrency, tx.AmountPaid)
		if err != nil {
			if errors.Is(err, ErrUnsupportedCurrency) {
				slog.Warn("Skipping transaction in unsupported currency",
					"clientID", clientID, "currency", tx.PaymentCurrency)
				return nil
			}
			slog.Error("Failed to convert transaction to USD",
				"clientID", clientID, "currency", tx.PaymentCurrency, "date", date, "error", err)
			return err
		}
		tx.AmountPaid = usdAmount
		tx.PaymentCurrency = Dollar
		results = append(results, tx)
	}
	return c.sendTransactionBatch(clientID, results)
}

func (c *Converter) handleEOFMessage(envelope external.InternalEnvelope) error {
	clientID := envelope.ClientId
	slog.Debug("Forwarding EOF to next worker...", "clientID", clientID)
	counts, err := c.codec.EncodeEOFCounts(map[broker.KeyType]int{broker.KeyNil: c.txProcessedCountForClient[clientID]})
	if err != nil {
		slog.Error("Error encoding EOF counts", "error", err)
		return err
	}
	eofEnvelope, err := c.codec.EncodeInternalEnvelope(external.InternalEnvelope{
		MsgType:  external.MsgTransactionsEOF,
		ClientId: clientID,
		Payload:  counts,
	})
	if err != nil {
		slog.Error("Error encoding EOF envelope", "error", err)
		return err
	}
	if err := c.Broker.Send(broker.Message{
		RoutingKey:  broker.KeyControlEOF,
		Body:        eofEnvelope,
		ContentType: broker.ContentTypeBinary,
	}); err != nil {
		slog.Error("Error sending EOF packet", "error", err)
		return err
	}
	slog.Debug("Sent EOF packet after processing conversion results", "clientID", clientID, "msg_sent", c.txProcessedCountForClient[clientID])
	return nil
}

func (c *Converter) handleMessage(msg broker.Message) error {
	envelope, err := c.codec.DecodeInternalEnvelope(msg.Body)
	if err != nil {
		slog.Error("Error decoding message", "error", err)
		return err
	}

	switch envelope.MsgType {
	case external.MsgTransactionsBatch:
		return c.handleTransactionMessage(envelope)
	case external.MsgTransactionsEOF:
		return c.handleEOFMessage(envelope)
	default:
		return fmt.Errorf("unexpected inbound packet type: %v", envelope.MsgType)
	}
}
