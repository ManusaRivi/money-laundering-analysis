package converter

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/checkpoint"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/messaging"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
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
	pub                       *messaging.Publisher
	txProcessedCountForClient map[uuid.UUID]int
	coord                     *checkpoint.Coordinator

	rates *rateClient
}

func NewConverter(cfg config.WorkerConfig, broker broker.Broker) *Converter {
	return &Converter{
		cfg:                       cfg,
		Broker:                    broker,
		pub:                       messaging.New(codec.New(), broker),
		txProcessedCountForClient: make(map[uuid.UUID]int),
		rates:                     newRateClient(),
	}
}

func (c *Converter) Run() error {
	defer c.Broker.StopConsuming()

	checkpointManager, err := checkpoint.NewManager(c.cfg.CheckpointDir)
	if err != nil {
		slog.Error("Error creating checkpoint manager", "error", err)
		return err
	}
	c.coord = checkpoint.NewCoordinator(checkpointManager, c.pub, nil, c.cfg.CheckpointInterval)
	if err := c.coord.Recover(); err != nil {
		return err
	}

	return c.Broker.StartConsuming(func(msg broker.Message, ack, nack func()) {
		clientID, msgType, err := c.handleMessage(msg)
		if err != nil {
			nack()
			return
		}
		c.coord.Track(clientID, ack)
		if msgType == protocol.MsgTransactionsEOF {
			c.coord.Flush()
		}
	})
}

func (c *Converter) Stop() {}

// Private methods

func (c *Converter) sendTransactionBatch(clientID uuid.UUID, transactions []protocol.Transaction, parentID protocol.MsgID) error {
	transactionsBytes, err := c.pub.EncodeTransactionBatch(transactions)
	if err != nil {
		slog.Error("Error encoding converted transactions batch", "error", err)
		return err
	}
	txID := protocol.DeriveMsgID(parentID, string(broker.KeyDollarTransaction), 0)
	if err := c.pub.PublishInternalWithID(clientID, protocol.MsgTransactionsBatch, broker.KeyDollarTransaction, transactionsBytes, txID); err != nil {
		slog.Error("Error sending converted transactions batch to broker", "error", err)
		return err
	}
	c.txProcessedCountForClient[clientID] += len(transactions)
	return nil
}

func (c *Converter) handleTransactionMessage(envelope protocol.InternalEnvelope) error {
	transactions, err := c.pub.DecodeTransactionBatch(envelope.Payload)
	clientID := envelope.ClientId
	if err != nil {
		slog.Error("Error decoding transactions batch", "error", err)
		return err
	}

	results := make([]protocol.Transaction, 0, len(transactions))

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
	return c.sendTransactionBatch(clientID, results, envelope.MsgID)
}

func (c *Converter) handleEOFMessage(envelope protocol.InternalEnvelope) error {
	clientID := envelope.ClientId
	slog.Debug("Forwarding EOF to next worker...", "clientID", clientID)
	counts, err := c.pub.EncodeEOFCounts(map[broker.KeyType]int{broker.KeyNil: c.txProcessedCountForClient[clientID]})
	if err != nil {
		slog.Error("Error encoding EOF counts", "error", err)
		return err
	}
	eofID := protocol.StageMsgID(clientID, fmt.Sprintf("%s#%d", c.cfg.WorkerPrefix, c.cfg.WorkerID), "eof", 0)
	if err := c.pub.PublishInternalWithID(clientID, protocol.MsgTransactionsEOF, broker.KeyControlEOF, counts, eofID); err != nil {
		slog.Error("Error sending EOF packet", "error", err)
		return err
	}
	slog.Debug("Sent EOF packet after processing conversion results", "clientID", clientID, "msg_sent", c.txProcessedCountForClient[clientID])
	return c.coord.Flush()
}

func (c *Converter) handleMessage(msg broker.Message) (uuid.UUID, protocol.MsgType, error) {
	return c.pub.Dispatch(msg, map[protocol.MsgType]messaging.Handler{
		protocol.MsgTransactionsBatch: c.handleTransactionMessage,
		protocol.MsgTransactionsEOF:   c.handleEOFMessage,
	})
}
