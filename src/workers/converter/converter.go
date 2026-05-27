package converter

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
	"github.com/google/uuid"
)

// Converter consumes non-USD transactions and republishes them with Paid
// converted to USD using historical FX rates from the Frankfurter API.
// Transactions in unsupported currencies (e.g. Bitcoin) are dropped with a
// log entry — they're counted as received but not as sent, which is what the
// downstream EOF synchronisation expects.
type Converter struct {
	cfg                       config.WorkerConfig
	Broker                    broker.Broker
	txProcessedCountForClient map[uuid.UUID]int

	rates *rateClient
}

func NewConverter(cfg config.WorkerConfig, broker broker.Broker) *Converter {
	return &Converter{
		cfg:                       cfg,
		Broker:                    broker,
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

func (c *Converter) handleTransactionMessage(pkt inner.Packet) error {
	var tx domain.Transaction
	if err := pkt.UnmarshalData(&tx); err != nil {
		slog.Error("Error unmarshalling transaction data", "error", err)
		return err
	}

	if tx.Paid == nil {
		slog.Warn("Skipping transaction without Paid amount", "clientID", pkt.ClientID)
		return nil
	}

	// USD transactions shouldn't reach this stage given the upstream routing,
	// but pass them through unchanged if they do.
	if tx.IsUSDTransaction() {
		return c.forward(pkt.ClientID, tx)
	}

	date, err := transactionDate(tx.Timestamp)
	if err != nil {
		slog.Warn("Skipping transaction with unparseable timestamp",
			"clientID", pkt.ClientID, "timestamp", tx.Timestamp, "error", err)
		return nil
	}

	usdAmount, err := c.rates.convertToUSD(date, tx.Paid.Currency, tx.Paid.Amount)
	if err != nil {
		if errors.Is(err, ErrUnsupportedCurrency) {
			slog.Warn("Skipping transaction in unsupported currency",
				"clientID", pkt.ClientID, "currency", tx.Paid.Currency)
			return nil
		}
		slog.Error("Failed to convert transaction to USD",
			"clientID", pkt.ClientID, "currency", tx.Paid.Currency, "date", date, "error", err)
		return err
	}

	tx.Paid = &domain.Money{Amount: usdAmount, Currency: "US Dollar"}
	return c.forward(pkt.ClientID, tx)
}

func (c *Converter) forward(clientID uuid.UUID, tx domain.Transaction) error {
	msg, err := inner.MarshalTransactionPacket(clientID, broker.KeyDollarTransaction, tx)
	if err != nil {
		slog.Error("Error marshalling converted transaction packet", "error", err)
		return err
	}
	if err := c.Broker.Send(*msg); err != nil {
		slog.Error("Error sending converted transaction to broker", "error", err)
		return err
	}
	c.txProcessedCountForClient[clientID]++
	return nil
}

func (c *Converter) handleEOFMessage(pkt inner.Packet) error {
	slog.Debug("Received EOF packet, forwarding...")

	eofMsg, err := inner.MarshalEOFPacket(pkt.ClientID, domain.EOFCounts{
		Counts: map[broker.KeyType]int{broker.KeyNil: c.txProcessedCountForClient[pkt.ClientID]},
	})
	if err != nil {
		slog.Error("Error marshalling EOF packet", "error", err)
		return err
	}
	slog.Debug("Sending EOF packet after processing aggregation results", "clientID", pkt.ClientID, "msg_sent", c.txProcessedCountForClient[pkt.ClientID])
	if err := c.Broker.Send(*eofMsg); err != nil {
		slog.Error("Error sending EOF packet", "error", err)
		return err
	}
	return nil
}

func (c *Converter) handleMessage(msg broker.Message) error {
	pkt, err := inner.UnmarshalPacket(msg)
	if err != nil {
		slog.Error("Error unmarshalling packet", "error", err)
		return err
	}

	switch pkt.Type {
	case inner.TypeTransaction:
		return c.handleTransactionMessage(*pkt)
	case inner.TypeEOF:
		return c.handleEOFMessage(*pkt)
	default:
		return fmt.Errorf("unexpected inbound packet type: %v", pkt.Type)
	}
}
