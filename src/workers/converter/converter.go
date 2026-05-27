package converter

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/eof"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
	"github.com/google/uuid"
)

// Converter consumes non-USD transactions and republishes them with Paid
// converted to USD using historical FX rates from the Frankfurter API.
// Transactions in unsupported currencies (e.g. Bitcoin) are dropped with a
// log entry — they're counted as received but not as sent, which is what the
// downstream EOF synchronisation expects.
type Converter struct {
	cfg    config.WorkerConfig
	Broker broker.Broker

	syncEOFController *eof.SyncEOFController
	rates             *rateClient
}

func NewConverter(cfg config.WorkerConfig, broker broker.Broker) *Converter {
	return &Converter{
		cfg:    cfg,
		Broker: broker,
		rates:  newRateClient(),
	}
}

func (c *Converter) Run() error {
	defer c.Broker.StopConsuming()

	var err error
	c.syncEOFController, err = eof.NewSyncEOFController(
		c.cfg.SyncEOFConfig,
		c.onflush,
		c.onLeaderFlush,
		c.onRetryExceeded,
	)
	if err != nil {
		slog.Error("Error creating SyncEOFController", "error", err)
		return err
	}

	go c.syncEOFController.Start()

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
	c.syncEOFController.MessageReceived(pkt.ClientID)

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
	c.syncEOFController.MessageSent(clientID)
	return nil
}

func (c *Converter) handleEOFMessage(pkt inner.Packet) error {
	slog.Debug("Received EOF packet, syncing with cluster...")
	var eofCounts domain.EOFCounts
	if err := pkt.UnmarshalData(&eofCounts); err != nil {
		slog.Error("Error unmarshalling EOF counts", "error", err)
		return err
	}
	// Upstream (format_filter) is a SyncFilter, which emits its outbound EOF
	// total under KeyNil. See SyncFilter.onLeaderFlush.
	total_transactions := eofCounts.Counts[broker.KeyNil]
	c.syncEOFController.SyncEof(pkt.ClientID, total_transactions)
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

func (c *Converter) onflush(clientID uuid.UUID) error {
	// The converter forwards each transaction as it arrives, so there's no
	// accumulated state to flush on the followers' callback.
	return nil
}

func (c *Converter) onLeaderFlush(clientID uuid.UUID, finalSent int) error {
	counts := map[broker.KeyType]int{broker.KeyNil: finalSent}
	eofMsg, err := inner.MarshalEOFPacket(clientID, domain.EOFCounts{Counts: counts})
	if err != nil {
		slog.Error("Error marshalling EOF packet", "error", err)
		return err
	}
	slog.Debug("Forwarding EOF to next worker...", "finalSent", finalSent)
	if err := c.Broker.Send(*eofMsg); err != nil {
		slog.Error("Error sending EOF packet to broker", "error", err)
		return err
	}
	return nil
}

func (c *Converter) onRetryExceeded(clientID uuid.UUID) error {
	// TODO: surface this via metrics; for now, log and let the controller
	// handle further escalation.
	slog.Warn("Converter exceeded EOF sync retries", "clientID", clientID)
	return nil
}
