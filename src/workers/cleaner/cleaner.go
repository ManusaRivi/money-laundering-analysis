package cleaner

import (
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/eof"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/messaging"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
	"github.com/google/uuid"
)

type Cleaner struct {
	cfg               config.WorkerConfig
	Broker            broker.Broker
	pub               *messaging.Publisher
	syncEOFController *eof.SyncEOFController
	fieldsToClean     []string
	syncEOFKey        broker.KeyType
}

func NewCleaner(cfg config.WorkerConfig, b broker.Broker) *Cleaner {
	params := cfg.Params
	fieldsToClean := make([]string, 0)
	if field, ok := params["field"]; ok {
		if fieldList, ok := field.([]any); ok {
			for _, f := range fieldList {
				if str, ok := f.(string); ok {
					fieldsToClean = append(fieldsToClean, str)
				}
			}
		}
	}

	syncEOFKey := eof.SyncKeyFromInputKeys(cfg.SyncEOFConfig.InputKeys)

	slog.Debug("Creating Cleaner worker with configuration", "fields_to_clean", fieldsToClean, "sync_eof_key", syncEOFKey)

	return &Cleaner{
		cfg:               cfg,
		Broker:            b,
		pub:               messaging.New(codec.New(), b),
		fieldsToClean:     fieldsToClean,
		syncEOFController: nil,
		syncEOFKey:        syncEOFKey,
	}
}

func (c *Cleaner) Run() error {
	defer func() {
		c.Broker.StopConsuming()
	}()

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

func (c *Cleaner) onflush(clientID uuid.UUID) error {
	// El cleaner esta constantemente haciendo flush, no tiene nada que hacer cuando recibe el callback de flush.
	return nil
}

func (c *Cleaner) onLeaderFlush(clientID uuid.UUID, finalSent map[broker.KeyType]int) error {
	eofPayload, err := c.pub.EncodeEOFCounts(finalSent)
	if err != nil {
		slog.Error("Error encoding EOF counts for leader flush", "error", err)
		return err
	}
	slog.Debug("Leader flush triggered, sending EOF packet to next worker...")
	if err := c.pub.PublishInternal(clientID, protocol.MsgTransactionsEOF, broker.KeyControlEOF, eofPayload); err != nil {
		slog.Error("Error sending EOF packet to broker during leader flush", "error", err)
		return err
	}
	return nil
}

func (c *Cleaner) onRetryExceeded(clientID uuid.UUID) error {
	// TODO: Loguear que el cliente supero el maximo de reintentos y tomar la decision que se considere (ej: emitir un EOF forzado, loguear un error, etc)
	return nil
}

func (c *Cleaner) Stop() {
	c.Broker.StopConsuming()
	c.Broker.Close()
}

func (c *Cleaner) cleanTransaction(tx protocol.Transaction) protocol.Transaction {
	cleanedTx := tx

	for _, field := range c.fieldsToClean {
		switch field {
		case "from_bank":
			cleanedTx.FromBank = ""
		case "from_account":
			cleanedTx.FromAccount = ""
		case "to_bank":
			cleanedTx.ToBank = ""
		case "to_account":
			cleanedTx.ToAccount = ""
		case "payment_format":
			cleanedTx.PaymentFormat = ""
		}
	}

	return cleanedTx
}

func (c *Cleaner) handleTransactionMessage(envelope protocol.InternalEnvelope) error {
	transactions, err := c.pub.DecodeTransactionBatch(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding transaction batch", "error", err)
		return err
	}

	c.syncEOFController.MessageReceived(envelope.ClientId, len(transactions))
	cleanedTx := make([]protocol.Transaction, len(transactions))

	for _, tx := range transactions {
		cleanedTx = append(cleanedTx, c.cleanTransaction(tx))
	}

	txPayload, err := c.pub.EncodeTransactionBatch(cleanedTx)
	if err != nil {
		slog.Error("Error encoding cleaned transaction batch", "error", err)
		return err
	}

	if err := c.pub.PublishInternal(envelope.ClientId, protocol.MsgTransactionsBatch, broker.KeyNil, txPayload); err != nil {
		slog.Error("Error sending cleaned transaction batch to broker", "error", err)
		return err
	}

	c.syncEOFController.MessageSentWithKey(envelope.ClientId, broker.KeyNil, len(transactions))

	return nil
}

func (c *Cleaner) handleEOFMessage(envelope protocol.InternalEnvelope) error {
	slog.Debug("Received EOF packet, starting EOF sync...")
	eofCounts, err := c.pub.DecodeEOFCounts(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding EOF counts", "error", err)
		return err
	}
	c.syncEOFController.SyncEof(envelope.ClientId, eofCounts, c.syncEOFKey)
	return nil
}

func (c *Cleaner) handleMessage(msg broker.Message) error {
	return c.pub.Dispatch(msg, map[protocol.MsgType]messaging.Handler{
		protocol.MsgTransactionsBatch: c.handleTransactionMessage,
		protocol.MsgTransactionsEOF:   c.handleEOFMessage,
	})
}
