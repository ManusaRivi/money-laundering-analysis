package cleaner

import (
	"fmt"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/eof"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external/codec"
	"github.com/google/uuid"
)

type Cleaner struct {
	cfg               config.WorkerConfig
	Broker            broker.Broker
	codec             codec.Codec
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
		codec:             codec.New(),
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
	eofPayload, err := c.codec.EncodeEOFCounts(finalSent)
	if err != nil {
		slog.Error("Error encoding EOF counts for leader flush", "error", err)
		return err
	}
	eofEnvelope, err := c.codec.EncodeInternalEnvelope(external.InternalEnvelope{
		MsgType:  external.MsgTransactionsEOF,
		ClientId: clientID,
		Payload:  eofPayload,
	})
	if err != nil {
		slog.Error("Error encoding internal envelope for leader flush", "error", err)
		return err
	}
	eofMsg := broker.Message{
		RoutingKey:  broker.KeyControlEOF,
		ContentType: broker.ContentTypeBinary,
		Body:        eofEnvelope,
	}
	slog.Debug("Leader flush triggered, sending EOF packet to next worker...")
	if err := c.Broker.Send(eofMsg); err != nil {
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

func (c *Cleaner) cleanTransaction(tx external.Transaction) external.Transaction {
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

func (c *Cleaner) handleTransactionMessage(envelope external.InternalEnvelope) error {
	transactions, err := c.codec.DecodeTransactionBatch(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding transaction batch", "error", err)
		return err
	}

	c.syncEOFController.MessageReceived(envelope.ClientId, len(transactions))
	cleanedTx := make([]external.Transaction, len(transactions))

	for _, tx := range transactions {
		cleanedTx = append(cleanedTx, c.cleanTransaction(tx))
	}

	txPayload, err := c.codec.EncodeTransactionBatch(cleanedTx)
	if err != nil {
		slog.Error("Error encoding cleaned transaction batch", "error", err)
		return err
	}

	resultEnvelope, err := c.codec.EncodeInternalEnvelope(external.InternalEnvelope{
		MsgType:  external.MsgTransactionsBatch,
		ClientId: envelope.ClientId,
		Payload:  txPayload,
	})
	if err != nil {
		slog.Error("Error encoding result envelope", "error", err)
		return err
	}

	cleanedMsg := broker.Message{
		ContentType: broker.ContentTypeBinary,
		Body:        resultEnvelope,
	}

	if err := c.Broker.Send(cleanedMsg); err != nil {
		slog.Error("Error sending cleaned transaction batch to broker", "error", err)
		return err
	}

	c.syncEOFController.MessageSentWithKey(envelope.ClientId, broker.KeyNil, len(transactions))

	return nil
}

func (c *Cleaner) handleEOFMessage(envelope external.InternalEnvelope) error {
	slog.Debug("Received EOF packet, starting EOF sync...")
	eofCounts, err := c.codec.DecodeEOFCounts(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding EOF counts", "error", err)
		return err
	}
	c.syncEOFController.SyncEof(envelope.ClientId, eofCounts, c.syncEOFKey)
	return nil
}

func (c *Cleaner) handleMessage(msg broker.Message) error {
	envelope, err := c.codec.DecodeInternalEnvelope(msg.Body)

	if err != nil {
		slog.Error("Error decoding envelope", "error", err)
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
