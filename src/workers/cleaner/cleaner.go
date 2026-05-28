package cleaner

import (
	"fmt"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/eof"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
	"github.com/google/uuid"
)

type Cleaner struct {
	cfg               config.WorkerConfig
	Broker            broker.Broker
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
	eofCounts := domain.EOFCounts{
		Counts: finalSent,
	}
	eofMsg, err := inner.MarshalEOFPacket(clientID, eofCounts)
	if err != nil {
		slog.Error("Error marshalling EOF packet", "error", err)
		return err
	}
	slog.Debug("Received EOF packet, forwarding to next worker...")
	if err := c.Broker.Send(*eofMsg); err != nil {
		slog.Error("Error sending EOF packet to broker", "error", err)
		return err
	}
	// limpieza adicional si es necesaria
	return nil
}

func (c *Cleaner) onRetryExceeded(clientID uuid.UUID) error {
	// TODO: Loguear que el cliente supero el maximo de reintentos y tomar la decision que se considere (ej: emitir un EOF forzado, loguear un error, etc)
	return nil
}

func (c *Cleaner) Stop() {}

func (c *Cleaner) handleTransactionMessage(pkt inner.Packet) error {
	var tx domain.Transaction
	if err := pkt.UnmarshalData(&tx); err != nil {
		slog.Error("Error unmarshalling transaction data", "error", err)
		return err
	}

	for _, f := range c.fieldsToClean {
		tx.CutColumn(f)
	}

	msg, err := inner.MarshalTransactionPacket(pkt.ClientID, broker.KeyNil, tx)

	if err != nil {
		slog.Error("Error marshalling cleaned packet", "error", err)
		return err
	}

	if err := c.Broker.Send(*msg); err != nil {
		slog.Error("Error sending cleaned packet to broker", "error", err)
		return err
	}
	slog.Debug("Cleaner sent transaction",
		"client_id", pkt.ClientID,
		"input_key", msg.RoutingKey,
	)
	c.syncEOFController.MessageReceived(pkt.ClientID)
	c.syncEOFController.MessageSentWithKey(pkt.ClientID, broker.KeyNil)

	return nil
}

func (c *Cleaner) handleEOFMessage(pkt inner.Packet) error {
	slog.Debug("Received EOF packet, forwarding to next worker...")
	var eofCounts domain.EOFCounts
	if err := pkt.UnmarshalData(&eofCounts); err != nil {
		slog.Error("Error unmarshalling EOF counts", "error", err)
		return err
	}
	c.syncEOFController.SyncEof(pkt.ClientID, eofCounts.Counts, c.syncEOFKey)
	return nil
}

func (c *Cleaner) handleMessage(msg broker.Message) error {
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
