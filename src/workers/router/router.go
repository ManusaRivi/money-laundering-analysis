package router

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"strconv"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/eof"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
	"github.com/google/uuid"
)

type Router struct {
	cfg               config.WorkerConfig
	Broker            broker.Broker
	syncEOFController *eof.SyncEOFController
	fieldToRouteBy    string
	nextWorkerAmount  int
	syncEOFKey        broker.KeyType
}

func NewRouter(cfg config.WorkerConfig, broker broker.Broker) (*Router, error) {
	nextWorkerAmount := os.Getenv("NEXT_WORKER_AMOUNT")
	if nextWorkerAmount == "" {
		slog.Error("NEXT_WORKER_AMOUNT environment variable is not set")
		return nil, fmt.Errorf("NEXT_WORKER_AMOUNT environment variable is not set")
	}
	nextWorkerAmountInt, err := strconv.Atoi(nextWorkerAmount)
	if err != nil {
		slog.Error("Invalid NEXT_WORKER_AMOUNT environment variable", "error", err)
		return nil, fmt.Errorf("Invalid NEXT_WORKER_AMOUNT environment variable: %w", err)
	}

	params := cfg.Params
	fieldToRouteBy := ""
	if field, ok := params["field"]; ok {
		if fieldMap, ok := field.(map[string]any); ok {
			if originField, ok := fieldMap["origin"]; ok {
				if str, ok := originField.(string); ok {
					fieldToRouteBy = str
				}
			}
		}
	}
	slog.Debug("Router created with fieldToRouteBy", "fieldToRouteBy", fieldToRouteBy)
	syncEOFKey := eof.SyncKeyFromInputKeys(cfg.SyncEOFConfig.InputKeys)

	return &Router{
		cfg:               cfg,
		Broker:            broker,
		syncEOFController: nil,
		fieldToRouteBy:    fieldToRouteBy,
		nextWorkerAmount:  nextWorkerAmountInt,
		syncEOFKey:        syncEOFKey,
	}, nil
}

func (r *Router) Run() error {
	defer func() {
		r.Broker.StopConsuming()
	}()

	var err error
	r.syncEOFController, err = eof.NewSyncEOFController(
		r.cfg.SyncEOFConfig,
		r.onflush,
		r.onLeaderFlush,
		r.onRetryExceeded,
	)
	if err != nil {
		slog.Error("Error creating SyncEOFController", "error", err)
		return err
	}

	go r.syncEOFController.Start()

	return r.Broker.StartConsuming(func(msg broker.Message, ack, nack func()) {
		if err := r.handleMessage(msg); err != nil {
			nack()
			return
		}
		ack()
	})
}

func (r *Router) onflush(clientID uuid.UUID) error {
	return nil
}

func (r *Router) onLeaderFlush(clientID uuid.UUID, finalSent map[broker.KeyType]int) error {
	eofCounts := domain.EOFCounts{
		Counts: finalSent,
	}
	eofMsg, err := inner.MarshalEOFPacket(clientID, eofCounts)
	if err != nil {
		slog.Error("Error marshalling EOF packet", "error", err)
		return err
	}
	slog.Debug("Forwarding EOF to next worker...")
	if err := r.Broker.Send(*eofMsg); err != nil {
		slog.Error("Error sending EOF packet to broker", "error", err)
		return err
	}
	// limpieza adicional si es necesaria
	return nil
}

func (r *Router) onRetryExceeded(clientID uuid.UUID) error {
	return nil
}

func (r *Router) Stop() {}

// Private methods

func (r *Router) shardByField(tx domain.Transaction) string {
	value := r.extractFieldValue(tx)
	h := fnv.New32a()
	h.Write([]byte(value))
	index := int(h.Sum32()) % r.nextWorkerAmount
	if index < 0 {
		index += r.nextWorkerAmount
	}
	return fmt.Sprintf("%s_%d", r.cfg.NextWorkerPrefix, index)
}

func (r *Router) extractFieldValue(tx domain.Transaction) string {
	if tx.Origin == nil {
		return ""
	}
	switch r.fieldToRouteBy {
	case "BankID":
		return tx.Origin.BankID
	case "ID":
		return tx.Origin.ID
	case "Format":
		return tx.Format
	default:
		return ""
	}
}

func (r *Router) handleTransactionMessage(pkt inner.Packet) error {
	// Aquí se implementaría la lógica para manejar mensajes de tipo transacción.
	// Por ejemplo, podríamos extraer el valor del campo especificado en r.fieldToRouteBy
	// y usarlo para determinar a qué worker enviar el mensaje.
	var tx domain.Transaction
	err := pkt.UnmarshalData(&tx)
	if err != nil {
		slog.Error("Error unmarshalling transaction data", "error", err)
		return err
	}

	routingKey := r.shardByField(tx)

	slog.Debug("Routing transaction", "bankID", tx.Origin.BankID, "routingKey", routingKey)

	msg, err := inner.MarshalTransactionPacket(pkt.ClientID, broker.KeyType(routingKey), tx)

	if err != nil {
		slog.Error("Error marshalling transaction packet", "error", err)
		return err
	}

	if err := r.Broker.Send(*msg); err != nil {
		slog.Error("Error sending message to broker", "error", err)
		return err
	}
	r.syncEOFController.MessageReceived(pkt.ClientID)
	r.syncEOFController.MessageSentWithKey(pkt.ClientID, broker.KeyType(routingKey))

	return nil
}

func (r *Router) handleEOFMessage(pkt inner.Packet) error {
	var eofCounts domain.EOFCounts
	if err := pkt.UnmarshalData(&eofCounts); err != nil {
		slog.Error("Error unmarshalling EOF counts", "error", err)
		return err
	}
	r.syncEOFController.SyncEof(pkt.ClientID, eofCounts.Counts, r.syncEOFKey)
	return nil
}

func (r *Router) handleMessage(msg broker.Message) error {
	pkt, err := inner.UnmarshalPacket(msg)

	if err != nil {
		slog.Error("Error unmarshalling message", "error", err)
		return err
	}

	switch pkt.Type {
	case inner.TypeTransaction:
		return r.handleTransactionMessage(*pkt)
	case inner.TypeEOF:
		return r.handleEOFMessage(*pkt)
	default:
		return fmt.Errorf("unknown packet type: %v", pkt.Type)
	}
}
