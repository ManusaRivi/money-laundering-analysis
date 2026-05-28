package router

import (
	"fmt"
	"hash/fnv"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/eof"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
	"github.com/google/uuid"
)

type Spliter struct {
	cfg               config.WorkerConfig
	Broker            broker.Broker
	syncEOFController *eof.SyncEOFController
	fieldsToRouteBy    []string
	nextWorkerAmount  int
	syncEOFKey        broker.KeyType
}

func NewSpliter(cfg config.WorkerConfig, broker broker.Broker) (*Spliter, error) {

	params := cfg.Params
	fieldsToRouteBy := []string{}
	if field, ok := params["field"]; ok {
		if fieldList, ok := field.([]any); ok && len(fieldList) > 0 {
			for _, v := range fieldList {
				if str, ok := v.(string); ok {
					fieldsToRouteBy = append(fieldsToRouteBy, str)
				}
			}
		}
	}
	slog.Debug("Spliter created with fieldsToRouteBy", "fieldsToRouteBy", fieldsToRouteBy)
	syncEOFKey := eof.SyncKeyFromInputKeys(cfg.SyncEOFConfig.InputKeys)

	return &Spliter{
		cfg:               cfg,
		Broker:            broker,
		syncEOFController: nil,
		fieldsToRouteBy:    fieldsToRouteBy,
		nextWorkerAmount:  cfg.NextWorkerAmount,
		syncEOFKey:        syncEOFKey,
	}, nil
}

func (r *Spliter) Run() error {
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

func (r *Spliter) onflush(clientID uuid.UUID) error {
	return nil
}

func (r *Spliter) onLeaderFlush(clientID uuid.UUID, finalSent map[broker.KeyType]int) error {
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

func (r *Spliter) onRetryExceeded(clientID uuid.UUID) error {
	return nil
}

func (r *Spliter) Stop() {
	r.Broker.StopConsuming()
	r.Broker.Close()
}

// Private methods

func (r *Spliter) routeByField(field string, tx domain.Transaction, clientID uuid.UUID) error {
	value := tx.GetTransactionField(field)
	if value == "" {
		slog.Error("Transaction missing routing field", "field", field, "transaction", tx)
		return fmt.Errorf("transaction missing routing field: %s", field)
	}
	routingKey := r.shardByValue(value)

	slog.Debug("Routing transaction", "routing_key", routingKey)

	txQ4Type := domain.GetTypeTxQ4ByField(field)
	slog.Debug("Marshalling TxQ4 packet", "type", txQ4Type, "field_used", field)
	txQ4 := domain.TxQ4PhaseOne{
		Type:        txQ4Type,
		Transaction: &tx,
	}
	msg, err := inner.MarshalTxQ4PhaseOnePacket(clientID, broker.KeyType(routingKey), txQ4)
	if err != nil {
		slog.Error("Error marshalling transaction packet", "error", err)
		return err
	}

	if err := r.Broker.Send(*msg); err != nil {
		slog.Error("Error sending message to broker", "error", err)
		return err
	}
	r.syncEOFController.MessageSentWithKey(clientID, broker.KeyType(routingKey))
	return nil
}


func (r *Spliter) shardByValue(value string) string {
	h := fnv.New32a()
	h.Write([]byte(value))
	index := int(h.Sum32()) % r.nextWorkerAmount
	if index < 0 {
		index += r.nextWorkerAmount
	}
	return fmt.Sprintf("%s_%d", r.cfg.NextWorkerPrefix, index)
}


func (r *Spliter) handleTransactionMessage(pkt inner.Packet) error {
	var tx domain.Transaction
	err := pkt.UnmarshalData(&tx)
	if err != nil {
		slog.Error("Error unmarshalling transaction data", "error", err)
		return err
	}
	slog.Debug("Received transaction to split")
	for _, field := range r.fieldsToRouteBy {
		if err := r.routeByField(field, tx, pkt.ClientID); err != nil {
			return err
		}
	}
	r.syncEOFController.MessageReceived(pkt.ClientID)

	return nil
}


func (r *Spliter) handleEOFMessage(pkt inner.Packet) error {
	var eofCounts domain.EOFCounts
	if err := pkt.UnmarshalData(&eofCounts); err != nil {
		slog.Error("Error unmarshalling EOF counts", "error", err)
		return err
	}
	r.syncEOFController.SyncEof(pkt.ClientID, eofCounts.Counts, r.syncEOFKey)
	return nil
}

func (r *Spliter) handleMessage(msg broker.Message) error {
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
