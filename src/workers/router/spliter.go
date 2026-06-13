package router

import (
	"fmt"
	"hash/fnv"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/eof"
	// "github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external/codec"
	"github.com/google/uuid"
)

type Spliter struct {
	cfg               config.WorkerConfig
	codec			  codec.Codec
	Broker            broker.Broker
	syncEOFController *eof.SyncEOFController
	fieldsToRouteBy   []string
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
		codec:			   codec.New(),
		Broker:            broker,
		syncEOFController: nil,
		fieldsToRouteBy:   fieldsToRouteBy,
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
	// eofCounts := domain.EOFCounts{
	// 	Counts: finalSent,
	// }
	// eofMsg, err := inner.MarshalEOFPacket(clientID, eofCounts)
	eofEnvelope, err := r.codec.EncodeEOFCountsEnvelope(clientID, finalSent)
	if err != nil {
		slog.Error("Error marshalling EOF packet", "error", err)
		return err
	}
	eofMsg := broker.Message{
		RoutingKey:  broker.KeyControlEOF,
		ContentType: broker.ContentTypeBinary,
		Body:        eofEnvelope,
	}
	slog.Debug("Forwarding EOF to next worker...")
	if err := r.Broker.Send(eofMsg); err != nil {
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

func (r *Spliter) routeByField(field string, tx external.Transaction, clientID uuid.UUID) error {
	value := tx.GetTransactionField(field)
	if value == "" {
		slog.Error("Transaction missing routing field", "field", field, "transaction", tx)
		return fmt.Errorf("transaction missing routing field: %s", field)
	}
	routingKey := r.shardByValue(value)

	slog.Debug("Routing transaction", "routing_key", routingKey)

	txQ4Type := domain.GetTypeTxQ4ByField(field)
	slog.Debug("Encoding TxQ4 packet", "type", txQ4Type, "field_used", field)
	txQ4 := domain.TxQ4PhaseOne{
		Type:        txQ4Type,
		Transaction: &tx,
	}
	// envelope, err := inner.MarshalTxQ4PhaseOnePacket(clientID, broker.KeyType(routingKey), txQ4)
	envelope, err := r.codec.EncodeTxQ4PhaseOneEnvelope(clientID, txQ4)
	if err != nil {
		slog.Error("Error encoding TxQ4 packet", "error", err)
		return err
	}
	brokerMsg := broker.Message{
		RoutingKey:  broker.KeyType(routingKey),
		ContentType: broker.ContentTypeBinary,
		Body:        envelope,
	}
	if err := r.Broker.Send(brokerMsg); err != nil {
		slog.Error("Error sending message to broker", "error", err)
		return err
	}
	r.syncEOFController.MessageSentWithKey(clientID, broker.KeyType(routingKey), 1)
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

// func (r *Spliter) handleTransactionMessage(pkt inner.Packet) error {
// 	var tx domain.Transaction
// 	err := pkt.UnmarshalData(&tx)
// 	if err != nil {
// 		slog.Error("Error unmarshalling transaction data", "error", err)
// 		return err
// 	}
// 	slog.Debug("Received transaction to split")
// 	for _, field := range r.fieldsToRouteBy {
// 		if err := r.routeByField(field, tx, pkt.ClientID); err != nil {
// 			return err
// 		}
// 	}
// 	r.syncEOFController.MessageReceived(pkt.ClientID, 1)

// 	return nil
// }

func (r *Spliter) handleTransactionBatchMessage(envelope external.InternalEnvelope) error {
	clientId := envelope.ClientId
	txBatch, err := r.codec.DecodeTransactionBatch(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding transaction batch", "error", err)
		return err
	}
	slog.Debug("Received transactions batch", "batchSize", len(txBatch), "clientId", clientId)
	for _, tx := range txBatch {
		for _, field := range r.fieldsToRouteBy {
			if err := r.routeByField(field, tx, clientId); err != nil {
				return err
			}
		}
	}
	r.syncEOFController.MessageReceived(clientId, len(txBatch))

	return nil
}

func (r *Spliter) handleEOFMessage(envelope external.InternalEnvelope) error {
	eofCounts, err := r.codec.DecodeEOFCounts(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding EOF counts", "error", err)
		return err
	}
	r.syncEOFController.SyncEof(envelope.ClientId, eofCounts, r.syncEOFKey)
	return nil
}

// func (r *Spliter) handleMessage(msg broker.Message) error {
// 	pkt, err := inner.UnmarshalPacket(msg)

// 	if err != nil {
// 		slog.Error("Error unmarshalling message", "error", err)
// 		return err
// 	}

// 	switch pkt.Type {
// 	case inner.TypeTransaction:
// 		return r.handleTransactionMessage(*pkt)
// 	case inner.TypeEOF:
// 		return r.handleEOFMessage(*pkt)
// 	default:
// 		return fmt.Errorf("unknown packet type: %v", pkt.Type)
// 	}
// }

func (r *Spliter) handleMessage(msg broker.Message) error {
	envelope, err := r.codec.DecodeInternalEnvelope(msg.Body)
	if err != nil {
		slog.Error("Error decoding message", "error", err)
		return err
	}

	switch envelope.MsgType {
	case external.MsgTransactionsBatch:
		return r.handleTransactionBatchMessage(envelope)
	case external.MsgTransactionsEOF:
		return r.handleEOFMessage(envelope)
	default:
		return fmt.Errorf("unknown packet type: %v", envelope.MsgType)
	}
}