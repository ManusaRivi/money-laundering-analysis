package router

import (
	"fmt"
	"hash/fnv"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/eof"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/messaging"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"

	// "github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
	"github.com/google/uuid"
)

type Spliter struct {
	cfg               config.WorkerConfig
	pub               *messaging.Publisher
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
		pub:               messaging.New(codec.New(), broker),
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

func (r *Spliter) onLeaderFlush(clientID uuid.UUID, finalSent map[broker.KeyType]int) error {
	// eofCounts := domain.EOFCounts{
	// 	Counts: finalSent,
	// }
	// eofMsg, err := inner.MarshalEOFPacket(clientID, eofCounts)
	eofEnvelope, err := r.pub.EncodeEOFCountsEnvelope(clientID, finalSent)
	if err != nil {
		slog.Error("Error marshalling EOF packet", "error", err)
		return err
	}
	slog.Debug("Forwarding EOF to next worker...")
	if err := r.pub.PublishRaw(broker.KeyControlEOF, eofEnvelope); err != nil {
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

// sendPhaseOneBatch ships one batched phase-one message for a (type, shard)
// group. The EOF count is the number of transactions in the batch, so the
// per-key accounting matches the old one-message-per-transaction behaviour.
func (r *Spliter) sendPhaseOneBatch(clientID uuid.UUID, txType domain.TypeTxQ4, routingKey broker.KeyType, txs []protocol.Transaction) error {
	envelope, err := r.pub.EncodeTxQ4PhaseOneBatchEnvelope(clientID, txType, txs)
	if err != nil {
		slog.Error("Error encoding TxQ4 phase-one batch", "error", err, "routing_key", routingKey)
		return err
	}
	if err := r.pub.PublishRaw(routingKey, envelope); err != nil {
		slog.Error("Error sending TxQ4 phase-one batch", "error", err, "routing_key", routingKey)
		return err
	}
	r.syncEOFController.MessageSentWithKey(clientID, routingKey, len(txs))
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

// bucketKey groups transactions heading to the same phase-one worker with the
// same scatter/gather role, so they can be shipped as one batch.
type bucketKey struct {
	txType     domain.TypeTxQ4
	routingKey string
}

func (r *Spliter) handleTransactionBatchMessage(envelope protocol.InternalEnvelope) error {
	clientId := envelope.ClientId
	txBatch, err := r.pub.DecodeTransactionBatch(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding transaction batch", "error", err)
		return err
	}
	slog.Debug("Received transactions batch", "batchSize", len(txBatch), "clientId", clientId)

	// Group the whole inbound batch by (type, shard) before sending, so each
	// phase-one worker gets one message per role instead of one per transaction.
	buckets := make(map[bucketKey][]protocol.Transaction)
	for _, tx := range txBatch {
		for _, field := range r.fieldsToRouteBy {
			value := tx.GetTransactionField(field)
			if value == "" {
				slog.Error("Transaction missing routing field", "field", field, "transaction", tx)
				return fmt.Errorf("transaction missing routing field: %s", field)
			}
			bk := bucketKey{
				txType:     domain.GetTypeTxQ4ByField(field),
				routingKey: r.shardByValue(value),
			}
			buckets[bk] = append(buckets[bk], tx)
		}
	}

	for bk, txs := range buckets {
		if err := r.sendPhaseOneBatch(clientId, bk.txType, broker.KeyType(bk.routingKey), txs); err != nil {
			return err
		}
	}

	r.syncEOFController.MessageReceived(clientId, len(txBatch))

	return nil
}

func (r *Spliter) handleEOFMessage(envelope protocol.InternalEnvelope) error {
	eofCounts, err := r.pub.DecodeEOFCounts(envelope.Payload)
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
	return r.pub.Dispatch(msg, map[protocol.MsgType]messaging.Handler{
		protocol.MsgTransactionsBatch: r.handleTransactionBatchMessage,
		protocol.MsgTransactionsEOF:   r.handleEOFMessage,
	})
}
