package router

import (
	"fmt"
	"hash/fnv"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/checkpoint"
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
	coord             *checkpoint.Coordinator
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
		r.onflush,
		r.onLeaderFlush,
		r.onRetryExceeded,
	)
	if err != nil {
		slog.Error("Error creating SyncEOFController", "error", err)
		return err
	}

	coord, err := checkpoint.NewCoordinator(r.cfg.CheckpointDir, r.pub, r.syncEOFController, nil, r.cfg.CheckpointInterval)
	if err != nil {
		slog.Error("Error creating checkpoint coordinator", "error", err)
		return err
	}
	r.coord = coord
	if err := r.coord.Recover(); err != nil {
		return err
	}

	go r.syncEOFController.Start()

	return r.Broker.StartConsuming(func(msg broker.Message, ack, nack func()) {
		clientID, err := r.handleMessage(msg)
		if err != nil {
			nack()
			return
		}
		r.coord.Track(clientID, ack)
	})
}

func (r *Spliter) onflush(clientID uuid.UUID) error {
	return nil
}

func (r *Spliter) onLeaderFlush(clientID uuid.UUID, finalSent map[broker.KeyType]int) error {
	eofEnvelope, err := r.pub.EncodeEOFCountsEnvelope(clientID, finalSent)
	if err != nil {
		slog.Error("Error marshalling EOF packet", "error", err)
		return err
	}
	slog.Debug("Forwarding EOF to next worker...")
	eofID := protocol.StageMsgID(clientID, r.cfg.WorkerPrefix, "eof", 0)
	if err := r.pub.PublishRawWithID(broker.KeyControlEOF, eofEnvelope, eofID); err != nil {
		slog.Error("Error sending EOF packet to broker", "error", err)
		return err
	}
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
func (r *Spliter) sendPhaseOneBatch(clientID uuid.UUID, txType domain.TypeTxQ4, routingKey broker.KeyType, txs []protocol.Transaction, parentID protocol.MsgID) error {
	envelope, err := r.pub.EncodeTxQ4PhaseOneBatchEnvelope(clientID, txType, txs)
	if err != nil {
		slog.Error("Error encoding TxQ4 phase-one batch", "error", err, "routing_key", routingKey)
		return err
	}
	id := protocol.DeriveMsgID(parentID, fmt.Sprintf("%v:%s", txType, routingKey), 0)
	if err := r.pub.PublishRawWithID(routingKey, envelope, id); err != nil {
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
		if err := r.sendPhaseOneBatch(clientId, bk.txType, broker.KeyType(bk.routingKey), txs, envelope.MsgID); err != nil {
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

func (r *Spliter) handleMessage(msg broker.Message) (uuid.UUID, error) {
	return r.pub.Dispatch(msg, map[protocol.MsgType]messaging.Handler{
		protocol.MsgTransactionsBatch: r.handleTransactionBatchMessage,
		protocol.MsgTransactionsEOF:   r.handleEOFMessage,
	})
}
