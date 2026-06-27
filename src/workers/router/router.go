package router

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/checkpoint"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/eof"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/messaging"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
	"github.com/google/uuid"
)

type Router struct {
	cfg               config.WorkerConfig
	Broker            broker.Broker
	pub               *messaging.Publisher
	syncEOFController *eof.SyncEOFController
	sectionToRouteBy  string
	fieldToRouteBy    string
	nextWorkerAmount  int
	syncEOFKey        broker.KeyType
	coord             *checkpoint.Coordinator
	mu                sync.Mutex
	flushed           map[uuid.UUID]struct{}
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

	section, field := parseRouteField(cfg.Params)
	slog.Debug("Router created", "section", section, "field", field)
	syncEOFKey := eof.SyncKeyFromInputKeys(cfg.SyncEOFConfig.InputKeys)

	return &Router{
		cfg:               cfg,
		Broker:            broker,
		pub:               messaging.New(codec.New(), broker),
		syncEOFController: nil,
		sectionToRouteBy:  section,
		fieldToRouteBy:    field,
		nextWorkerAmount:  nextWorkerAmountInt,
		syncEOFKey:        syncEOFKey,
		flushed:           make(map[uuid.UUID]struct{}),
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
		clientID, msgType, err := r.handleMessage(msg, ack)
		if err != nil {
			nack()
			return
		}
		if msgType == protocol.MsgTransactionsEOF {
			return
		}
		r.mu.Lock()
		_, isFlushed := r.flushed[clientID]
		r.mu.Unlock()
		if isFlushed {
			ack()
			return
		}
		r.coord.Track(clientID, ack)
	})
}

func (r *Router) Stop() {
	if r.syncEOFController != nil {
		r.syncEOFController.Stop()
	}
}

// Private methods

func (r *Router) encodeAndSendBatch(clientID uuid.UUID, msgType protocol.MsgType, payload []byte, routingKey broker.KeyType, batchLength int, id protocol.MsgID) error {
	slog.Debug("Sending batch to broker:", "batchSize", batchLength, "clientId", clientID, "msgType", msgType)
	if err := r.pub.PublishInternalWithID(clientID, msgType, routingKey, payload, id); err != nil {
		return err
	}
	r.syncEOFController.MessageSentWithKey(clientID, routingKey, id, batchLength)
	return nil
}

// parseRouteField reads params["field"] expecting `{ <section>: <field> }`
// (e.g. { origin: "BankID" } or { paid: "Currency" }) and returns the
// section and field name.
func parseRouteField(params map[string]any) (string, string) {
	field, ok := params["field"]
	if !ok {
		return "", ""
	}
	fieldMap, ok := field.(map[string]any)
	if !ok {
		return "", ""
	}
	for section, v := range fieldMap {
		if str, ok := v.(string); ok {
			return section, str
		}
	}
	return "", ""
}

func (r *Router) onflush(clientID uuid.UUID) error {
	if err := r.coord.Flush(); err != nil {
		slog.Error("Error flushing coordinator", "error", err)
		return err
	}
	r.pub.Forget(clientID)
	if err := r.coord.Delete(clientID); err != nil {
		return err
	}
	r.mu.Lock()
	r.flushed[clientID] = struct{}{}
	r.mu.Unlock()
	return nil
}

func (r *Router) onLeaderFlush(clientID uuid.UUID, finalSent map[broker.KeyType]int) error {
	slog.Debug("Forwarding EOF to next worker...", "clientId", clientID)
	eofCounts, err := r.pub.EncodeEOFCounts(finalSent)
	if err != nil {
		slog.Error("Error marshalling EOF counts", "error", err)
		return err
	}
	eofID := protocol.StageMsgID(clientID, r.cfg.WorkerPrefix, "eof", 0)
	return r.encodeAndSendBatch(clientID, protocol.MsgTransactionsEOF, eofCounts, broker.KeyControlEOF, 0, eofID)
}

func (r *Router) onRetryExceeded(clientID uuid.UUID) error {
	return nil
}

func (r *Router) handleTransactionMessage(envelope protocol.InternalEnvelope) error {
	r.mu.Lock()
	_, isFlushed := r.flushed[envelope.ClientId]
	r.mu.Unlock()
	if isFlushed {
		slog.Debug("Skipping transaction batch for flushed client", "client_id", envelope.ClientId)
		return nil
	}

	txBatch, err := r.pub.DecodeTransactionBatch(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding transaction batch", "error", err)
		return err
	}
	slog.Debug("Received transactions batch", "batchSize", len(txBatch), "clientId", envelope.ClientId)
	r.syncEOFController.MessageReceived(envelope.ClientId, envelope.MsgID, len(txBatch))
	transactionsPerRoutingKey := make(map[string][]protocol.Transaction)
	for _, tx := range txBatch {
		routingKey := r.shardByField(tx)
		transactionsPerRoutingKey[routingKey] = append(transactionsPerRoutingKey[routingKey], tx)
	}

	for routingKey, transactions := range transactionsPerRoutingKey {
		transactionBytes, err := r.pub.EncodeTransactionBatch(transactions)
		if err != nil {
			slog.Error("Error encoding transaction batch", "error", err)
			return err
		}

		slog.Debug("Routing transaction", "section", r.sectionToRouteBy, "field", r.fieldToRouteBy, "routingKey", routingKey)
		// Encode and send batch
		txID := protocol.DeriveMsgID(envelope.MsgID, routingKey, 0)
		key := broker.KeyType(routingKey)
		if err := r.encodeAndSendBatch(envelope.ClientId, protocol.MsgTransactionsBatch, transactionBytes, key, len(transactions), txID); err != nil {
			return err
		}
	}
	return nil
}

func (r *Router) handleEOFMessage(envelope protocol.InternalEnvelope, ack func()) error {
	r.mu.Lock()
	_, isFlushed := r.flushed[envelope.ClientId]
	r.mu.Unlock()
	if isFlushed {
		slog.Debug("Skipping EOF for flushed client", "client_id", envelope.ClientId)
		ack()
		return nil
	}

	slog.Debug("Received EOF packet, beginning syncing...", "clientId", envelope.ClientId)
	counts, err := r.pub.DecodeEOFCounts(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding EOF counts", "error", err)
		return err
	}
	r.coord.Flush()
	r.syncEOFController.SyncEof(envelope.ClientId, counts, r.syncEOFKey, ack)
	return nil
}

func (r *Router) handleMessage(msg broker.Message, ack func()) (uuid.UUID, protocol.MsgType, error) {
	return r.pub.Dispatch(msg, map[protocol.MsgType]messaging.Handler{
		protocol.MsgTransactionsBatch: r.handleTransactionMessage,
		protocol.MsgTransactionsEOF:   func(envelope protocol.InternalEnvelope) error { return r.handleEOFMessage(envelope, ack) },
	})
}
