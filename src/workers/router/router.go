package router

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/eof"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external/codec"
	"github.com/google/uuid"
)

type Router struct {
	cfg               config.WorkerConfig
	Broker            broker.Broker
	codec             codec.Codec
	syncEOFController *eof.SyncEOFController
	sectionToRouteBy  string
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

	section, field := parseRouteField(cfg.Params)
	slog.Debug("Router created", "section", section, "field", field)
	syncEOFKey := eof.SyncKeyFromInputKeys(cfg.SyncEOFConfig.InputKeys)

	return &Router{
		cfg:               cfg,
		Broker:            broker,
		codec:             codec.New(),
		syncEOFController: nil,
		sectionToRouteBy:  section,
		fieldToRouteBy:    field,
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

func (r *Router) Stop() {}

// Private methods

func (r *Router) encodeAndSendBatch(clientID uuid.UUID, msgType external.MsgType, payload []byte, routingKey broker.KeyType, batchLength int) error {
	slog.Debug("Sending batch to broker:", "batchSize", batchLength, "clientId", clientID, "msgType", msgType)
	envelope, err := r.codec.EncodeInternalEnvelope(external.InternalEnvelope{
		MsgType:  msgType,
		ClientId: clientID,
		Payload:  payload,
	})
	if err != nil {
		return fmt.Errorf("encoding internal envelope: %w", err)
	}
	err = r.Broker.Send(broker.Message{
		RoutingKey:  routingKey,
		Body:        envelope,
		ContentType: broker.ContentTypeBinary,
	})
	if err != nil {
		return fmt.Errorf("sending message to broker: %w", err)
	}
	r.syncEOFController.MessageSentWithKey(clientID, routingKey, batchLength)
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
	return nil
}

func (r *Router) onLeaderFlush(clientID uuid.UUID, finalSent map[broker.KeyType]int) error {
	slog.Debug("Forwarding EOF to next worker...", "clientId", clientID)
	eofCounts, err := r.codec.EncodeEOFCounts(finalSent)
	if err != nil {
		slog.Error("Error marshalling EOF counts", "error", err)
		return err
	}
	return r.encodeAndSendBatch(clientID, external.MsgTransactionsEOF, eofCounts, broker.KeyControlEOF, 0)
}

func (r *Router) onRetryExceeded(clientID uuid.UUID) error {
	return nil
}

func (r *Router) handleTransactionMessage(envelope external.InternalEnvelope) error {
	txBatch, err := r.codec.DecodeTransactionBatch(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding transaction batch", "error", err)
		return err
	}
	slog.Debug("Received transactions batch", "batchSize", len(txBatch), "clientId", envelope.ClientId)
	r.syncEOFController.MessageReceived(envelope.ClientId, len(txBatch))
	transactionsPerRoutingKey := make(map[string][]external.Transaction)
	for _, tx := range txBatch {
		routingKey := r.shardByField(tx)
		transactionsPerRoutingKey[routingKey] = append(transactionsPerRoutingKey[routingKey], tx)
	}

	for routingKey, transactions := range transactionsPerRoutingKey {
		transactionBytes, err := r.codec.EncodeTransactionBatch(transactions)
		if err != nil {
			slog.Error("Error encoding transaction batch", "error", err)
			return err
		}

		slog.Debug("Routing transaction", "section", r.sectionToRouteBy, "field", r.fieldToRouteBy, "routingKey", routingKey)
		// Encode and send batch
		routingKey := broker.KeyType(routingKey)
		if err := r.encodeAndSendBatch(envelope.ClientId, external.MsgTransactionsBatch, transactionBytes, routingKey, len(transactions)); err != nil {
			return err
		}
		r.syncEOFController.MessageSentWithKey(envelope.ClientId, routingKey, len(txBatch))
	}
	return nil
}

func (r *Router) handleEOFMessage(envelope external.InternalEnvelope) error {
	slog.Debug("Received EOF packet, beginning syncing...", "clientId", envelope.ClientId)
	counts, err := r.codec.DecodeEOFCounts(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding EOF counts", "error", err)
		return err
	}
	r.syncEOFController.SyncEof(envelope.ClientId, counts, r.syncEOFKey)
	return nil
}

func (r *Router) handleMessage(msg broker.Message) error {
	envelope, err := r.codec.DecodeInternalEnvelope(msg.Body)
	if err != nil {
		slog.Error("Error decoding message", "error", err)
		return err
	}

	switch envelope.MsgType {
	case external.MsgTransactionsBatch:
		return r.handleTransactionMessage(envelope)
	case external.MsgTransactionsEOF:
		return r.handleEOFMessage(envelope)
	default:
		return fmt.Errorf("unknown packet type: %v", envelope.MsgType)
	}
}
