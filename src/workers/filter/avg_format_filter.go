package filter

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/eof"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external/codec"
)

const defaultAvgMultiplier = 0.01

type AvgFormatFilter struct {
	cfg           config.WorkerConfig
	avgBroker     broker.Broker
	txBroker      broker.Broker
	prevWorkers   int
	avgMultiplier float64
	codec         codec.Codec

	mu                  sync.Mutex
	avgByClient         map[uuid.UUID]map[string]float64
	avgEofByClient      map[uuid.UUID]int
	avgDoneByClient     map[uuid.UUID]bool
	txCacheByClient     map[uuid.UUID]map[string][]external.Transaction
	txEOFByClient       map[uuid.UUID]bool
	pendingEOFCounts    map[uuid.UUID]map[broker.KeyType]int
	syncStartedByClient map[uuid.UUID]bool

	syncEOFController *eof.SyncEOFController
	syncEOFKey        broker.KeyType
}

func NewAvgFormatFilter(cfg config.WorkerConfig, txBroker broker.Broker, avgBroker broker.Broker) (*AvgFormatFilter, error) {
	multiplier := defaultAvgMultiplier
	if raw, ok := cfg.Params["multiplier"]; ok {
		if v, ok := raw.(float64); ok {
			multiplier = v
		}
	}

	return &AvgFormatFilter{
		cfg:                 cfg,
		avgBroker:           avgBroker,
		txBroker:            txBroker,
		prevWorkers:         cfg.PrevWorkerAmount,
		avgMultiplier:       multiplier,
		codec:               codec.New(),
		avgByClient:         make(map[uuid.UUID]map[string]float64),
		avgEofByClient:      make(map[uuid.UUID]int),
		avgDoneByClient:     make(map[uuid.UUID]bool),
		txCacheByClient:     make(map[uuid.UUID]map[string][]external.Transaction),
		txEOFByClient:       make(map[uuid.UUID]bool),
		pendingEOFCounts:    make(map[uuid.UUID]map[broker.KeyType]int),
		syncStartedByClient: make(map[uuid.UUID]bool),
		syncEOFKey:          eof.SyncKeyFromInputKeys(cfg.SyncEOFConfig.InputKeys),
	}, nil
}

func (f *AvgFormatFilter) Run() error {
	defer func() {
		f.txBroker.StopConsuming()
		f.avgBroker.StopConsuming()
	}()

	var err error
	f.syncEOFController, err = eof.NewSyncEOFController(
		f.cfg.SyncEOFConfig,
		f.onflush,
		f.onLeaderFlush,
		f.onRetryExceeded,
	)
	if err != nil {
		slog.Error("Error creating SyncEOFController", "error", err)
		return err
	}

	go f.syncEOFController.Start()

	errCh := make(chan error, 2)

	go func() {
		errCh <- f.avgBroker.StartConsuming(func(msg broker.Message, ack func(), nack func()) {
			if err := f.handleAvgMessage(msg); err != nil {
				nack()
				return
			}
			ack()
		})
	}()

	go func() {
		errCh <- f.txBroker.StartConsuming(func(msg broker.Message, ack func(), nack func()) {
			if err := f.handleTxMessage(msg); err != nil {
				nack()
				return
			}
			ack()
		})
	}()

	return <-errCh
}

func (f *AvgFormatFilter) Stop() {}

func (f *AvgFormatFilter) handleAvgMessage(msg broker.Message) error {
	envelope, err := f.codec.DecodeInternalEnvelope(msg.Body)
	if err != nil {
		slog.Error("Error decoding avg envelope", "error", err)
		return err
	}

	switch envelope.MsgType {
	case external.MsgTransactionsBatch:
		slog.Debug("Received avg batch", "client_id", envelope.ClientId, "payload_size", len(envelope.Payload))
		return f.handleAvgBatch(envelope)
	case external.MsgTransactionsEOF:
		slog.Debug("Received avg EOF", "client_id", envelope.ClientId)
		return f.handleAvgEOF(envelope)
	default:
		return fmt.Errorf("unexpected avg packet type: %v", envelope.MsgType)
	}
}

func (f *AvgFormatFilter) onflush(clientID uuid.UUID) error {
	return nil
}

func (f *AvgFormatFilter) onRetryExceeded(clientID uuid.UUID) error {
	return nil
}

func (f *AvgFormatFilter) onLeaderFlush(clientID uuid.UUID, finalSent map[broker.KeyType]int) error {
	slog.Debug("Handling leader flush", "client_id", clientID, "final_sent", finalSent)
	eofMsg, err := f.codec.EncodeInternalEnvelope(external.InternalEnvelope{
		MsgType:  external.MsgQuery3ResultEOF,
		ClientId: clientID,
		Payload:  nil,
	})
	if err != nil {
		slog.Error("Error marshalling EOF packet", "error", err)
		return err
	}
	if err := f.txBroker.Send(broker.Message{
		RoutingKey:  broker.KeyNil,
		ContentType: broker.ContentTypeBinary,
		Body:        eofMsg,
	}); err != nil {
		slog.Error("Error sending EOF packet to broker", "error", err)
		return err
	}
	return nil
}

func (f *AvgFormatFilter) handleTxMessage(msg broker.Message) error {
	envelope, err := f.codec.DecodeInternalEnvelope(msg.Body)
	if err != nil {
		slog.Error("Error decoding tx envelope", "error", err)
		return err
	}

	switch envelope.MsgType {
	case external.MsgTransactionsBatch:
		slog.Debug("Received transaction batch", "client_id", envelope.ClientId, "payload_size", len(envelope.Payload))
		return f.handleTransactionBatch(envelope)
	case external.MsgTransactionsEOF:
		slog.Debug("Received transaction EOF", "client_id", envelope.ClientId)
		return f.handleEOF(envelope)
	default:
		return fmt.Errorf("unexpected inbound packet type: %v", envelope.MsgType)
	}
}

func (f *AvgFormatFilter) handleAvgBatch(envelope external.InternalEnvelope) error {
	avgTransactions, err := f.codec.DecodeTransactionBatch(envelope.Payload)
	if err != nil {
		return err
	}
	for _, tx := range avgTransactions {
		if tx.PaymentFormat == "" {
			return fmt.Errorf("avg transaction missing payment format field")
		}
	}

	updatedFormats := make(map[string]struct{}, len(avgTransactions))
	f.mu.Lock()
	if f.avgByClient[envelope.ClientId] == nil {
		f.avgByClient[envelope.ClientId] = make(map[string]float64)
	}
	for _, tx := range avgTransactions {
		f.avgByClient[envelope.ClientId][tx.PaymentFormat] = tx.AmountPaid
		updatedFormats[tx.PaymentFormat] = struct{}{}
	}
	cached := make([]external.Transaction, 0)
	if f.txCacheByClient[envelope.ClientId] != nil {
		for format := range updatedFormats {
			cached = append(cached, f.txCacheByClient[envelope.ClientId][format]...)
			delete(f.txCacheByClient[envelope.ClientId], format)
		}
		if len(f.txCacheByClient[envelope.ClientId]) == 0 {
			delete(f.txCacheByClient, envelope.ClientId)
		}
	}
	f.mu.Unlock()

	if len(cached) == 0 {
		return nil
	}

	results := make([]external.Query3Result, 0, len(cached))
	for _, tx := range cached {
		avg, ok := f.lookupAvg(envelope.ClientId, tx.PaymentFormat)
		if !ok {
			continue
		}
		if result, ok := f.evaluateTransaction(tx, avg); ok {
			results = append(results, result)
		}
	}

	return f.sendQuery3Results(envelope.ClientId, results)
}

func (f *AvgFormatFilter) handleTransactionBatch(envelope external.InternalEnvelope) error {
	transactions, err := f.codec.DecodeTransactionBatch(envelope.Payload)
	if err != nil {
		return err
	}
	for _, tx := range transactions {
		if tx.FromBank == "" {
			return fmt.Errorf("transaction missing from bank field")
		}
		if tx.FromAccount == "" {
			return fmt.Errorf("transaction missing from account field")
		}
		if tx.PaymentFormat == "" {
			return fmt.Errorf("transaction missing payment format field")
		}
	}

	f.syncEOFController.MessageReceived(envelope.ClientId, len(transactions))

	results := make([]external.Query3Result, 0, len(transactions))

	f.mu.Lock()
	if f.txCacheByClient[envelope.ClientId] == nil {
		f.txCacheByClient[envelope.ClientId] = make(map[string][]external.Transaction)
	}
	for _, tx := range transactions {
		avg, ok := f.avgByClient[envelope.ClientId][tx.PaymentFormat]
		if !ok {
			if f.avgDoneByClient[envelope.ClientId] {
				continue
			}
			f.txCacheByClient[envelope.ClientId][tx.PaymentFormat] = append(f.txCacheByClient[envelope.ClientId][tx.PaymentFormat], tx)
			continue
		}

		if result, ok := f.evaluateTransaction(tx, avg); ok {
			results = append(results, result)
		}
	}
	f.mu.Unlock()

	return f.sendQuery3Results(envelope.ClientId, results)
}

func (f *AvgFormatFilter) evaluateTransaction(tx external.Transaction, avg float64) (external.Query3Result, bool) {
	if tx.AmountPaid >= avg*f.avgMultiplier {
		return external.Query3Result{}, false
	}
	return external.Query3Result{
		FromBank:      tx.FromBank,
		FromAccount:   tx.FromAccount,
		PaymentFormat: tx.PaymentFormat,
		AmountPaid:    tx.AmountPaid,
	}, true
}

func (f *AvgFormatFilter) lookupAvg(clientID uuid.UUID, format string) (float64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	byFormat := f.avgByClient[clientID]
	if byFormat == nil {
		return 0, false
	}
	avg, ok := byFormat[format]
	return avg, ok
}

func (f *AvgFormatFilter) sendQuery3Results(clientID uuid.UUID, results []external.Query3Result) error {
	if len(results) == 0 {
		return nil
	}

	payload, err := f.codec.EncodeQuery3ResultBatch(results)
	if err != nil {
		return err
	}
	envelope, err := f.codec.EncodeInternalEnvelope(external.InternalEnvelope{
		MsgType:  external.MsgQuery3Result,
		ClientId: clientID,
		Payload:  payload,
	})
	if err != nil {
		return err
	}
	if err := f.txBroker.Send(broker.Message{
		RoutingKey:  broker.KeyNil,
		ContentType: broker.ContentTypeBinary,
		Body:        envelope,
	}); err != nil {
		return err
	}
	f.syncEOFController.MessageSentWithKey(clientID, broker.KeyNil, len(results))
	return nil
}

func (f *AvgFormatFilter) handleAvgEOF(envelope external.InternalEnvelope) error {
	if _, err := f.codec.DecodeEOFCounts(envelope.Payload); err != nil {
		return err
	}

	f.mu.Lock()
	f.avgEofByClient[envelope.ClientId]++
	if f.avgEofByClient[envelope.ClientId] >= f.prevWorkers {
		f.avgDoneByClient[envelope.ClientId] = true
		f.dropUnresolvedCachedLocked(envelope.ClientId)
	}
	f.mu.Unlock()

	f.tryStartSyncEOF(envelope.ClientId)
	return nil
}

func (f *AvgFormatFilter) handleEOF(envelope external.InternalEnvelope) error {
	eofCounts, err := f.codec.DecodeEOFCounts(envelope.Payload)
	if err != nil {
		slog.Error("Error unmarshalling EOF counts", "error", err)
		return err
	}
	f.mu.Lock()
	f.txEOFByClient[envelope.ClientId] = true
	f.pendingEOFCounts[envelope.ClientId] = eofCounts
	f.mu.Unlock()
	f.tryStartSyncEOF(envelope.ClientId)
	return nil
}

func (f *AvgFormatFilter) dropUnresolvedCachedLocked(clientID uuid.UUID) {
	if f.txCacheByClient[clientID] == nil {
		return
	}
	avgs := f.avgByClient[clientID]
	for format := range f.txCacheByClient[clientID] {
		if avgs == nil {
			delete(f.txCacheByClient[clientID], format)
			continue
		}
		if _, ok := avgs[format]; !ok {
			delete(f.txCacheByClient[clientID], format)
		}
	}
	if len(f.txCacheByClient[clientID]) == 0 {
		delete(f.txCacheByClient, clientID)
	}
}

func (f *AvgFormatFilter) tryStartSyncEOF(clientID uuid.UUID) {
	f.mu.Lock()
	if f.syncStartedByClient[clientID] {
		f.mu.Unlock()
		return
	}
	if !f.txEOFByClient[clientID] || !f.avgDoneByClient[clientID] {
		f.mu.Unlock()
		return
	}
	counts := f.pendingEOFCounts[clientID]
	f.syncStartedByClient[clientID] = true
	f.mu.Unlock()

	f.syncEOFController.SyncEof(clientID, counts, f.syncEOFKey)
}
