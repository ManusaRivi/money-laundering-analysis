package filter

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/batch"
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
	avgDoneChByClient   map[uuid.UUID]chan struct{}
	avgExpectedByClient map[uuid.UUID]int
	avgReceivedByClient map[uuid.UUID]int
	txCacheByClient     map[uuid.UUID]map[string][]external.Transaction

	syncEOFController *eof.SyncEOFController
	syncEOFKey        broker.KeyType

	q3Buffer *batch.Buffer[external.Query3Result]
}

func NewAvgFormatFilter(cfg config.WorkerConfig, txBroker broker.Broker, avgBroker broker.Broker) (*AvgFormatFilter, error) {
	multiplier := defaultAvgMultiplier
	if raw, ok := cfg.Params["multiplier"]; ok {
		if v, ok := raw.(float64); ok {
			multiplier = v
		}
	}

	f := &AvgFormatFilter{
		cfg:                 cfg,
		avgBroker:           avgBroker,
		txBroker:            txBroker,
		prevWorkers:         cfg.PrevWorkerAmount,
		avgMultiplier:       multiplier,
		codec:               codec.New(),
		avgByClient:         make(map[uuid.UUID]map[string]float64),
		avgEofByClient:      make(map[uuid.UUID]int),
		avgDoneByClient:     make(map[uuid.UUID]bool),
		avgDoneChByClient:   make(map[uuid.UUID]chan struct{}),
		avgExpectedByClient: make(map[uuid.UUID]int),
		avgReceivedByClient: make(map[uuid.UUID]int),
		txCacheByClient:     make(map[uuid.UUID]map[string][]external.Transaction),
		syncEOFKey:          eof.SyncKeyFromInputKeys(cfg.SyncEOFConfig.InputKeys),
	}
	f.q3Buffer = batch.NewBuffer(batch.DefaultSize, f.flushQuery3Results)
	return f, nil
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
	f.mu.Lock()
	done := f.avgDoneByClient[clientID]
	var ch chan struct{}
	if !done {
		var exists bool
		ch, exists = f.avgDoneChByClient[clientID]
		if !exists {
			ch = make(chan struct{})
			f.avgDoneChByClient[clientID] = ch
		}
	}
	f.mu.Unlock()

	if !done {
		<-ch
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.q3Buffer.FlushClient(clientID); err != nil {
		slog.Error("Error flushing query3 buffer", "error", err)
		return err
	}
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

	updatedFormats := make(map[string]struct{}, len(avgTransactions))
	f.mu.Lock()
	if f.avgByClient[envelope.ClientId] == nil {
		f.avgByClient[envelope.ClientId] = make(map[string]float64)
	}
	for _, tx := range avgTransactions {
		if tx.PaymentFormat == "" {
			slog.Warn("Skipping avg transaction without payment format", "client_id", envelope.ClientId)
			continue
		}
		f.avgByClient[envelope.ClientId][tx.PaymentFormat] = tx.AmountPaid
		updatedFormats[tx.PaymentFormat] = struct{}{}
	}

	f.avgReceivedByClient[envelope.ClientId] += len(avgTransactions)

	cached := make([]external.Transaction, 0)
	if f.txCacheByClient[envelope.ClientId] != nil {
		for format := range updatedFormats {
			for _, tx := range f.txCacheByClient[envelope.ClientId][format] {
				cached = append(cached, tx)
			}
			delete(f.txCacheByClient[envelope.ClientId], format)
		}
		if len(f.txCacheByClient[envelope.ClientId]) == 0 {
			delete(f.txCacheByClient, envelope.ClientId)
		}
	}

	f.checkAvgDoneLocked(envelope.ClientId)

	if len(cached) == 0 {
		f.mu.Unlock()
		return nil
	}

	for _, tx := range cached {
		byFormat := f.avgByClient[envelope.ClientId]
		if byFormat == nil {
			continue
		}
		avg, ok := byFormat[tx.PaymentFormat]
		if !ok {
			continue
		}
		if result, ok := f.evaluateTransaction(tx, avg); ok {
			if err := f.q3Buffer.Add(envelope.ClientId, broker.KeyNil, result); err != nil {
				f.mu.Unlock()
				return err
			}
		}
	}
	f.mu.Unlock()

	return nil
}

func (f *AvgFormatFilter) handleTransactionBatch(envelope external.InternalEnvelope) error {
	transactions, err := f.codec.DecodeTransactionBatch(envelope.Payload)
	if err != nil {
		return err
	}

	f.mu.Lock()
	if f.txCacheByClient[envelope.ClientId] == nil {
		f.txCacheByClient[envelope.ClientId] = make(map[string][]external.Transaction)
	}
	for _, tx := range transactions {
		if tx.PaymentFormat == "" {
			continue
		}

		avg, ok := f.avgByClient[envelope.ClientId][tx.PaymentFormat]
		if !ok {
			if f.avgDoneByClient[envelope.ClientId] {
				continue
			}
			f.txCacheByClient[envelope.ClientId][tx.PaymentFormat] = append(f.txCacheByClient[envelope.ClientId][tx.PaymentFormat], tx)
			continue
		}

		if result, ok := f.evaluateTransaction(tx, avg); ok {
			if err := f.q3Buffer.Add(envelope.ClientId, broker.KeyNil, result); err != nil {
				f.mu.Unlock()
				return err
			}
		}
	}
	f.mu.Unlock()

	f.syncEOFController.MessageReceived(envelope.ClientId, len(transactions))
	return nil
}

func (f *AvgFormatFilter) evaluateTransaction(tx external.Transaction, avg float64) (external.Query3Result, bool) {
	if tx.AmountPaid >= avg * f.avgMultiplier {
		return external.Query3Result{}, false
	}
	return external.Query3Result{
		FromBank:      tx.FromBank,
		FromAccount:   tx.FromAccount,
		PaymentFormat: tx.PaymentFormat,
		AmountPaid:    tx.AmountPaid,
	}, true
}

func (f *AvgFormatFilter) flushQuery3Results(clientID uuid.UUID, key broker.KeyType, items []external.Query3Result) error {
	payload, err := f.codec.EncodeQuery3ResultBatch(items)
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
		RoutingKey:  key,
		ContentType: broker.ContentTypeBinary,
		Body:        envelope,
	}); err != nil {
		return err
	}
	f.syncEOFController.MessageSentWithKey(clientID, broker.KeyNil, len(items))
	return nil
}

func (f *AvgFormatFilter) handleAvgEOF(envelope external.InternalEnvelope) error {
	counts, err := f.codec.DecodeEOFCounts(envelope.Payload)
	if err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.avgEofByClient[envelope.ClientId]++

	var expected int
	for _, c := range counts {
		expected += c
	}
	f.avgExpectedByClient[envelope.ClientId] += expected

	f.checkAvgDoneLocked(envelope.ClientId)

	slog.Debug("Handled avg EOF", "client_id", envelope.ClientId, "eof_count", f.avgEofByClient[envelope.ClientId], "expected", f.avgExpectedByClient[envelope.ClientId], "received", f.avgReceivedByClient[envelope.ClientId])
	
	return nil
}

func (f *AvgFormatFilter) checkAvgDoneLocked(clientID uuid.UUID) {
	if f.avgDoneByClient[clientID] {
		return
	}
	if f.avgEofByClient[clientID] == f.prevWorkers && f.avgReceivedByClient[clientID] == f.avgExpectedByClient[clientID] {
		f.avgDoneByClient[clientID] = true
		f.dropUnresolvedCachedLocked(clientID)
		if ch, exists := f.avgDoneChByClient[clientID]; exists {
			close(ch)
			delete(f.avgDoneChByClient, clientID)
		}
	}
}

func (f *AvgFormatFilter) handleEOF(envelope external.InternalEnvelope) error {
	eofCounts, err := f.codec.DecodeEOFCounts(envelope.Payload)
	if err != nil {
		slog.Error("Error unmarshalling EOF counts", "error", err)
		return err
	}

	// Start syncing transactions immediately.
	f.syncEOFController.SyncEof(envelope.ClientId, eofCounts, f.syncEOFKey)
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
