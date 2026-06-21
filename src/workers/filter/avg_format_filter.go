package filter

import (
	"log/slog"
	"sync"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/batch"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/checkpoint"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/eof"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/messaging"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
)

const defaultAvgMultiplier = 0.01

type AvgFormatFilter struct {
	cfg           config.WorkerConfig
	avgBroker     broker.Broker
	txBroker      broker.Broker
	prevWorkers   int
	avgMultiplier float64
	pub           *messaging.Publisher

	mu                  sync.Mutex
	avgByClient         map[uuid.UUID]map[string]float64
	avgEofByClient      map[uuid.UUID]int
	avgDoneByClient     map[uuid.UUID]bool
	avgDoneChByClient   map[uuid.UUID]chan struct{}
	avgExpectedByClient map[uuid.UUID]int
	avgReceivedByClient map[uuid.UUID]int
	txCacheByClient     map[uuid.UUID]map[string][]protocol.Transaction

	syncEOFController *eof.SyncEOFController
	syncEOFKey        broker.KeyType
	coord             *checkpoint.Coordinator

	q3Buffer *batch.Buffer[protocol.Query3Result]

	txGate     chan struct{}
	txGateOnce sync.Once
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
		pub:                 messaging.New(codec.New(), txBroker),
		avgByClient:         make(map[uuid.UUID]map[string]float64),
		avgEofByClient:      make(map[uuid.UUID]int),
		avgDoneByClient:     make(map[uuid.UUID]bool),
		avgDoneChByClient:   make(map[uuid.UUID]chan struct{}),
		avgExpectedByClient: make(map[uuid.UUID]int),
		avgReceivedByClient: make(map[uuid.UUID]int),
		txCacheByClient:     make(map[uuid.UUID]map[string][]protocol.Transaction),
		syncEOFKey:          eof.SyncKeyFromInputKeys(cfg.SyncEOFConfig.InputKeys),
		txGate:              make(chan struct{}),
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

	checkpointManager, err := checkpoint.NewManager(f.cfg.CheckpointDir)
	if err != nil {
		slog.Error("Error creating checkpoint manager", "error", err)
		return err
	}
	f.coord = checkpoint.NewCoordinator(checkpointManager, f.pub, f.syncEOFController, nil, f.cfg.CheckpointInterval)
	if err := f.coord.Recover(); err != nil {
		return err
	}

	go f.syncEOFController.Start()

	errCh := make(chan error, 2)

	go func() {
		errCh <- f.avgBroker.StartConsuming(func(msg broker.Message, ack func(), nack func()) {
			clientID, err := f.handleAvgMessage(msg)
			if err != nil {
				nack()
				return
			}
			f.coord.Track(clientID, ack)
		})
	}()

	go func() {
		<-f.txGate
		slog.Info("Averages complete; starting transaction consumption")
		errCh <- f.txBroker.StartConsuming(func(msg broker.Message, ack func(), nack func()) {
			clientID, err := f.handleTxMessage(msg)
			if err != nil {
				nack()
				return
			}
			f.coord.Track(clientID, ack)
		})
	}()

	return <-errCh
}

func (f *AvgFormatFilter) Stop() {}

func (f *AvgFormatFilter) handleAvgMessage(msg broker.Message) (uuid.UUID, error) {
	return f.pub.Dispatch(msg, map[protocol.MsgType]messaging.Handler{
		protocol.MsgTransactionsBatch: f.handleAvgBatch,
		protocol.MsgTransactionsEOF:   f.handleAvgEOF,
	})
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

	if err := f.pub.PublishInternal(clientID, protocol.MsgQuery3ResultEOF, broker.KeyNil, nil); err != nil {
		slog.Error("Error sending EOF packet to broker", "error", err)
		return err
	}
	return nil
}

func (f *AvgFormatFilter) handleTxMessage(msg broker.Message) (uuid.UUID, error) {
	return f.pub.Dispatch(msg, map[protocol.MsgType]messaging.Handler{
		protocol.MsgTransactionsBatch: f.handleTransactionBatch,
		protocol.MsgTransactionsEOF:   f.handleEOF,
	})
}

func (f *AvgFormatFilter) handleAvgBatch(envelope protocol.InternalEnvelope) error {
	slog.Debug("Received avg batch", "client_id", envelope.ClientId, "payload_size", len(envelope.Payload))
	avgTransactions, err := f.pub.DecodeTransactionBatch(envelope.Payload)
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

	cached := make([]protocol.Transaction, 0)
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

func (f *AvgFormatFilter) handleTransactionBatch(envelope protocol.InternalEnvelope) error {
	slog.Debug("Received transaction batch", "client_id", envelope.ClientId, "payload_size", len(envelope.Payload))
	transactions, err := f.pub.DecodeTransactionBatch(envelope.Payload)
	if err != nil {
		return err
	}

	f.mu.Lock()
	if f.txCacheByClient[envelope.ClientId] == nil {
		f.txCacheByClient[envelope.ClientId] = make(map[string][]protocol.Transaction)
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

func (f *AvgFormatFilter) evaluateTransaction(tx protocol.Transaction, avg float64) (protocol.Query3Result, bool) {
	if tx.AmountPaid >= avg*f.avgMultiplier {
		return protocol.Query3Result{}, false
	}
	return protocol.Query3Result{
		FromBank:      tx.FromBank,
		FromAccount:   tx.FromAccount,
		PaymentFormat: tx.PaymentFormat,
		AmountPaid:    tx.AmountPaid,
	}, true
}

// flushQuery3Results emits one Query3 result batch. NOTE: results are produced as
// the avg and tx streams interleave (two goroutines), so batch boundaries are
// non-deterministic — like the Q2 join, this worker can't carry a deterministic
// per-batch MsgID. Idempotency is deferred to Capa 3 (input dedup + persisted
// output ids). See docs/message-ids.md §6. PublishInternal stamps a zero MsgID
// for now (not dedup-safe yet).
func (f *AvgFormatFilter) flushQuery3Results(clientID uuid.UUID, key broker.KeyType, items []protocol.Query3Result) error {
	payload, err := f.pub.EncodeQuery3ResultBatch(items)
	if err != nil {
		return err
	}
	if err := f.pub.PublishInternal(clientID, protocol.MsgQuery3Result, key, payload); err != nil {
		return err
	}
	f.syncEOFController.MessageSentWithKey(clientID, broker.KeyNil, len(items))
	return nil
}

func (f *AvgFormatFilter) handleAvgEOF(envelope protocol.InternalEnvelope) error {
	slog.Debug("Received avg EOF", "client_id", envelope.ClientId)
	counts, err := f.pub.DecodeEOFCounts(envelope.Payload)
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
		f.txGateOnce.Do(func() { close(f.txGate) })
	}
}

func (f *AvgFormatFilter) handleEOF(envelope protocol.InternalEnvelope) error {
	slog.Debug("Received transaction EOF", "client_id", envelope.ClientId)
	eofCounts, err := f.pub.DecodeEOFCounts(envelope.Payload)
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
