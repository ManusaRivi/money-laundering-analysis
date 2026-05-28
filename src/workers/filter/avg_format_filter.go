package filter

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/eof"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
)

const defaultAvgMultiplier = 0.01

type AvgFormatFilter struct {
	cfg           config.WorkerConfig
	avgBroker     broker.Broker
	txBroker      broker.Broker
	prevWorkers   int
	avgMultiplier float64

	mu            sync.Mutex
	avgByClient   map[uuid.UUID]map[string]float64
	avgEofByClient map[uuid.UUID]int
	avgDoneByClient map[uuid.UUID]bool
	txCacheByClient map[uuid.UUID]map[string][]domain.Transaction
	txEOFByClient   map[uuid.UUID]bool
	pendingEOFCounts map[uuid.UUID]map[broker.KeyType]int
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
		cfg:           cfg,
		avgBroker:     avgBroker,
		txBroker:      txBroker,
		prevWorkers:   cfg.PrevWorkerAmount,
		avgMultiplier: multiplier,
		avgByClient:   make(map[uuid.UUID]map[string]float64),
		avgEofByClient: make(map[uuid.UUID]int),
		avgDoneByClient: make(map[uuid.UUID]bool),
		txCacheByClient: make(map[uuid.UUID]map[string][]domain.Transaction),
		txEOFByClient: make(map[uuid.UUID]bool),
		pendingEOFCounts: make(map[uuid.UUID]map[broker.KeyType]int),
		syncStartedByClient: make(map[uuid.UUID]bool),
		syncEOFKey:    eof.SyncKeyFromInputKeys(cfg.SyncEOFConfig.InputKeys),
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
	pkt, err := inner.UnmarshalPacket(msg)
	if err != nil {
		slog.Error("Error unmarshalling avg packet", "error", err)
		return err
	}

	switch pkt.Type {
	case inner.TypeTransaction:
		var tx domain.Transaction
		if err := pkt.UnmarshalData(&tx); err != nil {
			return err
		}
		if tx.Paid == nil {
			return fmt.Errorf("avg transaction missing paid field")
		}
		if tx.Format == "" {
			return fmt.Errorf("avg transaction missing format field")
		}
		f.mu.Lock()
		if f.avgByClient[pkt.ClientID] == nil {
			f.avgByClient[pkt.ClientID] = make(map[string]float64)
		}
		f.avgByClient[pkt.ClientID][tx.Format] = tx.Paid.Amount
		cached := f.txCacheByClient[pkt.ClientID][tx.Format]
		delete(f.txCacheByClient[pkt.ClientID], tx.Format)
		f.mu.Unlock()

		for _, cachedTx := range cached {
			if err := f.processTransaction(pkt.ClientID, cachedTx, tx.Paid.Amount); err != nil {
				return err
			}
		}
	case inner.TypeEOF:
		f.mu.Lock()
		f.avgEofByClient[pkt.ClientID]++
		if f.avgEofByClient[pkt.ClientID] >= f.prevWorkers {
			f.avgDoneByClient[pkt.ClientID] = true
			f.dropUnresolvedCachedLocked(pkt.ClientID)
		}
		f.mu.Unlock()
		f.tryStartSyncEOF(pkt.ClientID)
	default:
		return fmt.Errorf("unexpected avg packet type: %v", pkt.Type)
	}

	return nil
}

func (f *AvgFormatFilter) onflush(clientID uuid.UUID) error {
	return nil
}

func (f *AvgFormatFilter) onRetryExceeded(clientID uuid.UUID) error {
	return nil
}

func (f *AvgFormatFilter) onLeaderFlush(clientID uuid.UUID, finalSent map[broker.KeyType]int) error {
	eofMsg, err := inner.MarshalQuery3EOFPacket(clientID)
	if err != nil {
		slog.Error("Error marshalling EOF packet", "error", err)
		return err
	}
	if err := f.txBroker.Send(*eofMsg); err != nil {
		slog.Error("Error sending EOF packet to broker", "error", err)
		return err
	}
	return nil
}

func (f *AvgFormatFilter) handleTxMessage(msg broker.Message) error {
	pkt, err := inner.UnmarshalPacket(msg)
	if err != nil {
		slog.Error("Error unmarshalling packet", "error", err)
		return err
	}

	switch pkt.Type {
	case inner.TypeTransaction:
		return f.handleTransaction(pkt)
	case inner.TypeEOF:
		return f.handleEOF(pkt)
	default:
		return fmt.Errorf("unexpected inbound packet type: %v", pkt.Type)
	}
}

func (f *AvgFormatFilter) handleTransaction(pkt *inner.Packet) error {
	var tx domain.Transaction
	if err := pkt.UnmarshalData(&tx); err != nil {
		return err
	}
	if tx.Paid == nil {
		return fmt.Errorf("transaction missing paid field")
	}
	if tx.Origin == nil {
		return fmt.Errorf("transaction missing origin field")
	}
	if tx.Format == "" {
		return fmt.Errorf("transaction missing format field")
	}
	f.syncEOFController.MessageReceived(pkt.ClientID)

	f.mu.Lock()
	avg := f.avgByClient[pkt.ClientID][tx.Format]
	if avg == 0 {
		if f.avgDoneByClient[pkt.ClientID] {
			f.mu.Unlock()
			return nil
		}
		if f.txCacheByClient[pkt.ClientID] == nil {
			f.txCacheByClient[pkt.ClientID] = make(map[string][]domain.Transaction)
		}
		f.txCacheByClient[pkt.ClientID][tx.Format] = append(f.txCacheByClient[pkt.ClientID][tx.Format], tx)
		f.mu.Unlock()
		return nil
	}
	f.mu.Unlock()

	return f.processTransaction(pkt.ClientID, tx, avg)
}

func (f *AvgFormatFilter) dropUnresolvedCachedLocked(clientID uuid.UUID) {
	if f.txCacheByClient[clientID] == nil {
		return
	}
	avgs := f.avgByClient[clientID]
	for format := range f.txCacheByClient[clientID] {
		if avgs == nil || avgs[format] == 0 {
			delete(f.txCacheByClient[clientID], format)
		}
	}
	if len(f.txCacheByClient[clientID]) == 0 {
		delete(f.txCacheByClient, clientID)
	}
}

func (f *AvgFormatFilter) processTransaction(clientID uuid.UUID, tx domain.Transaction, avg float64) error {
	if tx.Paid.Amount >= avg*f.avgMultiplier {
		return nil
	}
	result := domain.Query3Result{
		FromBank:      tx.Origin.BankID,
		FromAccount:   tx.Origin.ID,
		PaymentFormat: tx.Format,
		AmountPaid:    tx.Paid.Amount,
	}
	msg, err := inner.MarshalQuery3ResultPacket(clientID, result)
	if err != nil {
		return err
	}
	if err := f.txBroker.Send(*msg); err != nil {
		return err
	}
	f.syncEOFController.MessageSentWithKey(clientID, broker.KeyNil)
	return nil
}

func (f *AvgFormatFilter) handleEOF(pkt *inner.Packet) error {
	var eofCounts domain.EOFCounts
	if err := pkt.UnmarshalData(&eofCounts); err != nil {
		slog.Error("Error unmarshalling EOF counts", "error", err)
		return err
	}
	f.mu.Lock()
	f.txEOFByClient[pkt.ClientID] = true
	f.pendingEOFCounts[pkt.ClientID] = eofCounts.Counts
	f.mu.Unlock()
	f.tryStartSyncEOF(pkt.ClientID)
	return nil
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
