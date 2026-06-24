package filter

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/eof"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/messaging"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
)

type Client struct {
	avgByFormat map[string]float64
	eofCount    int
	expected    int
	received    int
	done        bool
	doneCh      chan struct{}
}

type AvgFormatFilter struct {
	cfg           config.WorkerConfig
	avgBroker     broker.Broker
	txBroker      broker.Broker
	prevWorkers   int
	avgMultiplier float64
	pub           *messaging.Publisher

	mu      sync.Mutex
	clients map[uuid.UUID]*Client

	syncEOFController *eof.SyncEOFController
	syncEOFKey        broker.KeyType
}

func NewAvgFormatFilter(cfg config.WorkerConfig, txBroker broker.Broker, avgBroker broker.Broker) (*AvgFormatFilter, error) {
	multiplier, ok := cfg.Params["multiplier"].(float64)
	if !ok {
		return nil, fmt.Errorf("Invalid type parameter for AvgFormatFilter")
	}

	f := &AvgFormatFilter{
		cfg:           cfg,
		avgBroker:     avgBroker,
		txBroker:      txBroker,
		prevWorkers:   cfg.PrevWorkerAmount,
		avgMultiplier: multiplier,
		pub:           messaging.New(codec.New(), txBroker),
		clients:       make(map[uuid.UUID]*Client),
		syncEOFKey:    eof.SyncKeyFromInputKeys(cfg.SyncEOFConfig.InputKeys),
	}
	return f, nil
}

func (f *AvgFormatFilter) getOrCreateClientLocked(clientID uuid.UUID) *Client {
	client, ok := f.clients[clientID]
	if !ok {
		client = &Client{
			avgByFormat: make(map[string]float64),
			doneCh:      make(chan struct{}),
		}
		f.clients[clientID] = client
	}
	return client
}

func (f *AvgFormatFilter) waitAvgDone(clientID uuid.UUID) {
	f.mu.Lock()
	client := f.getOrCreateClientLocked(clientID)
	if client.done {
		f.mu.Unlock()
		return
	}
	ch := client.doneCh
	f.mu.Unlock()

	<-ch
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
	return f.pub.Dispatch(msg, map[protocol.MsgType]messaging.Handler{
		protocol.MsgTransactionsBatch: f.handleAvgBatch,
		protocol.MsgTransactionsEOF:   f.handleAvgEOF,
	})
}

func (f *AvgFormatFilter) handleAvgBatch(envelope protocol.InternalEnvelope) error {
	slog.Debug("Received avg batch", "client_id", envelope.ClientId, "payload_size", len(envelope.Payload))
	avgTransactions, err := f.pub.DecodeTransactionBatch(envelope.Payload)
	if err != nil {
		return err
	}

	f.mu.Lock()
	client := f.getOrCreateClientLocked(envelope.ClientId)
	for _, tx := range avgTransactions {
		if tx.PaymentFormat == "" {
			slog.Warn("Skipping avg transaction without payment format", "client_id", envelope.ClientId)
			continue
		}
		client.avgByFormat[tx.PaymentFormat] = tx.AmountPaid
	}
	client.received += len(avgTransactions)
	f.checkAvgDoneLocked(envelope.ClientId, client)
	f.mu.Unlock()

	return nil
}

func (f *AvgFormatFilter) handleAvgEOF(envelope protocol.InternalEnvelope) error {
	slog.Debug("Received avg EOF", "client_id", envelope.ClientId)
	counts, err := f.pub.DecodeEOFCounts(envelope.Payload)
	if err != nil {
		return err
	}

	f.mu.Lock()
	client := f.getOrCreateClientLocked(envelope.ClientId)
	client.eofCount++
	for _, c := range counts {
		client.expected += c
	}
	f.checkAvgDoneLocked(envelope.ClientId, client)
	slog.Debug("Handled avg EOF",
		"client_id", envelope.ClientId,
		"eof_count", client.eofCount,
		"expected", client.expected,
		"received", client.received,
	)
	f.mu.Unlock()

	return nil
}

func (f *AvgFormatFilter) checkAvgDoneLocked(clientID uuid.UUID, client *Client) {
	if client.done {
		return
	}
	if client.eofCount == f.prevWorkers && client.received == client.expected {
		client.done = true
		close(client.doneCh)
		slog.Debug("Avg done for client", "client_id", clientID)
	}
}

func (f *AvgFormatFilter) handleTxMessage(msg broker.Message) error {
	return f.pub.Dispatch(msg, map[protocol.MsgType]messaging.Handler{
		protocol.MsgTransactionsBatch: f.handleTransactionBatch,
		protocol.MsgTransactionsEOF:   f.handleEOF,
	})
}

func (f *AvgFormatFilter) handleTransactionBatch(envelope protocol.InternalEnvelope) error {
	slog.Debug("Received transaction batch", "client_id", envelope.ClientId, "payload_size", len(envelope.Payload))
	transactions, err := f.pub.DecodeTransactionBatch(envelope.Payload)
	if err != nil {
		return err
	}

	f.waitAvgDone(envelope.ClientId)

	f.mu.Lock()
	client := f.clients[envelope.ClientId]
	results := make([]protocol.Query3Result, 0)
	for _, tx := range transactions {
		if tx.PaymentFormat == "" {
			continue
		}
		avg, ok := client.avgByFormat[tx.PaymentFormat]
		if !ok {
			slog.Warn("No average for payment format, skipping transaction", "client_id", envelope.ClientId, "payment_format", tx.PaymentFormat)
			continue
		}
		if result, ok := f.evaluateTransaction(tx, avg); ok {
			results = append(results, result)
		}
	}
	f.mu.Unlock()

	if len(results) > 0 {
		payload, err := f.pub.EncodeQuery3ResultBatch(results)
		if err != nil {
			return err
		}
		if err := f.pub.PublishInternal(envelope.ClientId, protocol.MsgQuery3Result, broker.KeyNil, payload); err != nil {
			return err
		}
		f.syncEOFController.MessageSentWithKey(envelope.ClientId, broker.KeyNil, len(results))
	}

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

func (f *AvgFormatFilter) handleEOF(envelope protocol.InternalEnvelope) error {
	slog.Debug("Received transaction EOF", "client_id", envelope.ClientId)
	eofCounts, err := f.pub.DecodeEOFCounts(envelope.Payload)
	if err != nil {
		slog.Error("Error unmarshalling EOF counts", "error", err)
		return err
	}
	f.syncEOFController.SyncEof(envelope.ClientId, eofCounts, f.syncEOFKey)
	return nil
}

func (f *AvgFormatFilter) onflush(clientID uuid.UUID) error {
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
