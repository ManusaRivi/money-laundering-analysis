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

// Q5Filter is the amount-filter stage of the query 5 pipeline. It combines the
// predicate logic of SyncFilter with a join-style multi-EOF barrier on the
// inbound side: it expects one EOF per upstream converter instance and only
// triggers cross-peer EOF synchronisation once all of them have arrived.
//
// Inputs flow:
//
//	N converter instances --(TypeTransaction + TypeEOF each)--> M q5_filter
//	                                                            instances
//
// Output is the original transaction (pass-through), routed to the aggregator
// stage which consumes TypeTransaction packets.
type Q5Filter struct {
	cfg    config.WorkerConfig
	Broker broker.Broker

	// Predicate parameters (parsed from cfg.Params).
	Type         string
	Field        string
	Operator     string
	ValueFloat   float64
	ValueStrings []string

	// previousWorkerAmount is the number of upstream converter instances. The
	// barrier closes once we have seen one EOF from each.
	previousWorkerAmount int

	// EOF accounting, guarded by mu. accumulatedCounts merges every inbound
	// EOFCounts.Counts map across the upstream converters so SyncEof can be
	// invoked with the full cluster expected total. syncTriggered guards
	// against double-firing if a duplicate EOF is redelivered after the
	// barrier has already closed.
	mu                sync.Mutex
	workerEofReceived map[uuid.UUID]int
	accumulatedCounts map[uuid.UUID]map[broker.KeyType]int
	syncTriggered     map[uuid.UUID]bool

	syncEOFController *eof.SyncEOFController
	syncEOFKey        broker.KeyType
}

func NewQ5Filter(cfg config.WorkerConfig, b broker.Broker) (*Q5Filter, error) {
	params := cfg.Params
	typeVal, ok := params["type"].(string)
	if !ok {
		return nil, fmt.Errorf("Invalid type parameter for Q5Filter")
	}
	field, ok := params["field"].(string)
	if !ok {
		return nil, fmt.Errorf("Invalid field parameter for Q5Filter")
	}
	operator, ok := params["operator"].(string)
	if !ok {
		return nil, fmt.Errorf("Invalid operator parameter for Q5Filter")
	}
	valueFloat, ok := params["value_float"].(float64)
	if !ok {
		return nil, fmt.Errorf("Invalid value_float parameter for Q5Filter")
	}
	valueStrings, err := parseValueStrings(params["value_string"])
	if err != nil {
		return nil, err
	}

	if cfg.PrevWorkerAmount <= 0 {
		return nil, fmt.Errorf("Q5Filter requires PREV_WORKER_AMOUNT to be set (got %d)", cfg.PrevWorkerAmount)
	}

	return &Q5Filter{
		cfg:                  cfg,
		Broker:               b,
		Type:                 typeVal,
		Field:                field,
		Operator:             operator,
		ValueFloat:           valueFloat,
		ValueStrings:         valueStrings,
		previousWorkerAmount: cfg.PrevWorkerAmount,
		workerEofReceived:    make(map[uuid.UUID]int),
		accumulatedCounts:    make(map[uuid.UUID]map[broker.KeyType]int),
		syncTriggered:        make(map[uuid.UUID]bool),
		syncEOFKey:           eof.SyncKeyFromInputKeys(cfg.SyncEOFConfig.InputKeys),
	}, nil
}

func (f *Q5Filter) Run() error {
	defer f.Broker.StopConsuming()

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

	return f.Broker.StartConsuming(func(msg broker.Message, ack func(), nack func()) {
		if err := f.handleMessage(msg); err != nil {
			slog.Error("Error handling message", "error", err)
			nack()
			return
		}
		ack()
	})
}

func (f *Q5Filter) Stop() {}

// Private methods

func (f *Q5Filter) handleMessage(msg broker.Message) error {
	pkt, err := inner.UnmarshalPacket(msg)
	if err != nil {
		slog.Error("Error unmarshalling packet", "error", err)
		return err
	}

	switch pkt.Type {
	case inner.TypeTransaction:
		return f.handleTransactionMessage(*pkt)
	case inner.TypeEOF:
		return f.handleEOFMessage(*pkt)
	default:
		return fmt.Errorf("unexpected inbound packet type: %v", pkt.Type)
	}
}

func (f *Q5Filter) handleTransactionMessage(pkt inner.Packet) error {
	var tx domain.Transaction
	if err := pkt.UnmarshalData(&tx); err != nil {
		return err
	}
	f.syncEOFController.MessageReceived(pkt.ClientID)

	if !filterTransaction(tx, f.Type, f.Operator, f.ValueFloat, f.ValueStrings) {
		return nil
	}

	// Pass-through: downstream is the aggregator, which consumes raw
	// transactions and counts them.
	out, err := inner.MarshalTransactionPacket(pkt.ClientID, broker.KeyNil, tx)
	if err != nil {
		return err
	}
	if err := f.Broker.Send(*out); err != nil {
		return err
	}
	f.syncEOFController.MessageSentWithKey(pkt.ClientID, broker.KeyNil)
	return nil
}

// handleEOFMessage implements the join-style barrier: each upstream converter
// emits its own EOF carrying its sent-count under KeyNil. We accumulate the
// counts and only invoke SyncEof once we've seen one EOF from every converter
// instance. The peer-sync protocol (SyncEOFController) then coordinates the
// final flush across q5_filter peers exactly as in SyncFilter.
func (f *Q5Filter) handleEOFMessage(pkt inner.Packet) error {
	var eofCounts domain.EOFCounts
	if err := pkt.UnmarshalData(&eofCounts); err != nil {
		slog.Error("Error unmarshalling EOF counts", "error", err)
		return err
	}

	f.mu.Lock()
	if f.syncTriggered[pkt.ClientID] {
		// A duplicate redelivery after the barrier already closed; ignore.
		f.mu.Unlock()
		slog.Debug("Q5Filter ignoring EOF after barrier closed", "clientID", pkt.ClientID)
		return nil
	}
	f.workerEofReceived[pkt.ClientID]++
	if f.accumulatedCounts[pkt.ClientID] == nil {
		f.accumulatedCounts[pkt.ClientID] = make(map[broker.KeyType]int)
	}
	for k, v := range eofCounts.Counts {
		f.accumulatedCounts[pkt.ClientID][k] += v
	}
	received := f.workerEofReceived[pkt.ClientID]
	closed := received == f.previousWorkerAmount
	var snapshot map[broker.KeyType]int
	if closed {
		f.syncTriggered[pkt.ClientID] = true
		snapshot = f.accumulatedCounts[pkt.ClientID]
	}
	f.mu.Unlock()

	slog.Debug("Q5Filter EOF received",
		"clientID", pkt.ClientID,
		"received", received,
		"expected", f.previousWorkerAmount,
	)

	if closed {
		slog.Debug("Q5Filter barrier closed, triggering peer sync",
			"clientID", pkt.ClientID, "accumulated", snapshot, "syncKey", f.syncEOFKey)
		f.syncEOFController.SyncEof(pkt.ClientID, snapshot, f.syncEOFKey)
	}
	return nil
}

// onflush is invoked on every q5_filter instance once the peer-sync protocol
// declares the cluster done for clientID. Clear per-client state.
func (f *Q5Filter) onflush(clientID uuid.UUID) error {
	f.mu.Lock()
	delete(f.workerEofReceived, clientID)
	delete(f.accumulatedCounts, clientID)
	delete(f.syncTriggered, clientID)
	f.mu.Unlock()
	return nil
}

// onLeaderFlush is invoked only on the q5_filter instance that wins the
// peer-sync leader election. It emits the downstream EOF carrying the
// cluster-wide sent counts to the aggregator.
func (f *Q5Filter) onLeaderFlush(clientID uuid.UUID, finalSent map[broker.KeyType]int) error {
	eofMsg, err := inner.MarshalEOFPacket(clientID, domain.EOFCounts{Counts: finalSent})
	if err != nil {
		slog.Error("Error marshalling EOF packet", "error", err)
		return err
	}
	slog.Debug("Q5Filter leader emitting downstream EOF", "clientID", clientID, "finalSent", finalSent)
	if err := f.Broker.Send(*eofMsg); err != nil {
		slog.Error("Error sending EOF packet to broker", "error", err)
		return err
	}
	return nil
}

func (f *Q5Filter) onRetryExceeded(clientID uuid.UUID) error {
	slog.Warn("Q5Filter exceeded EOF sync retries", "clientID", clientID)
	return nil
}
