package filter

import (
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
)

type Q5Filter struct {
	cfg    config.WorkerConfig
	Broker broker.Broker

	Type         string
	Field        string
	Operator     string
	ValueFloat   float64
	ValueStrings []string

	previousWorkerAmount int

	workerEofReceived     map[uuid.UUID]int
	receivedCount         map[uuid.UUID]int
	sentCount             map[uuid.UUID]int
	expectedPreviousCount map[uuid.UUID]int
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
		cfg:                   cfg,
		Broker:                b,
		Type:                  typeVal,
		Field:                 field,
		Operator:              operator,
		ValueFloat:            valueFloat,
		ValueStrings:          valueStrings,
		previousWorkerAmount:  cfg.PrevWorkerAmount,
		workerEofReceived:     make(map[uuid.UUID]int),
		receivedCount:         make(map[uuid.UUID]int),
		sentCount:             make(map[uuid.UUID]int),
		expectedPreviousCount: make(map[uuid.UUID]int),
	}, nil
}

func (f *Q5Filter) Run() error {
	defer f.Broker.StopConsuming()

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

func (f *Q5Filter) debugLogTransaction(tx domain.Transaction, clientID uuid.UUID) {
	if tx.Paid.Amount > 0.95 && tx.Paid.Amount < 1.05 {
		slog.Debug("Debug log for transaction around 1 USD", "amount", tx.Paid, "clientID", clientID, "transaction", tx)
	}
}

func (f *Q5Filter) handleTransactionMessage(pkt inner.Packet) error {
	f.receivedCount[pkt.ClientID]++
	var tx domain.Transaction
	if err := pkt.UnmarshalData(&tx); err != nil {
		return err
	}

	f.debugLogTransaction(tx, pkt.ClientID)

	if !filterTransaction(tx, f.Type, f.Operator, f.ValueFloat, f.ValueStrings) {
		slog.Debug("Transaction did not pass filter", "amount", tx.Paid.Amount, "clientID", pkt.ClientID, "transaction", tx)
		return nil
	}

	// slog.Debug("Transaction passed filter", "amount", tx.Paid, "clientID", pkt.ClientID, "transaction", tx)

	out, err := inner.MarshalTransactionPacket(pkt.ClientID, broker.KeyNil, tx)
	if err != nil {
		return err
	}
	if err := f.Broker.Send(*out); err != nil {
		return err
	}
	f.sentCount[pkt.ClientID]++
	return nil
}

func (f *Q5Filter) handleEOFMessage(pkt inner.Packet) error {
	var eofCounts domain.EOFCounts
	if err := pkt.UnmarshalData(&eofCounts); err != nil {
		slog.Error("Error unmarshalling EOF counts", "error", err)
		return err
	}

	f.workerEofReceived[pkt.ClientID]++
	f.expectedPreviousCount[pkt.ClientID] += eofCounts.Counts[broker.KeyNil]

	received := f.workerEofReceived[pkt.ClientID]
	closed := received == f.previousWorkerAmount

	slog.Debug("Q5Filter EOF received",
		"clientID", pkt.ClientID,
		"received", f.receivedCount[pkt.ClientID],
		"expected", f.expectedPreviousCount[pkt.ClientID],
	)

	if closed {
		if f.receivedCount[pkt.ClientID] != f.expectedPreviousCount[pkt.ClientID] {
			slog.Warn("EOF count mismatch for client",
				"clientID", pkt.ClientID,
				"receivedCount", f.receivedCount[pkt.ClientID],
				"expectedPreviousCount", f.expectedPreviousCount[pkt.ClientID],
			)
			// TODO: decide what to do when there's a mismatch between received count and expected count. For now, we log a warning and proceed to send the EOF downstream.
		}
		slog.Debug("Sending EOF packet downstream for client", "clientID", pkt.ClientID)

		eofMsg, err := inner.MarshalEOFPacket(pkt.ClientID, domain.EOFCounts{
			Counts: map[broker.KeyType]int{broker.KeyNil: f.sentCount[pkt.ClientID]},
		})
		if err != nil {
			slog.Error("Error marshalling EOF packet", "error", err)
			return err
		}
		slog.Debug("Sending EOF packet after processing aggregation results", "clientID", pkt.ClientID, "msg_sent", f.sentCount[pkt.ClientID])
		if err := f.Broker.Send(*eofMsg); err != nil {
			slog.Error("Error sending EOF packet", "error", err)
			return err
		}
		delete(f.workerEofReceived, pkt.ClientID)
		delete(f.receivedCount, pkt.ClientID)
		delete(f.sentCount, pkt.ClientID)
		delete(f.expectedPreviousCount, pkt.ClientID)
	}
	return nil
}
