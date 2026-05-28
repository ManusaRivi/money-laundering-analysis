package filter

import (
	"fmt"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
	"github.com/google/uuid"
)

type ScatterGatherThreshold struct {
	cfg              config.WorkerConfig
	broker           broker.Broker
	thresholdAmount  int
	prevWorkerAmount int
	eofCounters      map[uuid.UUID]int
}

func NewScatterGather(cfg config.WorkerConfig, b broker.Broker) (*ScatterGatherThreshold, error) {
	params := cfg.Params
	amount, ok := params["amount"].(int)
	if !ok {
		return nil, fmt.Errorf("invalid amount parameter")
	}

	return &ScatterGatherThreshold{
		cfg:              cfg,
		broker:           b,
		thresholdAmount:  amount,
		prevWorkerAmount: cfg.PrevWorkerAmount,
		eofCounters:      make(map[uuid.UUID]int),
	}, nil
}

func (f *ScatterGatherThreshold) Run() error {
	defer f.broker.StopConsuming()

	return f.broker.StartConsuming(func(msg broker.Message, ack func(), nack func()) {
		err := f.handleMessage(msg)
		if err != nil {
			slog.Error("Error handling message", "error", err)
			nack()
			return
		}
		ack()
	})
}

func (f *ScatterGatherThreshold) Stop() {
	f.broker.StopConsuming()
	f.broker.Close()
}

// Private methods

func (f *ScatterGatherThreshold) handleTxQ4Message(pkt inner.Packet) error {
	var txQ4 domain.TxQ4PhaseThree
	if err := pkt.UnmarshalData(&txQ4); err != nil {
		return err
	}
	slog.Debug("Received scatter-gather to filter", "clientID", pkt.ClientID)

	for _, entry := range txQ4.ScatterGather {
		if entry.Count >= f.thresholdAmount {
			slog.Debug("Pair exceeds threshold", "count", entry.Count)

			accounts := []domain.Account{entry.SrcAccount, entry.DstAccount}
			msg, err := inner.MarshalAccountsPacket(pkt.ClientID, broker.KeyNil, accounts)
			if err != nil {
				slog.Error("Error marshalling accounts packet", "error", err)
				continue
			}
			if err := f.broker.Send(*msg); err != nil {
				slog.Error("Error sending accounts packet to broker", "error", err)
				continue
			}
		}
	}

	return nil
}

func (f *ScatterGatherThreshold) handleEOFMessage(pkt inner.Packet) error {
	f.eofCounters[pkt.ClientID]++
	slog.Debug("Received EOF packet", "clientID", pkt.ClientID, "counter", f.eofCounters[pkt.ClientID], "target", f.prevWorkerAmount)

	if f.eofCounters[pkt.ClientID] < f.prevWorkerAmount {
		return nil
	}

	slog.Debug("All upstream EOFs received, forwarding EOF downstream", "clientID", pkt.ClientID)
	eofMsg, err := inner.MarshalEOFPacket(pkt.ClientID, domain.EOFCounts{
		Counts: map[broker.KeyType]int{broker.KeyNil: 0},
	})
	if err != nil {
		slog.Error("Error marshalling EOF packet", "error", err)
		return err
	}
	if err := f.broker.Send(*eofMsg); err != nil {
		slog.Error("Error sending EOF packet", "error", err)
		return err
	}

	delete(f.eofCounters, pkt.ClientID)
	return nil
}

func (f *ScatterGatherThreshold) handleMessage(msg broker.Message) error {
	pkt, err := inner.UnmarshalPacket(msg)

	if err != nil {
		slog.Error("Error unmarshalling packet", "error", err)
		return err
	}

	switch pkt.Type {
	case inner.TypeTxQ4:
		return f.handleTxQ4Message(*pkt)
	case inner.TypeEOF:
		return f.handleEOFMessage(*pkt)
	default:
		return fmt.Errorf("unexpected inbound packet type: %v", pkt.Type)
	}
}
