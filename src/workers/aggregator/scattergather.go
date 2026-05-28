package aggregator

import (
	"fmt"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
	"github.com/google/uuid"
)

// Stateful
// Va almacenando y acumulando en memoria los datos:
//  accumulator = Map<(src_acc,dst_acc), int >>
//  for key in scatter-gather:
//    accumulator[key] += scatter-gather[key]

// Al recibir EOF de etapa anterior envian accumulator


type clientscattergather struct {
	ID uuid.UUID
	accumulator map[domain.TxQ4PairKey]*domain.TxQ4PairEntry
}


type ScatterGather struct {
	cfg    config.WorkerConfig
	broker broker.Broker

	clients          map[uuid.UUID]*clientscattergather
	nextWorkerAmount int
	prevWorkerAmount int
	eofCounters      map[uuid.UUID]int
}

func NewScatterGather(cfg config.WorkerConfig, b broker.Broker) (*ScatterGather, error) {
	slog.Debug("ScatterGather created", "prevWorkerAmount", cfg.PrevWorkerAmount)

	return &ScatterGather{
		cfg:              cfg,
		broker:           b,
		clients:          make(map[uuid.UUID]*clientscattergather),
		nextWorkerAmount: cfg.NextWorkerAmount,
		prevWorkerAmount: cfg.PrevWorkerAmount,
		eofCounters:      make(map[uuid.UUID]int),
	}, nil
}

func (a *ScatterGather) Run() error {
	defer a.broker.StopConsuming()

	return a.broker.StartConsuming(func(msg broker.Message, ack, nack func()) {
		if err := a.handleMessage(msg); err != nil {
			slog.Error("Error handling message", "error", err)
			nack()
			return
		}
		ack()
	})
}

func (a *ScatterGather) Stop() {
	a.broker.StopConsuming()
	a.broker.Close()
}

// Private Methods

func (a *ScatterGather) getClient(clientID uuid.UUID) *clientscattergather {
	if c, exists := a.clients[clientID]; exists {
		return c
	}
	c := &clientscattergather{
		ID: clientID,
		accumulator: make(map[domain.TxQ4PairKey]*domain.TxQ4PairEntry),
	}
	a.clients[clientID] = c
	return c
}

func (a *ScatterGather) deleteClient(clientID uuid.UUID) {
	delete(a.clients, clientID)
}

func (a *ScatterGather) handleTxQ4Message(pkt inner.Packet) error {
	var txQ4 domain.TxQ4PhaseTwo
	if err := pkt.UnmarshalData(&txQ4); err != nil {
		slog.Error("Error unmarshalling TxQ4 data", "error", err)
		return err
	}

	slog.Debug("Received TxQ4 message", "type", txQ4.Key)

	client := a.getClient(pkt.ClientID)
	entry, ok := client.accumulator[txQ4.Key]
	if !ok {
		entry = &domain.TxQ4PairEntry{
			SrcAccount: *txQ4.SrcAccount,
			DstAccount: *txQ4.DstAccount,
		}
		client.accumulator[txQ4.Key] = entry
	}
	entry.Count += txQ4.Count

	return nil

}

func (a *ScatterGather) handleEOFMessage(pkt inner.Packet) error {
	a.eofCounters[pkt.ClientID]++
	slog.Debug("Received EOF packet", "clientID", pkt.ClientID, "counter", a.eofCounters[pkt.ClientID], "target", a.prevWorkerAmount)

	if a.eofCounters[pkt.ClientID] < a.prevWorkerAmount {
		return nil
	}

	scatterGather := a.getClient(pkt.ClientID).accumulator
	if len(scatterGather) > 0 {
		txQ4PhaseThree := domain.MakePhaseThree(scatterGather)
		slog.Debug("Sending Scatter-Gather to phase three", "clientID", pkt.ClientID, "scatter_gather_count", len(scatterGather))

		msg, err := inner.MarshalTxQ4PhaseThreePacket(pkt.ClientID, broker.KeyNil, txQ4PhaseThree)
		if err != nil {
			slog.Error("Error marshalling Scatter-Gather to phase three", "error", err)
			return err
		}
		if err := a.broker.Send(*msg); err != nil {
			slog.Error("Error sending Scatter-Gather to phase three", "error", err)
			return err
		}
	}

	slog.Debug("All upstream EOFs received, forwarding EOF downstream", "clientID", pkt.ClientID)
	eofMsg, err := inner.MarshalEOFPacket(pkt.ClientID, domain.EOFCounts{
		Counts: map[broker.KeyType]int{broker.KeyNil: 1},
	})
	if err != nil {
		slog.Error("Error marshalling EOF packet", "error", err)
		return err
	}
	if err := a.broker.Send(*eofMsg); err != nil {
		slog.Error("Error sending EOF packet", "error", err)
		return err
	}

	a.deleteClient(pkt.ClientID)
	delete(a.eofCounters, pkt.ClientID)
	return nil
}

func (a *ScatterGather) handleMessage(msg broker.Message) error {
	pkt, err := inner.UnmarshalPacket(msg)
	if err != nil {
		slog.Error("Error unmarshalling message", "error", err)
		return err
	}

	switch pkt.Type {
	case inner.TypeTxQ4:
		return a.handleTxQ4Message(*pkt)
	case inner.TypeEOF:
		return a.handleEOFMessage(*pkt)
	default:
		slog.Warn("Received message with unknown type", "type", pkt.Type)
		return fmt.Errorf("unknown packet type: %v", pkt.Type)
	}
}
