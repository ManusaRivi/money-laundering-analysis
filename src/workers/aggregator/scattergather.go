package aggregator

import (
	"fmt"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external/codec"
	// "github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
	"github.com/google/uuid"
)

// Stateful
// Va almacenando y acumulando en memoria los datos:
//  accumulator = Map<(src_acc,dst_acc), int >>
//  for key in scatter-gather:
//    accumulator[key] += scatter-gather[key]

// Al recibir EOF de etapa anterior envian accumulator

const BATCH_SIZE = 1000
type clientScattergather struct {
	ID uuid.UUID
	accumulator map[domain.TxQ4PairKey]*domain.TxQ4PairEntry
}


type ScatterGather struct {
	codec codec.Codec
	cfg    config.WorkerConfig
	broker broker.Broker

	clients          map[uuid.UUID]*clientScattergather
	nextWorkerAmount int
	prevWorkerAmount int
	eofCounters      map[uuid.UUID]int
}

func NewScatterGather(cfg config.WorkerConfig, b broker.Broker) (*ScatterGather, error) {
	slog.Debug("ScatterGather created", "prevWorkerAmount", cfg.PrevWorkerAmount)

	return &ScatterGather{
		codec:            codec.New(),
		cfg:              cfg,
		broker:           b,
		clients:          make(map[uuid.UUID]*clientScattergather),
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

func (a *ScatterGather) getClient(clientID uuid.UUID) *clientScattergather {
	if c, exists := a.clients[clientID]; exists {
		return c
	}
	c := &clientScattergather{
		ID: clientID,
		accumulator: make(map[domain.TxQ4PairKey]*domain.TxQ4PairEntry),
	}
	a.clients[clientID] = c
	return c
}

func (a *ScatterGather) deleteClient(clientID uuid.UUID) {
	delete(a.clients, clientID)
}

func (a *ScatterGather) handleTxQ4Message(envelope external.InternalEnvelope) error {
	// var txQ4 domain.TxQ4PhaseTwo
	// if err := envelope.UnmarshalData(&txQ4); err != nil {
	// 	slog.Error("Error unmarshalling TxQ4 data", "error", err)
	// 	return err
	// }
	txQ4, err := a.codec.DecodeTxQ4PhaseTwoEnvelope(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding TxQ4 envelope", "error", err)
		return err
	}
	slog.Debug("Received TxQ4 message", "type", txQ4.Key)

	client := a.getClient(envelope.ClientId)
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

func (a *ScatterGather) handleEOFMessage(envelope external.InternalEnvelope) error {
	clientID := envelope.ClientId
	a.eofCounters[clientID]++
	slog.Debug("Received EOF packet", "clientID", clientID, "counter", a.eofCounters[clientID], "target", a.prevWorkerAmount)

	if a.eofCounters[clientID] < a.prevWorkerAmount {
		return nil
	}

	scatterGather := a.getClient(clientID).accumulator
	msgSent := 0
	if len(scatterGather) > 0 {
		slog.Debug("Sending Scatter-Gather to phase three", "clientID", clientID, "scatter_gather_count", len(scatterGather))


		batch := make(map[string]*domain.TxQ4PairEntry, BATCH_SIZE)
		for pk, entry := range scatterGather {
			batch[pk.Key()] = entry
			if len(batch) >= BATCH_SIZE {
				txQ4PhaseThree := domain.TxQ4PhaseThree{ScatterGather: batch}
				// msg, err := inner.MarshalTxQ4PhaseThreePacket(clientID, broker.KeyNil, txQ4PhaseThree)
				envelope, err := a.codec.EncodeTxQ4PhaseThreeEnvelope(clientID, txQ4PhaseThree)
				if err != nil {
					slog.Error("Error encoding Scatter-Gather batch", "error", err)
					return err
				}
				msg := broker.Message{
					RoutingKey:  broker.KeyNil,
					ContentType: broker.ContentTypeBinary,
					Body:        envelope,
				}
				if err := a.broker.Send(msg); err != nil {
					slog.Error("Error sending Scatter-Gather batch", "error", err)
					return err
				}
				msgSent++
				batch = make(map[string]*domain.TxQ4PairEntry, BATCH_SIZE)
			}
		}
		if len(batch) > 0 {
			txQ4PhaseThree := domain.TxQ4PhaseThree{ScatterGather: batch}
			envelope, err := a.codec.EncodeTxQ4PhaseThreeEnvelope(clientID, txQ4PhaseThree)
			if err != nil {
				slog.Error("Error encoding Scatter-Gather final batch", "error", err)
				return err
			}
			msg := broker.Message{
				RoutingKey:  broker.KeyNil,
				ContentType: broker.ContentTypeBinary,
				Body:        envelope,
			}
			if err := a.broker.Send(msg); err != nil {
				slog.Error("Error sending Scatter-Gather final batch", "error", err)
				return err
			}
			msgSent++
		}
	}

	slog.Debug("All upstream EOFs received, forwarding EOF downstream", "clientID", clientID, "msg_sent", msgSent)
	// eofMsg, err := inner.MarshalEOFPacket(clientID, domain.EOFCounts{
	// 	Counts: map[broker.KeyType]int{broker.KeyNil: msgSent},
	// })
	eofCounts := map[broker.KeyType]int{broker.KeyNil: msgSent}
	eofEnvelope, err := a.codec.EncodeEOFCountsEnvelope(clientID, eofCounts)
	if err != nil {
		slog.Error("Error encoding EOF counts envelope", "error", err)
		return err
	}
	eofMsg := broker.Message{
		RoutingKey:  broker.KeyControlEOF,
		ContentType: broker.ContentTypeBinary,
		Body:        eofEnvelope,
	}
	slog.Debug("Sending EOF packet after processing scatter and gather", "clientID", clientID, "msg_sent", msgSent)
	if err := a.broker.Send(eofMsg); err != nil {
		slog.Error("Error sending EOF packet", "error", err)
		return err
	}

	a.deleteClient(clientID)
	delete(a.eofCounters, clientID)
	return nil
}

func (a *ScatterGather) handleMessage(msg broker.Message) error {
	// pkt, err := inner.UnmarshalPacket(msg)
	envelope, err := a.codec.DecodeInternalEnvelope(msg.Body)
	if err != nil {
		slog.Error("Error decoding message", "error", err)
		return err
	}

	switch envelope.MsgType {
	case external.MsgTxQ4:
		return a.handleTxQ4Message(envelope)
	case external.MsgTransactionsEOF:
		return a.handleEOFMessage(envelope)
	default:
		slog.Warn("Received message with unknown type", "type", envelope.MsgType)
		return fmt.Errorf("unknown packet type: %v", envelope.MsgType)
	}
}
