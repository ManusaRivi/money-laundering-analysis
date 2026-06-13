package filter

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

type ScatterGatherThreshold struct {
	codec 		 codec.Codec
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
		codec:           codec.New(),
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

func (f *ScatterGatherThreshold) handleTxQ4Message(envelope external.InternalEnvelope) error {
	// var txQ4 domain.TxQ4PhaseThree
	// if err := pkt.UnmarshalData(&txQ4); err != nil {
	// 	return err
	// }
	clientID := envelope.ClientId
	txQ4, err := f.codec.DecodeTxQ4PhaseThreeEnvelope(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding TxQ4PhaseThree envelope", "error", err)
		return err
	}
	slog.Debug("Received scatter-gather to filter", "clientID", clientID)

	for _, entry := range txQ4.ScatterGather {
		if entry.Count >= f.thresholdAmount {
			slog.Debug("Pair exceeds threshold", "count", entry.Count)

			accounts := []domain.Account{entry.SrcAccount, entry.DstAccount}
			// msg, err := inner.MarshalAccountsPacket(clientID, broker.KeyNil, accounts)
			envelope, err := f.codec.EncodeAccountsEnvelope(clientID, accounts)
			 if err != nil {
				slog.Error("Error encoding accounts envelope", "error", err)
				continue
			}
			msg := broker.Message{
				RoutingKey:   broker.KeyNil,
				ContentType: broker.ContentTypeBinary,
				Body: envelope,
			}
			if err := f.broker.Send(msg); err != nil {
				slog.Error("Error sending accounts envelope to broker", "error", err)
				continue
			}
		}
	}

	return nil
}

func (f *ScatterGatherThreshold) handleEOFMessage(envelope external.InternalEnvelope) error {
	clientID := envelope.ClientId
	f.eofCounters[clientID]++
	slog.Debug("Received EOF packet", "clientID", clientID, "counter", f.eofCounters[clientID], "target", f.prevWorkerAmount)

	if f.eofCounters[clientID] < f.prevWorkerAmount {
		return nil
	}

	slog.Debug("All upstream EOFs received, forwarding EOF downstream", "clientID", clientID)
	// eofMsg, err := inner.MarshalEOFPacket(clientID, domain.EOFCounts{

	eofCounts := map[broker.KeyType]int{broker.KeyNil: 0}
	eofEnvelope, err := f.codec.EncodeEOFCountsEnvelope(clientID, eofCounts)
	if err != nil {
		slog.Error("Error encoding EOF counts envelope", "error", err)
		return err
	}
	msg := broker.Message{
		RoutingKey:  broker.KeyControlEOF,
		ContentType: broker.ContentTypeBinary,
		Body:        eofEnvelope,
	}
	if err := f.broker.Send(msg); err != nil {
		slog.Error("Error sending EOF packet", "error", err)
		return err
	}

	delete(f.eofCounters, clientID)
	return nil
}

func (f *ScatterGatherThreshold) handleMessage(msg broker.Message) error {
	// pkt, err := inner.UnmarshalPacket(msg)
	envelope, err := f.codec.DecodeInternalEnvelope(msg.Body)
	if err != nil {
		slog.Error("Error decoding internal envelope", "error", err)
		return err
	}

	switch envelope.MsgType {
	case external.MsgTxQ4:
		return f.handleTxQ4Message(envelope)
	case external.MsgTransactionsEOF:
		return f.handleEOFMessage(envelope)
	default:
		return fmt.Errorf("unexpected inbound packet type: %v", envelope.MsgType)
	}
}
