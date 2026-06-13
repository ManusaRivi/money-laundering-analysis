package filter

import (
	"fmt"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/messaging"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"

	// "github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
	"github.com/google/uuid"
)

type ScatterGatherThreshold struct {
	pub              *messaging.Publisher
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
		pub:              messaging.New(codec.New(), b),
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

func (f *ScatterGatherThreshold) handleTxQ4Message(envelope protocol.InternalEnvelope) error {
	// var txQ4 domain.TxQ4PhaseThree
	// if err := pkt.UnmarshalData(&txQ4); err != nil {
	// 	return err
	// }
	clientID := envelope.ClientId
	txQ4, err := f.pub.DecodeTxQ4PhaseThreeEnvelope(envelope.Payload)
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
			envelope, err := f.pub.EncodeAccountsEnvelope(clientID, accounts)
			if err != nil {
				slog.Error("Error encoding accounts envelope", "error", err)
				continue
			}
			if err := f.pub.PublishRaw(broker.KeyNil, envelope); err != nil {
				slog.Error("Error sending accounts envelope to broker", "error", err)
				continue
			}
		}
	}

	return nil
}

func (f *ScatterGatherThreshold) handleEOFMessage(envelope protocol.InternalEnvelope) error {
	clientID := envelope.ClientId
	f.eofCounters[clientID]++
	slog.Debug("Received EOF packet", "clientID", clientID, "counter", f.eofCounters[clientID], "target", f.prevWorkerAmount)

	if f.eofCounters[clientID] < f.prevWorkerAmount {
		return nil
	}

	slog.Debug("All upstream EOFs received, forwarding EOF downstream", "clientID", clientID)
	// eofMsg, err := inner.MarshalEOFPacket(clientID, domain.EOFCounts{

	eofCounts := map[broker.KeyType]int{broker.KeyNil: 0}
	eofEnvelope, err := f.pub.EncodeEOFCountsEnvelope(clientID, eofCounts)
	if err != nil {
		slog.Error("Error encoding EOF counts envelope", "error", err)
		return err
	}
	if err := f.pub.PublishRaw(broker.KeyControlEOF, eofEnvelope); err != nil {
		slog.Error("Error sending EOF packet", "error", err)
		return err
	}

	delete(f.eofCounters, clientID)
	return nil
}

func (f *ScatterGatherThreshold) handleMessage(msg broker.Message) error {
	return f.pub.Dispatch(msg, map[protocol.MsgType]messaging.Handler{
		protocol.MsgTxQ4:            f.handleTxQ4Message,
		protocol.MsgTransactionsEOF: f.handleEOFMessage,
	})
}
