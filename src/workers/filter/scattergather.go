package filter

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/checkpoint"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/messaging"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"

	"github.com/google/uuid"
)

var _ checkpoint.Checkpointable = (*ScatterGatherThreshold)(nil)

type ScatterGatherThreshold struct {
	pub              *messaging.Publisher
	cfg              config.WorkerConfig
	broker           broker.Broker
	thresholdAmount  int
	prevWorkerAmount int
	eofCounters      map[uuid.UUID]int
	coord            *checkpoint.Coordinator
}

func NewScatterGather(cfg config.WorkerConfig, b broker.Broker) (*ScatterGatherThreshold, error) {
	// Shared with the Q4 aggregator's degree prune via SCATTER_GATHER_THRESHOLD.
	if cfg.Threshold <= 0 {
		return nil, fmt.Errorf("ScatterGatherFilter requires SCATTER_GATHER_THRESHOLD > 0 (got %d)", cfg.Threshold)
	}

	return &ScatterGatherThreshold{
		pub:              messaging.New(codec.New(), b),
		cfg:              cfg,
		broker:           b,
		thresholdAmount:  cfg.Threshold,
		prevWorkerAmount: cfg.PrevWorkerAmount,
		eofCounters:      make(map[uuid.UUID]int),
	}, nil
}

// Este tiene state: cuenta EOFs
func (f *ScatterGatherThreshold) Run() error {
	defer f.broker.StopConsuming()

	checkpointManager, err := checkpoint.NewManager(f.cfg.CheckpointDir)
	if err != nil {
		slog.Error("Error creating checkpoint manager", "error", err)
		return err
	}
	f.coord = checkpoint.NewCoordinator(checkpointManager, f.pub, nil, f, f.cfg.CheckpointInterval)
	if err := f.coord.Recover(); err != nil {
		return err
	}

	return f.broker.StartConsuming(func(msg broker.Message, ack func(), nack func()) {
		clientID, msgType, err := f.handleMessage(msg)
		if err != nil {
			slog.Error("Error handling message", "error", err)
			nack()
			return
		}
		f.coord.Track(clientID, ack)
		if msgType == protocol.MsgTransactionsEOF {
			if err := f.coord.Flush(); err != nil {
				slog.Error("Error flushing coordinator", "error", err)
				return
			}
			if _, counting := f.eofCounters[clientID]; !counting {
				f.pub.Forget(clientID)
				if err := f.coord.Delete(clientID); err != nil {
					slog.Error("Error deleting client from coordinator", "error", err)
				}
			}
		}
	})
}

func (f *ScatterGatherThreshold) Stop() {
	f.broker.StopConsuming()
	f.broker.Close()
}

type scatterGatherThresholdCheckpoint struct {
	EOFCounters int `json:"eof_counters,omitempty"`
}

func (f *ScatterGatherThreshold) SnapshotClient(clientID uuid.UUID) ([]byte, error) {
	return json.Marshal(scatterGatherThresholdCheckpoint{EOFCounters: f.eofCounters[clientID]})
}

func (f *ScatterGatherThreshold) RestoreClient(clientID uuid.UUID, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	var cp scatterGatherThresholdCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return err
	}
	f.eofCounters[clientID] = cp.EOFCounters
	return nil
}

// Private methods

func (f *ScatterGatherThreshold) handleTxQ4Message(envelope protocol.InternalEnvelope) error {
	clientID := envelope.ClientId
	parentID := envelope.MsgID
	txQ4, err := f.pub.DecodeTxQ4PhaseThreeEnvelope(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding TxQ4PhaseThree envelope", "error", err)
		return err
	}
	slog.Debug("Received scatter-gather to filter", "clientID", clientID)

	for key, entry := range txQ4.ScatterGather {
		if entry.Count >= f.thresholdAmount {
			slog.Debug("Pair exceeds threshold", "count", entry.Count)

			accounts := []domain.Account{entry.SrcAccount, entry.DstAccount}
			// each qualifying pair is a distinct output of this parent; the pair
			// key is a stable discriminator (order-independent), so no sort needed.
			out, err := f.pub.EncodeAccountsEnvelope(clientID, accounts)
			if err != nil {
				slog.Error("Error encoding accounts envelope", "error", err)
				continue
			}
			id := protocol.DeriveMsgID(parentID, key, 0)
			if err := f.pub.PublishRawWithID(broker.KeyNil, out, id); err != nil {
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
	eofID := protocol.StageMsgID(clientID, fmt.Sprintf("%s#%d", f.cfg.WorkerPrefix, f.cfg.WorkerID), "eof", 0)
	if err := f.pub.PublishRawWithID(broker.KeyControlEOF, eofEnvelope, eofID); err != nil {
		slog.Error("Error sending EOF packet", "error", err)
		return err
	}

	delete(f.eofCounters, clientID)
	return f.coord.Flush()
}

func (f *ScatterGatherThreshold) handleMessage(msg broker.Message) (uuid.UUID, protocol.MsgType, error) {
	clientID, msgType, err := f.pub.Dispatch(msg, map[protocol.MsgType]messaging.Handler{
		protocol.MsgTxQ4:            f.handleTxQ4Message,
		protocol.MsgTransactionsEOF: f.handleEOFMessage,
	})
	return clientID, msgType, err
}
