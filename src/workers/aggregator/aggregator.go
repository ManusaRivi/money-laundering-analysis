package aggregator

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/batch"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/checkpoint"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/messaging"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
	"github.com/google/uuid"
)

var _ checkpoint.Checkpointable = (*Aggregator)(nil)

type aggCheckpoint struct {
	Count int                             `json:"count"`
	State map[string]protocol.Transaction `json:"state,omitempty"`
	Avg   map[string]avgCheckpoint        `json:"avg,omitempty"`
}

type avgCheckpoint struct {
	Sum    float64              `json:"sum"`
	Count  int                  `json:"count"`
	Sample protocol.Transaction `json:"sample"`
}

type Aggregator struct {
	cfg    config.WorkerConfig
	Broker broker.Broker
	pub    *messaging.Publisher

	coord *checkpoint.Coordinator

	aggFunction aggFunction
	field       string // field used for aggregation comparison (e.g., "Amount")
	grouped     bool   // false => single-bucket aggregation across all received transactions
	groupSource string // "origin" or "dest" (only meaningful when grouped)
	groupField  string // "BankID" or "ID"  (only meaningful when grouped)

	// countState is the running counter for the ungrouped count aggregation.
	// Indexed by clientID so concurrent clients each accumulate independently.
	countState map[uuid.UUID]int
	state      map[uuid.UUID]map[string]protocol.Transaction
	avgState   map[uuid.UUID]map[string]avgState
}

func NewAggregator(cfg config.WorkerConfig, b broker.Broker) (*Aggregator, error) {
	function, field, grouped, groupSource, groupField, err := parseParams(cfg.Params)
	if err != nil {
		return nil, err
	}

	slog.Debug("Aggregator created",
		"aggFunction", function,
		"field", field,
		"grouped", grouped,
		"group_source", groupSource,
		"group_field", groupField,
		"query", cfg.Query,
	)

	return &Aggregator{
		cfg:         cfg,
		Broker:      b,
		pub:         messaging.New(codec.New(), b),
		aggFunction: function,
		field:       field,
		grouped:     grouped,
		groupSource: groupSource,
		groupField:  groupField,
		state:       make(map[uuid.UUID]map[string]protocol.Transaction),
		countState:  make(map[uuid.UUID]int),
		avgState:    make(map[uuid.UUID]map[string]avgState),
	}, nil
}

func (a *Aggregator) Run() error {
	defer a.Broker.StopConsuming()

	coord, err := checkpoint.NewCoordinator(a.cfg.CheckpointDir, a.pub, nil, a, a.cfg.CheckpointInterval)
	if err != nil {
		slog.Error("Error creating checkpoint coordinator", "error", err)
		return err
	}
	a.coord = coord
	if err := a.coord.Recover(); err != nil {
		return err
	}

	return a.Broker.StartConsuming(func(msg broker.Message, ack, nack func()) {
		clientID, msgType, err := a.handleMessage(msg)
		if err != nil {
			slog.Error("Error handling message", "error", err)
			nack()
			return
		}
		a.coord.Track(clientID, ack)
		if msgType == protocol.MsgTransactionsEOF {
			if err := a.coord.Flush(); err != nil {
				slog.Error("Error flushing coordinator", "error", err)
				return
			}
			a.pub.Forget(clientID)
			if err := a.coord.Delete(clientID); err != nil {
				slog.Error("Error deleting client from coordinator", "error", err)
			}
		}
	})
}

func (a *Aggregator) Stop() {}

// Private methods

// Checkpoint methods

func (a *Aggregator) SnapshotClient(clientID uuid.UUID) ([]byte, error) {
	cp := aggCheckpoint{
		Count: a.countState[clientID],
		State: a.state[clientID],
	}
	if avg := a.avgState[clientID]; len(avg) > 0 {
		cp.Avg = make(map[string]avgCheckpoint, len(avg))
		for key, st := range avg {
			cp.Avg[key] = avgCheckpoint{Sum: st.sum, Count: st.count, Sample: st.sample}
		}
	}
	return json.Marshal(cp)
}

func (a *Aggregator) RestoreClient(clientID uuid.UUID, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	slog.Debug("Restoring aggregator state for client", "clientID", clientID)
	var cp aggCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return err
	}
	if cp.Count != 0 {
		a.countState[clientID] = cp.Count
	}
	if len(cp.State) > 0 {
		a.state[clientID] = cp.State
	}
	if len(cp.Avg) > 0 {
		avg := make(map[string]avgState, len(cp.Avg))
		for key, st := range cp.Avg {
			avg[key] = avgState{sum: st.Sum, count: st.Count, sample: st.Sample}
		}
		a.avgState[clientID] = avg
	}
	return nil
}

// stage returns the per-replica seed for StageMsgID: results and EOFs are
// emitted by every replica (control.eof is broadcast), so the WorkerID must be
// folded in or sibling replicas would mint colliding ids.
func (a *Aggregator) stage() string {
	return fmt.Sprintf("%s#%d", a.cfg.WorkerPrefix, a.cfg.WorkerID)
}

// sortByTransactionID gives the flushed results a deterministic order, so their
// per-batch MsgIDs (StageMsgID by index) are reproducible across runs and
// restarts — Go map iteration order is not.
func sortByTransactionID(txs []protocol.Transaction) {
	sort.Slice(txs, func(i, j int) bool {
		return txs[i].GetTransactionId() < txs[j].GetTransactionId()
	})
}

func (a *Aggregator) handleTransactionMessage(envelope protocol.InternalEnvelope) error {
	slog.Debug("Handling transaction batch message", "clientID", envelope.ClientId)
	transactions, err := a.pub.DecodeTransactionBatch(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding transaction batch", "error", err)
		return err
	}
	if a.aggFunction == opCount {
		a.countState[envelope.ClientId] += len(transactions)
		return nil
	}
	for _, tx := range transactions {
		key, err := a.extractGroupKey(tx)
		if err != nil {
			slog.Error("Error extracting group key", "error", err)
			return err
		}
		if a.aggFunction == opAvg {
			if _, exists := a.avgState[envelope.ClientId]; !exists {
				a.avgState[envelope.ClientId] = make(map[string]avgState)
			}
			current := a.avgState[envelope.ClientId][key]
			amount := a.fieldValue(tx)
			current.sum += amount
			current.count++
			if current.count == 1 {
				current.sample = tx
			}
			a.avgState[envelope.ClientId][key] = current
			continue
		}

		if _, exists := a.state[envelope.ClientId]; !exists {
			a.state[envelope.ClientId] = make(map[string]protocol.Transaction)
		}
		current, exists := a.state[envelope.ClientId][key]
		a.state[envelope.ClientId][key] = a.combine(current, tx, exists)
	}
	return nil
}

const flushBatchSize = batch.DefaultSize

func (a *Aggregator) handleEOFMessage(envelope protocol.InternalEnvelope) error {
	clientID := envelope.ClientId
	slog.Debug("Received EOF packet, processing aggregation results", "clientID", clientID)

	flushCounts, err := a.pub.DecodeEOFCounts(envelope.Payload)
	if err == nil && codec.IsFlushEOF(flushCounts) {
		delete(a.state, clientID)
		delete(a.avgState, clientID)
		return a.sendTransactionsEOF(clientID, -1)
	}

	if !a.grouped && a.aggFunction == opCount {
		return a.emitUngroupedCount(clientID)
	}

	results := a.collectResults(clientID)
	sentCount, err := a.sendTransactionBatches(clientID, results)
	if err != nil {
		slog.Error("Error sending aggregated results", "error", err, "clientID", clientID)
		return err
	}
	slog.Debug("Flushed aggregation results", "clientID", clientID, "groups", len(results), "sent", sentCount)
	if err := a.sendTransactionsEOF(clientID, sentCount); err != nil {
		return err
	}
	return a.coord.Flush()
}

func (a *Aggregator) collectResults(clientID uuid.UUID) []protocol.Transaction {
	if a.aggFunction == opAvg {
		groups := a.avgState[clientID]
		results := make([]protocol.Transaction, 0, len(groups))
		for _, st := range groups {
			out := st.sample
			if st.count > 0 {
				out.AmountPaid = st.sum / float64(st.count)
			}
			results = append(results, out)
		}
		delete(a.avgState, clientID)
		sortByTransactionID(results)
		return results
	}

	groups := a.state[clientID]
	results := make([]protocol.Transaction, 0, len(groups))
	for _, tx := range groups {
		results = append(results, tx)
	}
	delete(a.state, clientID)
	sortByTransactionID(results)
	return results
}

func (a *Aggregator) sendTransactionBatches(clientID uuid.UUID, results []protocol.Transaction) (int, error) {
	sent := 0
	batchIdx := uint32(0)
	for start := 0; start < len(results); start += flushBatchSize {
		chunk := results[start:min(start+flushBatchSize, len(results))]
		payload, err := a.pub.EncodeTransactionBatch(chunk)
		if err != nil {
			return sent, fmt.Errorf("encoding aggregated batch: %w", err)
		}
		id := protocol.StageMsgID(clientID, a.stage(), "result", batchIdx)
		if err := a.pub.PublishInternalWithID(clientID, protocol.MsgTransactionsBatch, broker.KeyNil, payload, id); err != nil {
			return sent, fmt.Errorf("sending aggregated batch: %w", err)
		}
		sent += len(chunk)
		batchIdx++
	}
	return sent, nil
}

func (a *Aggregator) sendTransactionsEOF(clientID uuid.UUID, sent int) error {
	counts, err := a.pub.EncodeEOFCounts(map[broker.KeyType]int{broker.KeyNil: sent})
	if err != nil {
		return fmt.Errorf("encoding eof counts: %w", err)
	}
	slog.Debug("Sending EOF packet after processing aggregation results", "clientID", clientID, "msg_sent", sent)
	eofID := protocol.StageMsgID(clientID, a.stage(), "eof", 0)
	return a.pub.PublishInternalWithID(clientID, protocol.MsgTransactionsEOF, broker.KeyControlEOF, counts, eofID)
}

func (a *Aggregator) handleMessage(msg broker.Message) (uuid.UUID, protocol.MsgType, error) {
	clientID, msgType, err := a.pub.Dispatch(msg, map[protocol.MsgType]messaging.Handler{
		protocol.MsgTransactionsBatch: a.handleTransactionMessage,
		protocol.MsgTransactionsEOF:   a.handleEOFMessage,
	})
	return clientID, msgType, err
}

func (a *Aggregator) emitUngroupedCount(clientID uuid.UUID) error {
	count := a.countState[clientID]
	delete(a.countState, clientID)

	resultMsg, err := a.pub.EncodeQuery5Result(protocol.Query5Result{Count: int64(count)})
	if err != nil {
		slog.Error("Error encoding query 5 result", "error", err)
		return err
	}
	resultID := protocol.StageMsgID(clientID, a.stage(), "q5result", 0)
	err = a.pub.PublishInternalWithID(clientID, protocol.MsgQuery5Result, broker.KeyNil, resultMsg, resultID)
	if err != nil {
		slog.Error("Error sending count result", "error", err)
		return err
	}
	slog.Debug("Sent count result", "clientID", clientID, "count", count, "query", a.cfg.Query)
	eofID := protocol.StageMsgID(clientID, a.stage(), "q5eof", 0)
	return a.pub.PublishInternalWithID(clientID, protocol.MsgQuery5ResultEOF, broker.KeyNil, nil, eofID)
}
