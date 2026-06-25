package join

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/checkpoint"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/messaging"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
)

var _ checkpoint.Checkpointable = (*Join)(nil)

type joinCheckpoint struct {
	BankNames   map[string]string `json:"bank_names,omitempty"`
	TxCache     []cachedBatch     `json:"tx_cache,omitempty"`
	WorkerEOF   int               `json:"worker_eof,omitempty"`
	AccountsEOF bool              `json:"accounts_eof,omitempty"`
	Finalized   bool              `json:"finalized,omitempty"`
}

// cachedBatch holds a transaction batch that arrived before the accounts were
// complete, together with its input MsgID so the join it eventually produces can
// be emitted with a deterministic, restart-stable id.
type cachedBatch struct {
	MsgID protocol.MsgID         `json:"msg_id"`
	Txs   []protocol.Transaction `json:"txs"`
}

// Join builds Query2Result records by joining max-amount transactions (received
// from an upstream sum aggregator) with bank account info (received directly
// from the gateway on a separate queue).
type Join struct {
	cfg            config.WorkerConfig
	resultsBroker  broker.Broker
	accountsBroker broker.Broker
	pub            *messaging.Publisher

	coord *checkpoint.Coordinator

	previousWorkerAmount int

	// procMu serializes the two consumer goroutines: it is held across the whole
	// handle + Track sequence (see process), so only one message is in flight at
	// a time and a checkpoint flush (inside Track) can never interleave with the
	// other goroutine's handler and capture an inconsistent state/dedup snapshot.
	// All maps below — and SnapshotClient/RestoreClient — are covered by it; the
	// snapshot/restore paths run under it (flush) or at startup, so they do not
	// re-acquire it.
	procMu              sync.Mutex
	workerEofReceived   map[uuid.UUID]int
	accountsEofReceived map[uuid.UUID]bool
	finalized           map[uuid.UUID]bool
	bankNamesPerCli     map[uuid.UUID]map[string]string
	// txCachePerCl buffers transaction batches only while the bank table is still
	// incomplete (before the accounts EOF). Once accounts are complete it is
	// drained and stays empty — later batches are joined and emitted on arrival.
	txCachePerCl map[uuid.UUID][]cachedBatch
}

func NewJoin(cfg config.WorkerConfig, resultsBroker broker.Broker) (*Join, error) {
	accountsConfigPath := os.Getenv("ACCOUNTS_CONFIG_PATH")
	accountCfg, err := config.LoadAccountConfig(accountsConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load accounts config: %w", err)
	}
	var accountsBroker broker.Broker
	accountsBroker, err = broker.NewBroker(*accountCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create accounts broker: %w", err)
	}
	return &Join{
		cfg:                  cfg,
		resultsBroker:        resultsBroker,
		accountsBroker:       accountsBroker,
		pub:                  messaging.New(codec.New(), resultsBroker),
		previousWorkerAmount: cfg.PrevWorkerAmount,
		workerEofReceived:    make(map[uuid.UUID]int),
		accountsEofReceived:  make(map[uuid.UUID]bool),
		finalized:            make(map[uuid.UUID]bool),
		bankNamesPerCli:      make(map[uuid.UUID]map[string]string),
		txCachePerCl:         make(map[uuid.UUID][]cachedBatch),
	}, nil
}

func (j *Join) Run() error {
	defer func() {
		j.resultsBroker.StopConsuming()
		j.accountsBroker.StopConsuming()
	}()

	errCh := make(chan error, 2)

	checkpointManager, err := checkpoint.NewManager(j.cfg.CheckpointDir)
	if err != nil {
		slog.Error("Error creating checkpoint manager", "error", err)
		return err
	}
	j.coord = checkpoint.NewCoordinator(checkpointManager, j.pub, nil, j, j.cfg.CheckpointInterval)
	if err := j.coord.Recover(); err != nil {
		return err
	}

	go func() {
		errCh <- j.accountsBroker.StartConsuming(func(msg broker.Message, ack, nack func()) {
			j.process(j.handleAccountsMessage, msg, ack, nack)
		})
	}()

	go func() {
		errCh <- j.resultsBroker.StartConsuming(func(msg broker.Message, ack, nack func()) {
			j.process(j.handleTransactionMessage, msg, ack, nack)
		})
	}()

	return <-errCh
}

func (j *Join) Stop() {}

// Private Methods

// process serializes the two consumer goroutines. procMu is held across the
// whole handle + Track sequence so a checkpoint flush cannot interleave with the
// sibling goroutine's handler — which would snapshot state and dedup at
// different instants and (for the EOF counters) double-count on restart.
func (j *Join) process(handle func(broker.Message) (uuid.UUID, protocol.MsgType, error), msg broker.Message, ack, nack func()) {
	j.procMu.Lock()
	defer j.procMu.Unlock()
	clientID, msgType, err := handle(msg)
	if err != nil {
		slog.Error("Error handling join message", "error", err)
		nack()
		return
	}
	if err := j.coord.Track(clientID, ack); err != nil {
		slog.Error("Error tracking message in checkpoint coordinator", "error", err)
	}
	if msgType == protocol.MsgTransactionsEOF {
		if err := j.coord.Flush(); err != nil {
			slog.Error("Error flushing checkpoint coordinator", "error", err)
		}
	}
}

func toQuery2Result(tx protocol.Transaction, bankName string) protocol.Query2Result {
	return protocol.Query2Result{
		FromBank:    tx.FromBank,
		FromAccount: tx.FromAccount,
		BankName:    bankName,
		AmountPaid:  tx.AmountPaid,
	}
}

// joinAndEmit joins one transaction batch against the (complete) bank table and
// emits the results as a single Query2 batch. The output id is derived from the
// input batch's MsgID, so the same input always yields the same output id — a
// re-emission after a restart (immediate or drained) is collapsed downstream.
// Banks absent from the accounts dataset are dropped. Caller holds procMu.
func (j *Join) joinAndEmit(clientID uuid.UUID, inputMsgID protocol.MsgID, txs []protocol.Transaction, bankNames map[string]string) error {
	results := make([]protocol.Query2Result, 0, len(txs))
	for _, tx := range txs {
		name, known := bankNames[tx.FromBank]
		if !known {
			slog.Warn("Dropping transaction with no matching bank", "clientID", clientID, "bankID", tx.FromBank)
			continue
		}
		results = append(results, toQuery2Result(tx, name))
	}
	if len(results) == 0 {
		return nil
	}
	payload, err := j.pub.EncodeQuery2ResultBatch(results)
	if err != nil {
		slog.Error("Error encoding query result batch", "error", err)
		return err
	}
	id := protocol.DeriveMsgID(inputMsgID, "q2result", 0)
	if err := j.pub.PublishInternalWithID(clientID, protocol.MsgQuery2Result, broker.KeyNil, payload, id); err != nil {
		slog.Error("Error sending message to broker", "error", err)
		return err
	}
	return nil
}

func (j *Join) handleAccountsBatch(envelope protocol.InternalEnvelope) error {
	accounts, err := j.pub.DecodeAccountBatch(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding accounts batch", "error", err)
		return err
	}
	clientID := envelope.ClientId
	slog.Debug("Received accounts batch", "clientID", clientID, "batchLen", len(accounts))

	bankNames := j.bankNamesPerCli[clientID]
	if bankNames == nil {
		bankNames = make(map[string]string)
		j.bankNamesPerCli[clientID] = bankNames
	}
	for _, info := range accounts {
		if _, ok := bankNames[info.BankID]; !ok {
			bankNames[info.BankID] = info.BankName
		}
	}
	return nil
}

// handleAccountsEOF marks the bank table complete and drains everything cached
// while it was still incomplete, then drops the cache. From here on transactions
// are joined on arrival (handleTransactionBatch), so nothing else is buffered.
func (j *Join) handleAccountsEOF(envelope protocol.InternalEnvelope) error {
	clientID := envelope.ClientId
	slog.Debug("Received accounts EOF for client", "clientID", clientID)
	j.accountsEofReceived[clientID] = true

	bankNames := j.bankNamesPerCli[clientID]
	for _, cb := range j.txCachePerCl[clientID] {
		if err := j.joinAndEmit(clientID, cb.MsgID, cb.Txs, bankNames); err != nil {
			return err
		}
	}
	delete(j.txCachePerCl, clientID)

	if j.shouldFinalize(clientID) {
		return j.finalizeClient(clientID)
	}
	return nil
}

func (j *Join) handleAccountsMessage(msg broker.Message) (uuid.UUID, protocol.MsgType, error) {
	return j.pub.Dispatch(msg, map[protocol.MsgType]messaging.Handler{
		protocol.MsgAccountsBatch: j.handleAccountsBatch,
		protocol.MsgAccountsEOF:   j.handleAccountsEOF,
	})
}

// handleTransactionBatch joins on arrival once the bank table is complete; until
// then it caches the whole batch (with its id) so it can be drained
// deterministically at the accounts EOF.
func (j *Join) handleTransactionBatch(envelope protocol.InternalEnvelope) error {
	transactions, err := j.pub.DecodeTransactionBatch(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding transaction batch", "error", err)
		return err
	}
	clientID := envelope.ClientId
	slog.Debug("Received transaction batch for join", "batchLen", len(transactions))

	if !j.accountsEofReceived[clientID] {
		j.txCachePerCl[clientID] = append(j.txCachePerCl[clientID], cachedBatch{MsgID: envelope.MsgID, Txs: transactions})
		return nil
	}
	return j.joinAndEmit(clientID, envelope.MsgID, transactions, j.bankNamesPerCli[clientID])
}

func (j *Join) handleTransactionsEOF(envelope protocol.InternalEnvelope) error {
	clientID := envelope.ClientId
	slog.Debug("Received transaction EOF for client", "clientID", clientID)
	slog.Debug("Sleeping...")
	time.Sleep(5 * time.Second)
	j.workerEofReceived[clientID]++
	slog.Debug("Current EOF count", "clientID", clientID, "workerEofReceived", j.workerEofReceived[clientID], "previousWorkerAmount", j.previousWorkerAmount)
	if j.shouldFinalize(clientID) {
		return j.finalizeClient(clientID)
	}
	return j.coord.Flush()
}

func (j *Join) handleTransactionMessage(msg broker.Message) (uuid.UUID, protocol.MsgType, error) {
	return j.pub.Dispatch(msg, map[protocol.MsgType]messaging.Handler{
		protocol.MsgTransactionsBatch: j.handleTransactionBatch,
		protocol.MsgTransactionsEOF:   j.handleTransactionsEOF,
	})
}

// shouldFinalize reports whether both the worker-side EOF barrier and the
// accounts-side EOF have been reached for the client, and atomically claims the
// finalize so the two consumer goroutines won't both run it. Caller holds procMu.
func (j *Join) shouldFinalize(clientID uuid.UUID) bool {
	if j.finalized[clientID] {
		return false
	}
	if j.workerEofReceived[clientID] != j.previousWorkerAmount {
		return false
	}
	if !j.accountsEofReceived[clientID] {
		return false
	}
	j.finalized[clientID] = true
	return true
}

// finalizeClient runs once per client when both EOF barriers are met. By then
// all results have already been emitted (drained at the accounts EOF and/or
// joined on arrival), so it only emits the terminal Query2 EOF and tears the
// client's state down. Caller holds procMu.
func (j *Join) finalizeClient(clientID uuid.UUID) error {
	eofID := protocol.StageMsgID(clientID, j.cfg.WorkerPrefix, "eof", 0)
	if err := j.pub.PublishInternalWithID(clientID, protocol.MsgQuery2ResultEOF, broker.KeyNil, nil, eofID); err != nil {
		slog.Error("Error sending Query2 EOF to broker", "error", err)
		return err
	}
	slog.Debug("Emitted Query2 EOF for client", "clientID", clientID)
	slog.Debug("Sleeping...")
	time.Sleep(5 * time.Second)

	// Keep `finalized` as the terminal marker so a redelivered EOF won't
	// re-finalize a client we've already completed.
	delete(j.bankNamesPerCli, clientID)
	delete(j.txCachePerCl, clientID)
	delete(j.workerEofReceived, clientID)
	delete(j.accountsEofReceived, clientID)
	return nil
}

// Checkpoint Methods

// SnapshotClient runs inside coord.Flush, which executes under procMu (held by
// process), so the maps are quiescent; it must not re-acquire procMu.
func (j *Join) SnapshotClient(clientID uuid.UUID) ([]byte, error) {
	cp := joinCheckpoint{
		BankNames:   j.bankNamesPerCli[clientID],
		TxCache:     j.txCachePerCl[clientID],
		WorkerEOF:   j.workerEofReceived[clientID],
		AccountsEOF: j.accountsEofReceived[clientID],
		Finalized:   j.finalized[clientID],
	}
	return json.Marshal(cp)
}

// RestoreClient runs at startup (coord.Recover, no concurrency), so it does not
// take procMu.
func (j *Join) RestoreClient(clientID uuid.UUID, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	var cp joinCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return err
	}
	if cp.BankNames != nil {
		j.bankNamesPerCli[clientID] = cp.BankNames
	}
	if cp.TxCache != nil {
		j.txCachePerCl[clientID] = cp.TxCache
	}
	if cp.WorkerEOF != 0 {
		j.workerEofReceived[clientID] = cp.WorkerEOF
	}
	if cp.AccountsEOF {
		j.accountsEofReceived[clientID] = cp.AccountsEOF
	}
	if cp.Finalized {
		j.finalized[clientID] = cp.Finalized
	}
	return nil
}
