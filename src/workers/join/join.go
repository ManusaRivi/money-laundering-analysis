package join

import (
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external/codec"
)

// Join builds Query2Result records by joining max-amount transactions (received
// from an upstream sum aggregator) with bank account info (received directly
// from the gateway on a separate queue).
type Join struct {
	resultsBroker  broker.Broker
	accountsBroker broker.Broker
	codec          codec.Codec

	previousWorkerAmount int

	// All fields below are guarded by mu.
	mu                  sync.Mutex
	workerEofReceived   map[uuid.UUID]int
	accountsEofReceived map[uuid.UUID]bool
	finalized           map[uuid.UUID]bool
	bankNamesPerCli     map[uuid.UUID]map[string]string
	txCachePerCl        map[uuid.UUID]map[string][]external.Transaction
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
		resultsBroker:        resultsBroker,
		accountsBroker:       accountsBroker,
		codec:                codec.New(),
		previousWorkerAmount: cfg.PrevWorkerAmount,
		workerEofReceived:    make(map[uuid.UUID]int),
		accountsEofReceived:  make(map[uuid.UUID]bool),
		finalized:            make(map[uuid.UUID]bool),
		bankNamesPerCli:      make(map[uuid.UUID]map[string]string),
		txCachePerCl:         make(map[uuid.UUID]map[string][]external.Transaction),
	}, nil
}

func (j *Join) Run() error {
	defer func() {
		j.resultsBroker.StopConsuming()
		j.accountsBroker.StopConsuming()
	}()

	errCh := make(chan error, 2)

	go func() {
		errCh <- j.accountsBroker.StartConsuming(func(msg broker.Message, ack, nack func()) {
			if err := j.handleAccountsMessage(msg); err != nil {
				nack()
				return
			}
			ack()
		})
	}()

	go func() {
		errCh <- j.resultsBroker.StartConsuming(func(msg broker.Message, ack, nack func()) {
			if err := j.handleTransactionMessage(msg); err != nil {
				nack()
				return
			}
			ack()
		})
	}()

	return <-errCh
}

func (j *Join) Stop() {}

// Private Methods

func toQuery2Result(tx external.Transaction, bankName string) external.Query2Result {
	return external.Query2Result{
		FromBank:    tx.FromBank,
		FromAccount: tx.FromAccount,
		BankName:    bankName,
		AmountPaid:  tx.AmountPaid,
	}
}

func (j *Join) handleAccountsBatch(envelope external.InternalEnvelope) error {
	accounts, err := j.codec.DecodeAccountBatch(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding accounts batch", "error", err)
		return err
	}
	clientID := envelope.ClientId
	slog.Debug("Received accounts batch", "clientID", clientID, "batchLen", len(accounts))

	j.mu.Lock()
	// Create bank ID -> name map for this client
	if j.bankNamesPerCli[clientID] == nil {
		j.bankNamesPerCli[clientID] = make(map[string]string)
	}
	j.mu.Unlock()
	// For each account received, if the Bank ID is not stored, store it with the Bank Name.
	for _, info := range accounts {
		j.mu.Lock()
		if _, ok := j.bankNamesPerCli[clientID][info.BankID]; !ok {
			j.bankNamesPerCli[clientID][info.BankID] = info.BankName
		}
		var cached []external.Transaction
		if perBank, ok := j.txCachePerCl[clientID]; ok {
			cached = perBank[info.BankID]
			delete(perBank, info.BankID)
		}
		j.mu.Unlock()
		if len(cached) == 0 {
			continue
		}
		results := make([]external.Query2Result, 0, len(cached))
		for _, tx := range cached {
			results = append(results, toQuery2Result(tx, info.BankName))
		}
		if err := j.emitResult(clientID, results); err != nil {
			j.mu.Lock()
			// Re-cache the whole cached object if sending fails, to retry if the same bank is received.
			if j.txCachePerCl[clientID] == nil {
				j.txCachePerCl[clientID] = make(map[string][]external.Transaction)
			}
			j.txCachePerCl[clientID][info.BankID] = cached
			j.mu.Unlock()
			slog.Error("Error emitting cached result", "error", err)
			return err
		}
		slog.Debug("Emitted cached results for bank", "clientID", clientID, "bankID", info.BankID, "count", len(cached))
	}
	return nil
}

func (j *Join) handleAccountsEOF(envelope external.InternalEnvelope) error {
	clientID := envelope.ClientId
	slog.Debug("Received accounts EOF for client", "clientID", clientID)
	j.mu.Lock()
	j.accountsEofReceived[clientID] = true
	ready := j.shouldFinalizeLocked(clientID)
	j.mu.Unlock()
	if ready {
		return j.finalizeClient(clientID)
	}
	return nil
}

func (j *Join) handleAccountsMessage(msg broker.Message) error {
	envelope, err := j.codec.DecodeInternalEnvelope(msg.Body)
	if err != nil {
		slog.Error("Error unmarshalling accounts packet", "error", err)
		return err
	}

	switch envelope.MsgType {
	case external.MsgAccountsBatch:
		return j.handleAccountsBatch(envelope)
	case external.MsgAccountsEOF:
		return j.handleAccountsEOF(envelope)
	default:
		return fmt.Errorf("unexpected accounts packet type: %v", envelope.MsgType)
	}
}

func (j *Join) handleTransactionBatch(envelope external.InternalEnvelope) error {
	transactions, err := j.codec.DecodeTransactionBatch(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding transaction batch", "error", err)
		return err
	}
	results := make([]external.Query2Result, 0, len(transactions))
	j.mu.Lock()
	slog.Debug("Received transaction batch for join", "batchLen", len(transactions), "msgType", envelope.MsgType)
	for _, tx := range transactions {
		name, ok := j.bankNamesPerCli[envelope.ClientId][tx.FromBank]
		if !ok {
			if j.txCachePerCl[envelope.ClientId] == nil {
				j.txCachePerCl[envelope.ClientId] = make(map[string][]external.Transaction)
			}
			slog.Debug("Bank name not found for transaction, caching", "clientID", envelope.ClientId, "bankID", tx.FromBank)
			j.txCachePerCl[envelope.ClientId][tx.FromBank] = append(j.txCachePerCl[envelope.ClientId][tx.FromBank], tx)
		} else {
			results = append(results, toQuery2Result(tx, name))
		}
	}
	j.mu.Unlock()
	if len(results) == 0 {
		return nil
	}
	return j.emitResult(envelope.ClientId, results)
}

func (j *Join) handleTransactionsEOF(envelope external.InternalEnvelope) error {
	slog.Debug("Received transaction EOF for client", "clientID", envelope.ClientId)
	j.mu.Lock()
	j.workerEofReceived[envelope.ClientId]++
	count := j.workerEofReceived[envelope.ClientId]
	ready := j.shouldFinalizeLocked(envelope.ClientId)
	j.mu.Unlock()
	slog.Debug("Current EOF count", "clientID", envelope.ClientId, "workerEofReceived", count, "previousWorkerAmount", j.previousWorkerAmount)
	if ready {
		return j.finalizeClient(envelope.ClientId)
	}
	return nil
}

func (j *Join) handleTransactionMessage(msg broker.Message) error {
	envelope, err := j.codec.DecodeInternalEnvelope(msg.Body)
	if err != nil {
		slog.Error("Error decoding transaction batch", "error", err)
		return err
	}

	switch envelope.MsgType {
	case external.MsgTransactionsBatch:
		return j.handleTransactionBatch(envelope)
	case external.MsgTransactionsEOF:
		return j.handleTransactionsEOF(envelope)
	default:
		return fmt.Errorf("unexpected inbound packet type: %v", envelope.MsgType)
	}
}

// shouldFinalizeLocked reports whether both the worker-side EOF barrier and the
// accounts-side EOF have been reached for the given client, and atomically
// claims the finalize for the caller so concurrent callers won't double-emit.
// Caller must hold j.mu.
func (j *Join) shouldFinalizeLocked(clientID uuid.UUID) bool {
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

// finalizeClient is called once per client, when both EOF barriers have been
// reached. It drains any transactions still cached for banks that have already
// been registered (the failed-emit retry path stores them back under their
// known bank), drops cached txs whose bank never appeared in the accounts
// dataset, emits the Query2 EOF, and tears down the client's state.
func (j *Join) finalizeClient(clientID uuid.UUID) error {
	j.mu.Lock()
	banks := j.bankNamesPerCli[clientID]
	cached := j.txCachePerCl[clientID]
	delete(j.bankNamesPerCli, clientID)
	delete(j.txCachePerCl, clientID)
	delete(j.workerEofReceived, clientID)
	delete(j.accountsEofReceived, clientID)
	j.mu.Unlock()

	for bankID, txs := range cached {
		name, known := banks[bankID]
		if !known {
			slog.Warn("Dropping cached transactions with no matching bank",
				"clientID", clientID, "bankID", bankID, "count", len(txs))
			continue
		}
		results := make([]external.Query2Result, 0, len(txs))
		for _, tx := range txs {
			results = append(results, toQuery2Result(tx, name))
		}
		if err := j.emitResult(clientID, results); err != nil {
			slog.Error("Error emitting cached result during finalize",
				"clientID", clientID, "bankID", bankID, "error", err)
			return err
		}
	}

	eofEnvelope, err := j.codec.EncodeInternalEnvelope(external.InternalEnvelope{
		MsgType:  external.MsgQuery2ResultEOF,
		ClientId: clientID,
		Payload:  nil,
	})
	if err != nil {
		slog.Error("Error encoding Query2 EOF envelope", "error", err)
		return err
	}
	err = j.resultsBroker.Send(broker.Message{
		RoutingKey:  broker.KeyNil,
		Body:        eofEnvelope,
		ContentType: broker.ContentTypeBinary,
	})
	if err != nil {
		slog.Error("Error sending Query2 EOF to broker", "error", err)
		return err
	}
	slog.Debug("Emitted Query2 EOF for client", "clientID", clientID)
	return nil
}

func (j *Join) emitResult(clientID uuid.UUID, results []external.Query2Result) error {
	slog.Debug("Emitting result for client", "clientID", clientID)
	resultsBytes, err := j.codec.EncodeQuery2ResultBatch(results)
	if err != nil {
		slog.Error("Error encoding query result batch", "error", err)
		return err
	}
	envelope, err := j.codec.EncodeInternalEnvelope(external.InternalEnvelope{
		MsgType:  external.MsgQuery2Result,
		ClientId: clientID,
		Payload:  resultsBytes,
	})
	if err != nil {
		slog.Error("Error encoding internal envelope", "error", err)
		return err
	}
	err = j.resultsBroker.Send(broker.Message{
		RoutingKey:  broker.KeyNil,
		Body:        envelope,
		ContentType: broker.ContentTypeBinary,
	})
	if err != nil {
		slog.Error("Error sending message to broker", "error", err)
		return err
	}
	return nil
}
