package join

import (
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
)

// Join builds Query2Result records by joining max-amount transactions (received
// from an upstream sum aggregator) with bank account info (received directly
// from the gateway on a separate queue).
type Join struct {
	resultsBroker  broker.Broker
	accountsBroker broker.Broker

	previousWorkerAmount int

	// All fields below are guarded by mu.
	mu                  sync.Mutex
	workerEofReceived   map[uuid.UUID]int
	accountsEofReceived map[uuid.UUID]bool
	finalized           map[uuid.UUID]bool
	bankNamesPerCli     map[uuid.UUID]map[string]string
	txCachePerCl        map[uuid.UUID]map[string][]domain.Transaction
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
		previousWorkerAmount: cfg.PrevWorkerAmount,
		workerEofReceived:    make(map[uuid.UUID]int),
		accountsEofReceived:  make(map[uuid.UUID]bool),
		finalized:            make(map[uuid.UUID]bool),
		bankNamesPerCli:      make(map[uuid.UUID]map[string]string),
		txCachePerCl:         make(map[uuid.UUID]map[string][]domain.Transaction),
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

func (j *Join) handleAccountsMessage(msg broker.Message) error {
	pkt, err := inner.UnmarshalPacket(msg)
	if err != nil {
		slog.Error("Error unmarshalling accounts packet", "error", err)
		return err
	}

	switch pkt.Type {
	case inner.TypeBankInfo:
		var info domain.BankInfo
		if err := pkt.UnmarshalData(&info); err != nil {
			slog.Error("Error unmarshalling bank info", "error", err)
			return err
		}
		if info.ID == "" {
			return fmt.Errorf("bank info missing 'id' field")
		}

		// slog.Debug("Received bank info", "clientID", pkt.ClientID, "bankID", info.ID, "bankName", info.Name)

		j.mu.Lock()
		if j.bankNamesPerCli[pkt.ClientID] == nil {
			j.bankNamesPerCli[pkt.ClientID] = make(map[string]string)
		}
		j.bankNamesPerCli[pkt.ClientID][info.ID] = info.Name
		// slog.Debug("Cached bank name", "clientID", pkt.ClientID, "bankID", info.ID, "bankName", info.Name)
		cached := j.txCachePerCl[pkt.ClientID][info.ID]
		j.mu.Unlock()

		for i, tx := range cached {
			slog.Debug("Emitting cached result", "clientID", pkt.ClientID, "bankID", info.ID)
			if err := j.emitResult(pkt.ClientID, tx, info.Name); err != nil {
				// Keep the failed tx (and any after it) in the cache so a
				// redelivery of this bank info can retry them.
				j.mu.Lock()
				j.txCachePerCl[pkt.ClientID][info.ID] = cached[i:]
				j.mu.Unlock()
				slog.Error("Error emitting cached result", "error", err)
				return err
			}
		}

		if len(cached) > 0 {
			slog.Debug("Emitted all cached results for bank, clearing cache", "clientID", pkt.ClientID, "bankID", info.ID)
		}
		j.mu.Lock()
		if perBank, ok := j.txCachePerCl[pkt.ClientID]; ok {
			delete(perBank, info.ID)
		}
		j.mu.Unlock()
	case inner.TypeAccountsEOF:
		slog.Debug("Received accounts EOF for client", "clientID", pkt.ClientID)
		j.mu.Lock()
		j.accountsEofReceived[pkt.ClientID] = true
		ready := j.shouldFinalizeLocked(pkt.ClientID)
		j.mu.Unlock()
		if ready {
			return j.finalizeClient(pkt.ClientID)
		}
	default:
		return fmt.Errorf("unexpected accounts packet type: %v", pkt.Type)
	}
	return nil
}

func (j *Join) handleTransactionMessage(msg broker.Message) error {
	pkt, err := inner.UnmarshalPacket(msg)
	if err != nil {
		slog.Error("Error unmarshalling transaction packet", "error", err)
		return err
	}

	switch pkt.Type {
	case inner.TypeTransaction:
		var tx domain.Transaction
		if err := pkt.UnmarshalData(&tx); err != nil {
			slog.Error("Error unmarshalling transaction data", "error", err)
			return err
		}
		if tx.Origin == nil {
			return fmt.Errorf("transaction missing 'origin' field")
		}
		if tx.Paid == nil {
			return fmt.Errorf("transaction missing 'paid' field")
		}

		j.mu.Lock()
		slog.Debug("Received transaction message for join", "msgType", pkt.Type)
		name, ok := j.bankNamesPerCli[pkt.ClientID][tx.Origin.BankID]
		if !ok {
			if j.txCachePerCl[pkt.ClientID] == nil {
				j.txCachePerCl[pkt.ClientID] = make(map[string][]domain.Transaction)
			}
			slog.Debug("Bank name not found for transaction, caching", "clientID", pkt.ClientID, "bankID", tx.Origin.BankID)
			j.txCachePerCl[pkt.ClientID][tx.Origin.BankID] = append(j.txCachePerCl[pkt.ClientID][tx.Origin.BankID], tx)
		}
		j.mu.Unlock()

		if ok {
			return j.emitResult(pkt.ClientID, tx, name)
		}
	case inner.TypeEOF:
		slog.Debug("Received transaction EOF for client", "clientID", pkt.ClientID)
		j.mu.Lock()
		j.workerEofReceived[pkt.ClientID]++
		count := j.workerEofReceived[pkt.ClientID]
		ready := j.shouldFinalizeLocked(pkt.ClientID)
		j.mu.Unlock()
		slog.Debug("Current EOF count", "clientID", pkt.ClientID, "workerEofReceived", count, "previousWorkerAmount", j.previousWorkerAmount)
		if ready {
			return j.finalizeClient(pkt.ClientID)
		}
	default:
		return fmt.Errorf("unexpected inbound packet type: %v", pkt.Type)
	}
	return nil
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
		for _, tx := range txs {
			if err := j.emitResult(clientID, tx, name); err != nil {
				slog.Error("Error emitting cached result during finalize",
					"clientID", clientID, "bankID", bankID, "error", err)
				return err
			}
		}
	}

	slog.Debug("Emitting Query2 EOF for client", "clientID", clientID)
	eofMsg, err := inner.MarshalQuery2EOFPacket(clientID)
	if err != nil {
		slog.Error("Error marshalling Query2 EOF", "error", err)
		return err
	}
	return j.resultsBroker.Send(*eofMsg)
}

func (j *Join) emitResult(clientID uuid.UUID, tx domain.Transaction, name string) error {
	slog.Debug("Emitting result for client", "clientID", clientID)
	result := domain.Query2Result{
		FromBank:    tx.Origin.BankID,
		FromAccount: tx.Origin.ID,
		BankName:    name,
		AmountPaid:  tx.Paid.Amount,
	}
	out, err := inner.MarshalQuery2ResultPacket(clientID, result)
	if err != nil {
		slog.Error("Error marshalling query result", "error", err)
		return err
	}
	return j.resultsBroker.Send(*out)
}
