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

	mu              sync.Mutex
	bankNamesPerCli map[uuid.UUID]map[string]string
	txCachePerCl    map[uuid.UUID]map[string][]domain.Transaction
}

func NewJoin(resultsBroker broker.Broker) (*Join, error) {
	accountsConfigPath := os.Getenv("ACCOUNTS_CONFIG_PATH")
	cfg, err := config.LoadAccountConfig(accountsConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load accounts config: %w", err)
	}
	var accountsBroker broker.Broker
	accountsBroker, err = broker.NewBroker(*cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create accounts broker: %w", err)
	}
	return &Join{
		resultsBroker:   resultsBroker,
		accountsBroker:  accountsBroker,
		bankNamesPerCli: make(map[uuid.UUID]map[string]string),
		txCachePerCl:    make(map[uuid.UUID]map[string][]domain.Transaction),
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

		slog.Debug("Received bank info", "clientID", pkt.ClientID, "bankID", info.ID, "bankName", info.Name)

		j.mu.Lock()
		if j.bankNamesPerCli[pkt.ClientID] == nil {
			j.bankNamesPerCli[pkt.ClientID] = make(map[string]string)
		}
		j.bankNamesPerCli[pkt.ClientID][info.ID] = info.Name
		cached := j.txCachePerCl[pkt.ClientID][info.ID]
		j.mu.Unlock()

		slog.Debug("Emitting cached results", "clientID", pkt.ClientID, "bankID", info.ID, "count", len(cached))
		for i, tx := range cached {
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

		slog.Debug("Flushing cached results", "clientID", pkt.ClientID, "bankID", info.ID)
		j.mu.Lock()
		if perBank, ok := j.txCachePerCl[pkt.ClientID]; ok {
			delete(perBank, info.ID)
		}
		j.mu.Unlock()
	case inner.TypeAccountsEOF:
		slog.Debug("Received accounts EOF for client", "clientID", pkt.ClientID)
		// No more bank info will be received for this client, so we can clear
		// any cached transactions that haven't been joined yet, as they won't
		// be able to produce results.
		j.mu.Lock()
		delete(j.txCachePerCl, pkt.ClientID)
		j.mu.Unlock()
		slog.Debug("Cleared transaction cache for client", "clientID", pkt.ClientID)
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
		name, ok := j.bankNamesPerCli[pkt.ClientID][tx.Origin.BankID]
		if !ok {
			if j.txCachePerCl[pkt.ClientID] == nil {
				j.txCachePerCl[pkt.ClientID] = make(map[string][]domain.Transaction)
			}
			j.txCachePerCl[pkt.ClientID][tx.Origin.BankID] = append(j.txCachePerCl[pkt.ClientID][tx.Origin.BankID], tx)
		}
		j.mu.Unlock()

		if ok {
			return j.emitResult(pkt.ClientID, tx, name)
		}
	case inner.TypeEOF:
		// Transaction EOF: forward query EOF downstream.
		eofMsg, err := inner.MarshalQuery2EOFPacket(pkt.ClientID)
		if err != nil {
			slog.Error("Error marshalling Query2 EOF", "error", err)
			return err
		}
		return j.resultsBroker.Send(*eofMsg)
	default:
		return fmt.Errorf("unexpected inbound packet type: %v", pkt.Type)
	}
	return nil
}

func (j *Join) emitResult(clientID uuid.UUID, tx domain.Transaction, name string) error {
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
