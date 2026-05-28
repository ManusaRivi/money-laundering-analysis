package join

import (
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
)

type Query4Client struct {
	accountsSet map[domain.Account]struct{}
}

type Query4 struct {
	clients map[uuid.UUID]*Query4Client 
	
	broker  broker.Broker
	
	prevWorkerAmount int
	eofCounters      map[uuid.UUID]int

}

func NewQuery4(cfg config.WorkerConfig, b broker.Broker) (*Query4, error) {
	return &Query4{
		clients:         make(map[uuid.UUID]*Query4Client),
		broker:          b,
		prevWorkerAmount: cfg.PrevWorkerAmount,
		eofCounters:     make(map[uuid.UUID]int),
	}, nil
}

func (j *Query4) Run() error {
	defer func() {
		j.broker.StopConsuming()
	}()
	return j.broker.StartConsuming(func(msg broker.Message, ack func(), nack func()) {
		err := j.handleMessage(msg)
		if err != nil {
			slog.Error("Error handling transaction message", "error", err)
			nack()
			return
		}
		ack()
	})
}

func (j *Query4) Stop() {
	j.broker.StopConsuming()
	j.broker.Close()
}

func (j *Query4) handleAccountsMessage(pkt inner.Packet) error {
	var accounts []domain.Account
	if err := pkt.UnmarshalData(&accounts); err != nil {
		return fmt.Errorf("error unmarshalling accounts data: %w", err)
	}
	client := j.clients[pkt.ClientID]
	if client == nil {
		client = &Query4Client{
			accountsSet: make(map[domain.Account]struct{}),
		}
		j.clients[pkt.ClientID] = client
	}

	for _, account := range accounts {
		client.accountsSet[account] = struct{}{}
	}

	return nil

}

func (j *Query4) handleEOFMessage(pkt inner.Packet) error {
	j.eofCounters[pkt.ClientID]++
	if j.eofCounters[pkt.ClientID] < j.prevWorkerAmount {
		slog.Debug("Received EOF from a worker, waiting for more...", "clientID", pkt.ClientID, "count", j.eofCounters[pkt.ClientID])
		return nil
	}

	client := j.clients[pkt.ClientID]
	if client == nil {
		slog.Debug("No accounts received for this client, sending EOF only", "clientID", pkt.ClientID)
		eof, err := inner.MarshalQuery4EOFPacket(pkt.ClientID)
		if err != nil {
			return fmt.Errorf("error marshalling Query4 EOF: %w", err)
		}
		return j.broker.Send(*eof)
	}

	accounts := make([]domain.Account, 0, len(client.accountsSet))
	for account := range client.accountsSet {
		accounts = append(accounts, account)
	}

	const batchSize = 1000
	for i := 0; i < len(accounts); i += batchSize {
		end := i + batchSize
		if end > len(accounts) {
			end = len(accounts)
		}
		data := domain.Query4Result{
			Accounts: accounts[i:end],
		}
		msg, err := inner.MarshalQuery4ResultPacket(pkt.ClientID, broker.KeyNil, data)
		if err != nil {
			return fmt.Errorf("error marshalling accounts batch: %w", err)
		}
		if err := j.broker.Send(*msg); err != nil {
			return fmt.Errorf("error sending accounts batch: %w", err)
		}
	}

	delete(j.clients, pkt.ClientID)
	delete(j.eofCounters, pkt.ClientID)

	eof, err := inner.MarshalQuery4EOFPacket(pkt.ClientID)
	if err != nil {
		return fmt.Errorf("error marshalling Query4 EOF: %w", err)
	}
	return j.broker.Send(*eof)
}


func (j *Query4) handleMessage(msg broker.Message) error {
	pkt, err := inner.UnmarshalPacket(msg)

	if err != nil {
		slog.Error("Error unmarshalling packet", "error", err)
		return err
	}

	switch pkt.Type {
	case inner.TypeAccounts:
		return j.handleAccountsMessage(*pkt)
	case inner.TypeEOF:
		return j.handleEOFMessage(*pkt)
	default:
		return fmt.Errorf("unexpected inbound packet type: %v", pkt.Type)
	}
}
