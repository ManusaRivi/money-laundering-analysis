package join

import (
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/messaging"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
	// "github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
)

const BATCH_SIZE = 1000

type Query4Client struct {
	accountsSet map[domain.Account]struct{}
}

type Query4 struct {
	pub     *messaging.Publisher
	clients map[uuid.UUID]*Query4Client

	broker broker.Broker

	prevWorkerAmount int
	eofCounters      map[uuid.UUID]int
}

func NewQuery4(cfg config.WorkerConfig, b broker.Broker) (*Query4, error) {
	return &Query4{
		pub:              messaging.New(codec.New(), b),
		clients:          make(map[uuid.UUID]*Query4Client),
		broker:           b,
		prevWorkerAmount: cfg.PrevWorkerAmount,
		eofCounters:      make(map[uuid.UUID]int),
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

func (j *Query4) handleAccountsMessage(envelope protocol.InternalEnvelope) error {
	// var accounts []domain.Account
	// if err := pkt.UnmarshalData(&accounts); err != nil {
	// 	return fmt.Errorf("error unmarshalling accounts data: %w", err)
	// }
	clientId := envelope.ClientId
	accounts, err := j.pub.DecodeAccountsEnvelope(envelope.Payload)
	if err != nil {
		return fmt.Errorf("error decoding accounts: %w", err)
	}
	client := j.clients[clientId]
	if client == nil {
		client = &Query4Client{
			accountsSet: make(map[domain.Account]struct{}),
		}
		j.clients[clientId] = client
	}

	for _, account := range accounts {
		client.accountsSet[account] = struct{}{}
	}

	return nil

}

func (j *Query4) handleEOFMessage(envelope protocol.InternalEnvelope) error {
	clientId := envelope.ClientId
	j.eofCounters[clientId]++
	if j.eofCounters[clientId] < j.prevWorkerAmount {
		slog.Debug("Received EOF from a worker, waiting for more...", "clientID", clientId, "count", j.eofCounters[clientId])
		return nil
	}

	client := j.clients[clientId]
	if client == nil {
		slog.Debug("No accounts received for this client, sending EOF only", "clientID", clientId)
		// eof, err := inner.MarshalQuery4EOFPacket(clientId)
		// if err != nil {
		// 	return fmt.Errorf("error marshalling Query4 EOF: %w", err)
		// }
		// return j.broker.Send(*eof)
		return j.pub.PublishInternal(clientId, protocol.MsgQuery4ResultEOF, broker.KeyControlEOF, nil)
	}

	// accounts := make([]domain.Account, 0, len(client.accountsSet))
	// for account := range client.accountsSet {
	// 	accounts = append(accounts, account)
	// }

	// for i := 0; i < len(accounts); i += BATCH_SIZE {
	// 	end := i + BATCH_SIZE
	// 	if end > len(accounts) {
	// 		end = len(accounts)
	// 	}
	// 	data := domain.Query4Result{
	// 		Accounts: accounts[i:end],
	// 	}
	// 	msg, err := inner.MarshalQuery4ResultPacket(pkt.ClientID, broker.KeyNil, data)
	// 	if err != nil {
	// 		return fmt.Errorf("error marshalling accounts batch: %w", err)
	// 	}
	// 	if err := j.broker.Send(*msg); err != nil {
	// 		return fmt.Errorf("error sending accounts batch: %w", err)
	// 	}
	// }
	subset := make(map[domain.Account]struct{}, BATCH_SIZE)
	for account := range client.accountsSet {
		subset[account] = struct{}{}
		if len(subset) == BATCH_SIZE {
			slog.Debug("Sending accounts batch to gateway", "batchSize", len(subset), "clientID", clientId)
			err := j.sendQuery4ResultEnvelope(clientId, subset)
			if err != nil {
				return err
			}
			subset = make(map[domain.Account]struct{}, BATCH_SIZE)
		}
	}
	if len(subset) > 0 {
		err := j.sendQuery4ResultEnvelope(clientId, subset)
		if err != nil {
			return err
		}
	}

	slog.Debug("All accounts sent for client, sending EOF", "clientID", clientId)

	delete(j.clients, clientId)
	delete(j.eofCounters, clientId)

	// eof, err := inner.MarshalQuery4EOFPacket(pkt.ClientID)
	err := j.pub.PublishInternal(clientId, protocol.MsgQuery4ResultEOF, broker.KeyControlEOF, nil)
	if err != nil {
		return err
	}

	return nil
}

func (j *Query4) handleMessage(msg broker.Message) error {
	return j.pub.Dispatch(msg, map[protocol.MsgType]messaging.Handler{
		protocol.MsgTxAccounts:      j.handleAccountsMessage,
		protocol.MsgTransactionsEOF: j.handleEOFMessage,
	})
}

func (j *Query4) sendQuery4ResultEnvelope(clientId uuid.UUID, subset map[domain.Account]struct{}) error {
	envelope, err := j.pub.EncodeQuery4ResultEnvelope(clientId, subset)
	if err != nil {
		return fmt.Errorf("error encoding query4 result envelope: %w", err)
	}
	if err := j.pub.PublishRaw(broker.KeyNil, envelope); err != nil {
		return fmt.Errorf("error sending query4 result envelope: %w", err)
	}
	return nil
}
