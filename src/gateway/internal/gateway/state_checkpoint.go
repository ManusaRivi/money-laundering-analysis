package gateway

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

type noopCheckpointable struct{}

func (n *noopCheckpointable) SnapshotClient(_ uuid.UUID) ([]byte, error) {
	return nil, nil
}

func (n *noopCheckpointable) RestoreClient(_ uuid.UUID, _ []byte) error {
	return nil
}

func (n *noopCheckpointable) DrainClient(_ uuid.UUID) ([]byte, error) {
	return nil, nil
}

func (n *noopCheckpointable) CommitClient(_ uuid.UUID) error {
	return nil
}

func (n *noopCheckpointable) ReplayClient(_ uuid.UUID, _ []byte) error {
	return nil
}

type clientStateSnapshot struct {
	TxCount             int    `json:"tx_count"`
	TxUSDCount          int    `json:"tx_usd_count"`
	TxSeq               uint64 `json:"tx_seq"`
	AccSeq              uint64 `json:"acc_seq"`
	AccountsEOFSent     bool   `json:"accounts_eof_sent"`
	TransactionsEOFSent bool   `json:"transactions_eof_sent"`
}

type gatewayStateCheckpointable struct {
	gateway *Gateway
}

func newGatewayStateCheckpointable(gateway *Gateway) *gatewayStateCheckpointable {
	return &gatewayStateCheckpointable{gateway: gateway}
}

func (s *gatewayStateCheckpointable) SnapshotClient(clientID uuid.UUID) ([]byte, error) {
	s.gateway.clientsMu.Lock()
	client := s.gateway.clients[clientID]
	s.gateway.clientsMu.Unlock()
	if client == nil {
		return nil, fmt.Errorf("client %s not found for snapshot", clientID)
	}

	snap := clientStateSnapshot{
		TxCount:             client.tx_count,
		TxUSDCount:          client.tx_usd_count,
		TxSeq:               client.txSeq,
		AccSeq:              client.accSeq,
		AccountsEOFSent:     client.accountsEOFSent,
		TransactionsEOFSent: client.transactionsEOFSent,
	}
	return json.Marshal(snap)
}

func (s *gatewayStateCheckpointable) RestoreClient(clientID uuid.UUID, data []byte) error {
	snap := clientStateSnapshot{}
	if len(data) != 0 {
		if err := json.Unmarshal(data, &snap); err != nil {
			return fmt.Errorf("decode gateway client state: %w", err)
		}
	}

	s.gateway.clientsMu.Lock()
	s.gateway.clients[clientID] = &Client{
		ID:                  clientID,
		tx_count:            snap.TxCount,
		tx_usd_count:        snap.TxUSDCount,
		txSeq:               snap.TxSeq,
		accSeq:              snap.AccSeq,
		accountsEOFSent:     snap.AccountsEOFSent,
		transactionsEOFSent: snap.TransactionsEOFSent,
	}
	s.gateway.clientsMu.Unlock()
	return nil
}
