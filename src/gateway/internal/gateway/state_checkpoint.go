package gateway

import (
	"github.com/google/uuid"
)

type noopAppendSource struct{}

func (n *noopAppendSource) DrainClient(_ uuid.UUID) ([]byte, error)  { return nil, nil }
func (n *noopAppendSource) CommitClient(_ uuid.UUID) error           { return nil }
func (n *noopAppendSource) ReplayClient(_ uuid.UUID, _ []byte) error { return nil }

type gatewayStateCheckpointable struct {
	gateway *Gateway
}

func newGatewayStateCheckpointable(gateway *Gateway) *gatewayStateCheckpointable {
	return &gatewayStateCheckpointable{gateway: gateway}
}

func (s *gatewayStateCheckpointable) SnapshotClient(_ uuid.UUID) ([]byte, error) {
	return []byte{1}, nil
}

func (s *gatewayStateCheckpointable) RestoreClient(clientID uuid.UUID, _ []byte) error {
	s.gateway.clientsMu.Lock()
	s.gateway.clients[clientID] = &Client{ID: clientID}
	s.gateway.clientsMu.Unlock()
	return nil
}
