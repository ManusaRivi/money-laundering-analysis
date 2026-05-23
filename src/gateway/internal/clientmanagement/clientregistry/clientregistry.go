package clientregistry

import (
	"sync"

	"github.com/ManusaRivi/money-laundering-analysis/src/gateway/internal/clientmanagement/clientconnection"
	"github.com/google/uuid"
)

type ClientRegistry struct {
	mutex   sync.Mutex
	clients map[uuid.UUID]*clientconnection.ClientConnection
}

func NewClientRegistry() ClientRegistry {
	return ClientRegistry{clients: make(map[uuid.UUID]*clientconnection.ClientConnection)}
}

func (registry *ClientRegistry) Add(client *clientconnection.ClientConnection) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	registry.clients[client.ClientId] = client
}

func (registry *ClientRegistry) Remove(target *clientconnection.ClientConnection) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	delete(registry.clients, target.ClientId)
}

func (registry *ClientRegistry) WithLock(action func(map[uuid.UUID]*clientconnection.ClientConnection)) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	action(registry.clients)
}
