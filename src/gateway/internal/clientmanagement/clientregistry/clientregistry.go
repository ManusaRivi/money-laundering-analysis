package clientregistry

import (
	"sync"

	"github.com/ManusaRivi/money-laundering-analysis/src/gateway/internal/clientmanagement/clientconnection"
)

type ClientRegistry struct {
	mutex   sync.Mutex
	clients []*clientconnection.ClientConnection
}

func (registry *ClientRegistry) Add(client *clientconnection.ClientConnection) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	registry.clients = append(registry.clients, client)
}

func (registry *ClientRegistry) Remove(target *clientconnection.ClientConnection) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	for i, c := range registry.clients {
		if c == target {
			registry.clients = append(registry.clients[:i], registry.clients[i+1:]...)
			return
		}
	}
}

func (registry *ClientRegistry) WithLock(action func([]*clientconnection.ClientConnection)) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	action(registry.clients)
}
