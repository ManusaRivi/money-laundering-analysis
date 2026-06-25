package messaging

import (
	"sync"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
)

type dedupClientState struct {
	seen map[protocol.MsgID]struct{}
	sent map[broker.KeyType]map[protocol.MsgID]struct{}
}

type dedupState struct {
	mu   sync.Mutex
	clients map[uuid.UUID]*dedupClientState
}

func newDedupState() *dedupState {
	return &dedupState{clients: make(map[uuid.UUID]*dedupClientState)}
}

func (d *dedupState) getClient(clientID uuid.UUID) *dedupClientState {
	d.mu.Lock()
	defer d.mu.Unlock()
	client, ok := d.clients[clientID]
	if !ok {
		client = &dedupClientState{
			seen: make(map[protocol.MsgID]struct{}),
			sent: make(map[broker.KeyType]map[protocol.MsgID]struct{}),
		}
		d.clients[clientID] = client
	}
	return client
}

func (d *dedupState) getSent(clientID uuid.UUID) map[broker.KeyType]map[protocol.MsgID]struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	ids := d.getClient(clientID).sent
	return ids
}

func (d *dedupState) getSeen(clientID uuid.UUID) map[protocol.MsgID]struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	ids := d.getClient(clientID).seen
	return ids
}

func (d *dedupState) markSent(clientID uuid.UUID, key broker.KeyType, id protocol.MsgID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	client := d.getClient(clientID)
	if client.sent[key] == nil {
		client.sent[key] = make(map[protocol.MsgID]struct{})
	}
	client.sent[key][id] = struct{}{}
}

func (d *dedupState) alreadySeen(clientID uuid.UUID, id protocol.MsgID) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	client := d.getClient(clientID)
	ids := client.seen
	_, ok := ids[id]
	return ok
}

func (d *dedupState) markSeen(clientID uuid.UUID, id protocol.MsgID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	ids := d.getClient(clientID).seen
	ids[id] = struct{}{}
}

func (d *dedupState) forget(clientID uuid.UUID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.clients, clientID)
}

func (d *dedupState) snapshotClient(clientID uuid.UUID) []byte {
	// TODO: GUARDAR CON KEYS LOS SENT Y LOS SEEN, PARA PODER RECONSTRUIR EL ESTADO DE DEDUPLICACION
	d.mu.Lock()
	defer d.mu.Unlock()
	ids := d.getClient(clientID).seen
	buf := make([]byte, 0, len(ids)*len(protocol.MsgID{}))
	for id := range ids {
		buf = append(buf, id[:]...)
	}
	return buf
}

func (d *dedupState) restoreClient(clientID uuid.UUID, data []byte) {
	idLen := len(protocol.MsgID{})
	d.mu.Lock()
	defer d.mu.Unlock()
	set := make(map[protocol.MsgID]struct{}, len(data)/idLen)
	for i := 0; i+idLen <= len(data); i += idLen {
		var id protocol.MsgID
		copy(id[:], data[i:i+idLen])
		set[id] = struct{}{}
	}
	d.getClient(clientID).seen = set
}
