package messaging

import (
	"sync"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
)

type dedupState struct {
	mu   sync.Mutex
	seen map[uuid.UUID]map[protocol.MsgID]struct{}
}

func newDedupState() *dedupState {
	return &dedupState{seen: make(map[uuid.UUID]map[protocol.MsgID]struct{})}
}

func (d *dedupState) alreadySeen(clientID uuid.UUID, id protocol.MsgID) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.seen[clientID][id]
	return ok
}

func (d *dedupState) markSeen(clientID uuid.UUID, id protocol.MsgID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	ids := d.seen[clientID]
	if ids == nil {
		ids = make(map[protocol.MsgID]struct{})
		d.seen[clientID] = ids
	}
	ids[id] = struct{}{}
}

func (d *dedupState) forget(clientID uuid.UUID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.seen, clientID)
}
