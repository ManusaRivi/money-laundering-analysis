package messaging

import (
	"sync"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
)

type dedupState struct {
	mu      sync.Mutex
	seen    map[uuid.UUID]map[protocol.MsgID]struct{}
	pending map[uuid.UUID][]protocol.MsgID
}

func newDedupState() *dedupState {
	return &dedupState{
		seen:    make(map[uuid.UUID]map[protocol.MsgID]struct{}),
		pending: make(map[uuid.UUID][]protocol.MsgID),
	}
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
	if _, ok := ids[id]; ok {
		return
	}
	ids[id] = struct{}{}
	d.pending[clientID] = append(d.pending[clientID], id)
}

func (d *dedupState) forget(clientID uuid.UUID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.seen, clientID)
	delete(d.pending, clientID)
}

func (d *dedupState) drainClient(clientID uuid.UUID) []byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	ids := d.pending[clientID]
	buf := make([]byte, 0, len(ids)*len(protocol.MsgID{}))
	for _, id := range ids {
		buf = append(buf, id[:]...)
	}
	return buf
}

func (d *dedupState) commitDrained(clientID uuid.UUID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.pending, clientID)
}

func (d *dedupState) replayClient(clientID uuid.UUID, data []byte) {
	idLen := len(protocol.MsgID{})
	d.mu.Lock()
	defer d.mu.Unlock()
	set := d.seen[clientID]
	if set == nil {
		set = make(map[protocol.MsgID]struct{}, len(data)/idLen)
		d.seen[clientID] = set
	}
	for i := 0; i+idLen <= len(data); i += idLen {
		var id protocol.MsgID
		copy(id[:], data[i:i+idLen])
		set[id] = struct{}{}
	}
}
