package messaging

import (
	"log/slog"
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
	slog.Debug("Dedup forgot client", "clientID", clientID, "remaining", len(d.seen))
}

func (d *dedupState) snapshotClient(clientID uuid.UUID) []byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	ids := d.seen[clientID]
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
	d.seen[clientID] = set
}
