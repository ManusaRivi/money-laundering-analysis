package messaging

import (
	"encoding/binary"
	"sync"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
)

type dedupClientState struct {
	seen        map[protocol.MsgID]int
	virtualSeen map[protocol.MsgID]int
	sent        map[broker.KeyType]map[protocol.MsgID]int
}

type dedupState struct {
	mu      sync.Mutex
	clients map[uuid.UUID]*dedupClientState
}

func newDedupState() *dedupState {
	return &dedupState{clients: make(map[uuid.UUID]*dedupClientState)}
}

func (d *dedupState) getClient(clientID uuid.UUID) *dedupClientState {
	client, ok := d.clients[clientID]
	if !ok {
		client = &dedupClientState{
			seen:        make(map[protocol.MsgID]int),
			virtualSeen: make(map[protocol.MsgID]int),
			sent:        make(map[broker.KeyType]map[protocol.MsgID]int),
		}
		d.clients[clientID] = client
	}
	return client
}

func (d *dedupState) getSent(clientID uuid.UUID) map[broker.KeyType]map[protocol.MsgID]int {
	d.mu.Lock()
	defer d.mu.Unlock()
	client := d.getClient(clientID)
	out := make(map[broker.KeyType]map[protocol.MsgID]int, len(client.sent))
	for key, ids := range client.sent {
		copied := make(map[protocol.MsgID]int, len(ids))
		for id, count := range ids {
			copied[id] = count
		}
		out[key] = copied
	}
	return out
}

func (d *dedupState) getSeen(clientID uuid.UUID) map[protocol.MsgID]int {
	d.mu.Lock()
	defer d.mu.Unlock()
	client := d.getClient(clientID)
	out := make(map[protocol.MsgID]int, len(client.seen)+len(client.virtualSeen))
	for id, count := range client.seen {
		out[id] = count
	}
	for id, count := range client.virtualSeen {
		out[id] = count
	}
	return out
}

func (d *dedupState) markSent(clientID uuid.UUID, key broker.KeyType, id protocol.MsgID, count int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	client := d.getClient(clientID)
	if client.sent[key] == nil {
		client.sent[key] = make(map[protocol.MsgID]int)
	}
	client.sent[key][id] = count
}

func (d *dedupState) alreadySeen(clientID uuid.UUID, id protocol.MsgID) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	client := d.getClient(clientID)
	_, ok := client.seen[id]
	if !ok {
		_, ok = client.virtualSeen[id]
	}
	return ok
}

func (d *dedupState) markReceived(clientID uuid.UUID, id protocol.MsgID, count int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.getClient(clientID).virtualSeen[id] = count
}

func (d *dedupState) markSeen(clientID uuid.UUID, id protocol.MsgID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	virtualIds := d.getClient(clientID).virtualSeen
	if _, ok := virtualIds[id]; !ok {
		virtualIds[id] = 0
	}
}

func (d *dedupState) forget(clientID uuid.UUID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.clients, clientID)
}

func (d *dedupState) snapshotClient(clientID uuid.UUID) []byte {
	d.mu.Lock()
	defer d.mu.Unlock()

	client := d.getClient(clientID)

	buf := make([]byte, 0, len(client.virtualSeen)*24)

	for id, rcvCount := range client.virtualSeen {
		buf = append(buf, id[:]...)
		buf = binary.AppendUvarint(buf, uint64(rcvCount))

		var wasSent bool
		var sentKey broker.KeyType
		var sentCount int

		for k, idSet := range client.sent {
			if cnt, ok := idSet[id]; ok {
				wasSent = true
				sentKey = k
				sentCount = cnt
				break
			}
		}

		if !wasSent {
			buf = append(buf, 0x00)
		} else {
			buf = append(buf, 0x01)

			keyBytes := []byte(sentKey)
			var lenBuf [2]byte
			binary.BigEndian.PutUint16(lenBuf[:], uint16(len(keyBytes)))
			buf = append(buf, lenBuf[:]...)
			buf = append(buf, keyBytes...)
			buf = binary.AppendUvarint(buf, uint64(sentCount))
		}

		client.seen[id] = rcvCount
	}

	client.virtualSeen = make(map[protocol.MsgID]int)

	return buf
}

func (d *dedupState) restoreClient(clientID uuid.UUID, data []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()

	seenSet := make(map[protocol.MsgID]int)
	sentSet := make(map[broker.KeyType]map[protocol.MsgID]int)

	idLen := len(protocol.MsgID{})
	offset := 0

	for offset < len(data) {
		if offset+idLen > len(data) {
			break
		}
		var id protocol.MsgID
		copy(id[:], data[offset:offset+idLen])
		offset += idLen

		rcvCount, n := binary.Uvarint(data[offset:])
		if n <= 0 {
			break
		}
		offset += n
		seenSet[id] = int(rcvCount)

		if offset >= len(data) {
			break
		}
		wasSent := data[offset] == 0x01
		offset++

		if wasSent {
			if offset+2 > len(data) {
				break
			}
			keyLen := binary.BigEndian.Uint16(data[offset : offset+2])
			offset += 2

			if offset+int(keyLen) > len(data) {
				break
			}
			keyString := string(data[offset : offset+int(keyLen)])
			keyType := broker.KeyType(keyString)
			offset += int(keyLen)

			sentCount, n := binary.Uvarint(data[offset:])
			if n <= 0 {
				break
			}
			offset += n

			if sentSet[keyType] == nil {
				sentSet[keyType] = make(map[protocol.MsgID]int)
			}
			sentSet[keyType][id] = int(sentCount)
		}
	}

	client := d.getClient(clientID)

	client.seen = seenSet
	client.sent = sentSet
}
