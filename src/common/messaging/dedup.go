package messaging

import (
	"encoding/binary"
	"sync"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
)

type dedupClientState struct {
	seen map[protocol.MsgID]struct{}
	virtualSeen map[protocol.MsgID]struct{}
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
			virtualSeen: make(map[protocol.MsgID]struct{}),
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
	virtualIds := d.getClient(clientID).virtualSeen
	virtualIds[id] = struct{}{}
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

	// ~20 bytes por mensaje (16 de ID + 1 de Flag + ~3 de Key)
	buf := make([]byte, 0, len(client.virtualSeen)*20)

	for id := range client.virtualSeen {
		buf = append(buf, id[:]...)

		var wasSent bool
		var sentKey broker.KeyType

		for k, idSet := range client.sent {
			if _, ok := idSet[id]; ok {
				wasSent = true
				sentKey = k
				break
			}
		}

		if !wasSent {
			// FLAG: 0x00 -> No enviado (conceptualmente 'nil')
			buf = append(buf, 0x00)
		} else {
			// FLAG: 0x01 -> Sí fue enviado
			buf = append(buf, 0x01)

			keyBytes := []byte(sentKey)
			var lenBuf [2]byte
			binary.BigEndian.PutUint16(lenBuf[:], uint16(len(keyBytes)))
			buf = append(buf, lenBuf[:]...)

			buf = append(buf, keyBytes...)
		}

		client.seen[id] = struct{}{}
	}

	client.virtualSeen = make(map[protocol.MsgID]struct{})

	return buf
}

func (d *dedupState) restoreClient(clientID uuid.UUID, data []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()

	seenSet := make(map[protocol.MsgID]struct{})
	sentSet := make(map[broker.KeyType]map[protocol.MsgID]struct{})

	idLen := len(protocol.MsgID{})
	offset := 0

	for offset < len(data) {
		if offset+idLen > len(data) {
			break
		}
		var id protocol.MsgID
		copy(id[:], data[offset:offset+idLen])
		offset += idLen

		seenSet[id] = struct{}{}

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


			if sentSet[keyType] == nil {
				sentSet[keyType] = make(map[protocol.MsgID]struct{})
			}
			sentSet[keyType][id] = struct{}{}
		}
	}

	client := d.getClient(clientID)
	
	client.seen = seenSet
	client.sent = sentSet
}