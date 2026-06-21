package eof

import (
	"bytes"
	"testing"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
)

func TestSyncEOFSnapshotRestore(t *testing.T) {
	c := &SyncEOFController{clients: make(map[uuid.UUID]*client)}
	clientID := uuid.New()
	c.MessageReceived(clientID, 5)
	c.MessageSentWithKey(clientID, broker.KeyDollarTransaction, 3)

	blob, err := c.SnapshotClient(clientID)
	if err != nil {
		t.Fatalf("SnapshotClient: %v", err)
	}
	if len(blob) == 0 {
		t.Fatal("expected a non-empty snapshot")
	}

	restored := &SyncEOFController{clients: make(map[uuid.UUID]*client)}
	if err := restored.RestoreClient(clientID, blob); err != nil {
		t.Fatalf("RestoreClient: %v", err)
	}

	blob2, err := restored.SnapshotClient(clientID)
	if err != nil {
		t.Fatalf("SnapshotClient after restore: %v", err)
	}
	if !bytes.Equal(blob, blob2) {
		t.Fatalf("round-trip mismatch:\n%s\n%s", blob, blob2)
	}

	cl := restored.clients[clientID]
	if cl.msgRcvCount != 5 {
		t.Fatalf("restored msgRcvCount = %d, want 5", cl.msgRcvCount)
	}
	if cl.msgSentCountByKey[broker.KeyDollarTransaction] != 3 {
		t.Fatalf("restored sent[dollar] = %d, want 3", cl.msgSentCountByKey[broker.KeyDollarTransaction])
	}
}

func TestSyncEOFSnapshotUnknownClient(t *testing.T) {
	c := &SyncEOFController{clients: make(map[uuid.UUID]*client)}
	blob, err := c.SnapshotClient(uuid.New())
	if err != nil {
		t.Fatalf("SnapshotClient: %v", err)
	}
	if blob != nil {
		t.Fatalf("unknown client snapshot should be nil, got %v", blob)
	}
}

func TestSyncEOFRestoreEmptyIsNoop(t *testing.T) {
	c := &SyncEOFController{clients: make(map[uuid.UUID]*client)}
	if err := c.RestoreClient(uuid.New(), nil); err != nil {
		t.Fatalf("RestoreClient(nil): %v", err)
	}
}
