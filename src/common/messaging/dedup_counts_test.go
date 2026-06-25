package messaging

import (
	"testing"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/google/uuid"
)

func mkID(b byte) protocol.MsgID {
	var m protocol.MsgID
	m[0] = b
	return m
}

func TestDedupSnapshotRestorePreservesCounts(t *testing.T) {
	d := newDedupState()
	cid := uuid.New()

	d.markReceived(cid, mkID(1), 100)
	d.markReceived(cid, mkID(2), 50)
	d.markSent(cid, broker.KeyType("tx.usd"), mkID(1), 80)

	data := d.snapshotClient(cid)

	restored := newDedupState()
	restored.restoreClient(cid, data)

	seen := restored.getSeen(cid)
	if seen[mkID(1)] != 100 || seen[mkID(2)] != 50 {
		t.Fatalf("received counts not preserved across snapshot: %v", seen)
	}
	sent := restored.getSent(cid)
	if sent[broker.KeyType("tx.usd")][mkID(1)] != 80 {
		t.Fatalf("sent count/key not preserved across snapshot: %v", sent)
	}
}

func TestGetSeenMergesVirtualWithCounts(t *testing.T) {
	d := newDedupState()
	cid := uuid.New()

	d.markReceived(cid, mkID(1), 7) // pending in virtualSeen
	if got := d.getSeen(cid)[mkID(1)]; got != 7 {
		t.Fatalf("getSeen[id] = %d; want 7", got)
	}

	// markSeen (the Dispatch membership fallback) must not clobber a recorded count.
	d.markSeen(cid, mkID(1))
	if got := d.getSeen(cid)[mkID(1)]; got != 7 {
		t.Fatalf("markSeen clobbered count: got %d; want 7", got)
	}

	// markSeen on a fresh id records membership with count 0.
	d.markSeen(cid, mkID(2))
	if _, ok := d.getSeen(cid)[mkID(2)]; !ok {
		t.Fatalf("markSeen did not record membership for id(2)")
	}
}
