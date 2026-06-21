package messaging

import (
	"testing"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
)

func TestDedupSnapshotRestore(t *testing.T) {
	p := New(codec.New(), nil)
	clientID := uuid.New()

	ids := []protocol.MsgID{
		protocol.SourceMsgID(clientID, protocol.StreamTransactions, 1),
		protocol.SourceMsgID(clientID, protocol.StreamTransactions, 2),
		protocol.SourceMsgID(clientID, protocol.StreamTransactions, 3),
	}
	seen := 0
	handlers := countingHandlers(&seen)
	for _, id := range ids {
		if _, err := p.Dispatch(msgWith(t, clientID, id), handlers); err != nil {
			t.Fatalf("dispatch: %v", err)
		}
	}

	blob, err := p.SnapshotClient(clientID)
	if err != nil {
		t.Fatalf("SnapshotClient: %v", err)
	}

	p2 := New(codec.New(), nil)
	if err := p2.RestoreClient(clientID, blob); err != nil {
		t.Fatalf("RestoreClient: %v", err)
	}

	calls := 0
	handlers2 := countingHandlers(&calls)
	for _, id := range ids {
		if _, err := p2.Dispatch(msgWith(t, clientID, id), handlers2); err != nil {
			t.Fatalf("dispatch after restore: %v", err)
		}
	}
	if calls != 0 {
		t.Fatalf("restored dedup must drop all seen ids: ran %d, want 0", calls)
	}

	fresh := protocol.SourceMsgID(clientID, protocol.StreamTransactions, 99)
	if _, err := p2.Dispatch(msgWith(t, clientID, fresh), handlers2); err != nil {
		t.Fatalf("dispatch fresh: %v", err)
	}
	if calls != 1 {
		t.Fatalf("an unseen id must be processed: ran %d, want 1", calls)
	}
}

func TestDedupSnapshotEmptyClient(t *testing.T) {
	p := New(codec.New(), nil)
	blob, err := p.SnapshotClient(uuid.New())
	if err != nil {
		t.Fatalf("SnapshotClient: %v", err)
	}
	if len(blob) != 0 {
		t.Fatalf("empty client snapshot should be empty, got %d bytes", len(blob))
	}
}
