package messaging

import (
	"testing"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
)

func TestDedupDrainReplay(t *testing.T) {
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

	blob, err := p.DrainClient(clientID)
	if err != nil {
		t.Fatalf("DrainClient: %v", err)
	}

	p2 := New(codec.New(), nil)
	if err := p2.ReplayClient(clientID, blob); err != nil {
		t.Fatalf("ReplayClient: %v", err)
	}

	calls := 0
	handlers2 := countingHandlers(&calls)
	for _, id := range ids {
		if _, err := p2.Dispatch(msgWith(t, clientID, id), handlers2); err != nil {
			t.Fatalf("dispatch after replay: %v", err)
		}
	}
	if calls != 0 {
		t.Fatalf("replayed dedup must drop all seen ids: ran %d, want 0", calls)
	}

	fresh := protocol.SourceMsgID(clientID, protocol.StreamTransactions, 99)
	if _, err := p2.Dispatch(msgWith(t, clientID, fresh), handlers2); err != nil {
		t.Fatalf("dispatch fresh: %v", err)
	}
	if calls != 1 {
		t.Fatalf("an unseen id must be processed: ran %d, want 1", calls)
	}
}

func TestDedupDrainPeeksCommitClears(t *testing.T) {
	p := New(codec.New(), nil)
	clientID := uuid.New()
	id := protocol.SourceMsgID(clientID, protocol.StreamTransactions, 1)
	seen := 0
	if _, err := p.Dispatch(msgWith(t, clientID, id), countingHandlers(&seen)); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	first, err := p.DrainClient(clientID)
	if err != nil {
		t.Fatalf("DrainClient: %v", err)
	}
	if len(first) == 0 {
		t.Fatal("drain should return the pending delta")
	}

	again, err := p.DrainClient(clientID)
	if err != nil {
		t.Fatalf("DrainClient again: %v", err)
	}
	if len(again) != len(first) {
		t.Fatalf("drain must not discard the delta: got %d then %d bytes", len(first), len(again))
	}

	if err := p.CommitClient(clientID); err != nil {
		t.Fatalf("CommitClient: %v", err)
	}
	after, err := p.DrainClient(clientID)
	if err != nil {
		t.Fatalf("DrainClient after commit: %v", err)
	}
	if len(after) != 0 {
		t.Fatalf("drain after commit should be empty, got %d bytes", len(after))
	}
}

func TestDedupDrainEmptyClient(t *testing.T) {
	p := New(codec.New(), nil)
	blob, err := p.DrainClient(uuid.New())
	if err != nil {
		t.Fatalf("DrainClient: %v", err)
	}
	if len(blob) != 0 {
		t.Fatalf("empty client drain should be empty, got %d bytes", len(blob))
	}
}
