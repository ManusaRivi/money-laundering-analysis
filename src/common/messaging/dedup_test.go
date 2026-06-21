package messaging

import (
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
)

func msgWith(t *testing.T, clientID uuid.UUID, id protocol.MsgID) broker.Message {
	t.Helper()
	body, err := codec.New().EncodeInternalEnvelope(protocol.InternalEnvelope{
		MsgType:  protocol.MsgTransactionsBatch,
		ClientId: clientID,
		MsgID:    id,
		Payload:  []byte("payload"),
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return broker.Message{ContentType: broker.ContentTypeBinary, Body: body}
}

func countingHandlers(calls *int) map[protocol.MsgType]Handler {
	return map[protocol.MsgType]Handler{
		protocol.MsgTransactionsBatch: func(protocol.InternalEnvelope) error {
			*calls++
			return nil
		},
	}
}

func TestDispatchDropsDuplicates(t *testing.T) {
	p := New(codec.New(), nil)
	clientID := uuid.New()
	id := protocol.SourceMsgID(clientID, protocol.StreamTransactions, 1)

	calls := 0
	handlers := countingHandlers(&calls)
	msg := msgWith(t, clientID, id)

	for i := 0; i < 3; i++ {
		if _, err := p.Dispatch(msg, handlers); err != nil {
			t.Fatalf("dispatch: %v", err)
		}
	}
	if calls != 1 {
		t.Fatalf("handler ran %d times, want 1", calls)
	}
}

func TestDispatchPassesDistinctIDs(t *testing.T) {
	p := New(codec.New(), nil)
	clientID := uuid.New()
	calls := 0
	handlers := countingHandlers(&calls)

	for seq := uint64(0); seq < 5; seq++ {
		id := protocol.SourceMsgID(clientID, protocol.StreamTransactions, seq)
		if _, err := p.Dispatch(msgWith(t, clientID, id), handlers); err != nil {
			t.Fatalf("dispatch: %v", err)
		}
	}
	if calls != 5 {
		t.Fatalf("handler ran %d times, want 5", calls)
	}
}

func TestDispatchZeroMsgIDNotDeduped(t *testing.T) {
	p := New(codec.New(), nil)
	clientID := uuid.New()
	calls := 0
	handlers := countingHandlers(&calls)
	msg := msgWith(t, clientID, protocol.MsgID{})

	for i := 0; i < 3; i++ {
		if _, err := p.Dispatch(msg, handlers); err != nil {
			t.Fatalf("dispatch: %v", err)
		}
	}
	if calls != 3 {
		t.Fatalf("zero-MsgID must not be deduped: ran %d, want 3", calls)
	}
}

func TestDispatchPerClientIsolation(t *testing.T) {
	p := New(codec.New(), nil)
	a, b := uuid.New(), uuid.New()
	idA := protocol.SourceMsgID(a, protocol.StreamTransactions, 1)
	idB := protocol.SourceMsgID(b, protocol.StreamTransactions, 1)
	calls := 0
	handlers := countingHandlers(&calls)

	if _, err := p.Dispatch(msgWith(t, a, idA), handlers); err != nil {
		t.Fatalf("dispatch a: %v", err)
	}
	if _, err := p.Dispatch(msgWith(t, b, idB), handlers); err != nil {
		t.Fatalf("dispatch b: %v", err)
	}
	if calls != 2 {
		t.Fatalf("distinct clients must not cross-dedup: ran %d, want 2", calls)
	}
}

func TestDispatchHandlerErrorIsReprocessed(t *testing.T) {
	p := New(codec.New(), nil)
	clientID := uuid.New()
	id := protocol.SourceMsgID(clientID, protocol.StreamTransactions, 1)

	calls := 0
	failFirst := true
	handlers := map[protocol.MsgType]Handler{
		protocol.MsgTransactionsBatch: func(protocol.InternalEnvelope) error {
			calls++
			if failFirst {
				failFirst = false
				return fmt.Errorf("boom")
			}
			return nil
		},
	}
	msg := msgWith(t, clientID, id)

	if _, err := p.Dispatch(msg, handlers); err == nil {
		t.Fatal("expected handler error")
	}
	if _, err := p.Dispatch(msg, handlers); err != nil {
		t.Fatalf("redelivery dispatch: %v", err)
	}
	if calls != 2 {
		t.Fatalf("failed handle must be reprocessed: ran %d, want 2", calls)
	}
}

func TestForgetClearsSeen(t *testing.T) {
	p := New(codec.New(), nil)
	clientID := uuid.New()
	id := protocol.SourceMsgID(clientID, protocol.StreamTransactions, 1)
	calls := 0
	handlers := countingHandlers(&calls)

	if _, err := p.Dispatch(msgWith(t, clientID, id), handlers); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	p.Forget(clientID)
	if _, err := p.Dispatch(msgWith(t, clientID, id), handlers); err != nil {
		t.Fatalf("dispatch after forget: %v", err)
	}
	if calls != 2 {
		t.Fatalf("after Forget the id must be processable again: ran %d, want 2", calls)
	}
}
