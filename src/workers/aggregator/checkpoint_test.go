package aggregator

import (
	"reflect"
	"testing"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
)

func newTestAgg() *Aggregator {
	return &Aggregator{
		countState: map[uuid.UUID]int{},
		state:      map[uuid.UUID]map[string]protocol.Transaction{},
		avgState:   map[uuid.UUID]map[string]avgState{},
	}
}

func TestAggregatorStateSnapshotRestore(t *testing.T) {
	a := newTestAgg()
	cl := uuid.New()
	a.state[cl] = map[string]protocol.Transaction{
		"b1": {FromBank: "b1", FromAccount: "a1", AmountPaid: 1000},
		"b2": {FromBank: "b2", FromAccount: "a2", AmountPaid: 500},
	}

	blob, err := a.SnapshotClient(cl)
	if err != nil {
		t.Fatalf("SnapshotClient: %v", err)
	}

	b := newTestAgg()
	if err := b.RestoreClient(cl, blob); err != nil {
		t.Fatalf("RestoreClient: %v", err)
	}
	if !reflect.DeepEqual(b.state[cl], a.state[cl]) {
		t.Fatalf("restored state mismatch:\n%+v\n%+v", b.state[cl], a.state[cl])
	}
}

func TestAggregatorCountSnapshotRestore(t *testing.T) {
	a := newTestAgg()
	cl := uuid.New()
	a.countState[cl] = 42

	blob, err := a.SnapshotClient(cl)
	if err != nil {
		t.Fatalf("SnapshotClient: %v", err)
	}
	b := newTestAgg()
	if err := b.RestoreClient(cl, blob); err != nil {
		t.Fatalf("RestoreClient: %v", err)
	}
	if b.countState[cl] != 42 {
		t.Fatalf("restored count = %d, want 42", b.countState[cl])
	}
}

func TestAggregatorAvgSnapshotRestore(t *testing.T) {
	a := newTestAgg()
	cl := uuid.New()
	a.avgState[cl] = map[string]avgState{
		"wire": {sum: 300, count: 3, sample: protocol.Transaction{PaymentFormat: "wire", FromBank: "b"}},
	}

	blob, err := a.SnapshotClient(cl)
	if err != nil {
		t.Fatalf("SnapshotClient: %v", err)
	}
	b := newTestAgg()
	if err := b.RestoreClient(cl, blob); err != nil {
		t.Fatalf("RestoreClient: %v", err)
	}
	got := b.avgState[cl]["wire"]
	if got.sum != 300 || got.count != 3 || got.sample.PaymentFormat != "wire" {
		t.Fatalf("restored avg mismatch: %+v", got)
	}
}

func TestAggregatorRestoreEmptyIsNoop(t *testing.T) {
	a := newTestAgg()
	if err := a.RestoreClient(uuid.New(), nil); err != nil {
		t.Fatalf("RestoreClient(nil): %v", err)
	}
}
