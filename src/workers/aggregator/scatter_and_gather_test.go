package aggregator

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func newTestSG() *ScatterAndGather {
	return &ScatterAndGather{
		clients:   make(map[uuid.UUID]*client),
		completed: make(map[uuid.UUID]struct{}),
		monitor:   NewHeavyAccountsMonitor(0, 1),
	}
}

func TestScatterGather_CompletedMarkerRoundTrip(t *testing.T) {
	a := newTestSG()
	c := uuid.New()

	client := a.getClient(c)
	client.scatterGroups[acc("s")] = accountSet{acc("d"): {}}
	a.monitor.MergeLocal(c, set("s"), set("d"))
	a.markCompleted(c)

	blob, err := a.SnapshotClient(c)
	if err != nil {
		t.Fatalf("SnapshotClient: %v", err)
	}

	var cp scatterGatherCheckpoint
	if err := json.Unmarshal(blob, &cp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !cp.Completed || len(cp.ScatterGroups) != 0 || len(cp.HeavySources) != 0 || cp.Sealed {
		t.Fatalf("expected bare completed marker, got %+v", cp)
	}

	b := newTestSG()
	if err := b.RestoreClient(c, blob); err != nil {
		t.Fatalf("RestoreClient: %v", err)
	}
	if !b.isCompleted(c) {
		t.Fatal("expected client tombstoned after restoring completed marker")
	}
	if _, live := b.clients[c]; live {
		t.Fatal("completed marker must not restore live client state")
	}
	if _, ok := b.monitor.Snapshot(c); ok {
		t.Fatal("completed marker must not restore monitor state")
	}
}
