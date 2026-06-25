package aggregator

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/google/uuid"
)

func acc(id string) domain.Account { return domain.Account{BankID: "b", ID: id} }

func set(ids ...string) map[domain.Account]struct{} {
	m := make(map[domain.Account]struct{}, len(ids))
	for _, id := range ids {
		m[acc(id)] = struct{}{}
	}
	return m
}

func TestMonitor_SingleWorkerReadyOnLocalMerge(t *testing.T) {
	m := NewHeavyAccountsMonitor(0, 1) // peerTarget 0
	c := uuid.New()

	if !m.MergeLocal(c, set("s1"), set("d1")) {
		t.Fatal("expected MergeLocal to lift the barrier for a single worker")
	}
	if _, ok := m.HeavySources(c)[acc("s1")]; !ok {
		t.Fatal("missing local heavy source")
	}
}

func TestMonitor_WaitsForAllPeersThenReadyOnce(t *testing.T) {
	m := NewHeavyAccountsMonitor(0, 3) // peerTarget 2
	c := uuid.New()

	if m.MergeLocal(c, set("s0"), nil) {
		t.Fatal("ready before any peer done")
	}
	m.RecordHeavyBatch(c, 1, protocol.Q4HeavyRoleSource, []domain.Account{acc("s1")})
	if m.RecordDone(c, 1) {
		t.Fatal("ready after only one peer")
	}
	if !m.RecordDone(c, 2) {
		t.Fatal("expected ready after the final peer")
	}

	src := m.HeavySources(c)
	if _, ok := src[acc("s0")]; !ok {
		t.Fatal("missing local source in global set")
	}
	if _, ok := src[acc("s1")]; !ok {
		t.Fatal("missing peer source in global set")
	}
}

func TestMonitor_DoneDedup(t *testing.T) {
	m := NewHeavyAccountsMonitor(0, 3) // peerTarget 2
	c := uuid.New()

	m.MergeLocal(c, nil, nil)
	if m.RecordDone(c, 1) {
		t.Fatal("ready after only one peer")
	}
	if m.RecordDone(c, 1) {
		t.Fatal("duplicate done lifted the barrier early")
	}
	if !m.RecordDone(c, 2) {
		t.Fatal("expected ready after the final distinct peer")
	}
}

func TestMonitor_IgnoresSelf(t *testing.T) {
	m := NewHeavyAccountsMonitor(0, 2) // peerTarget 1
	c := uuid.New()

	m.MergeLocal(c, nil, nil)
	if m.RecordDone(c, 0) { // self id -> ignored
		t.Fatal("self done lifted the barrier")
	}
	m.RecordHeavyBatch(c, 0, protocol.Q4HeavyRoleSource, []domain.Account{acc("x")}) // self -> ignored
	if _, ok := m.HeavySources(c)[acc("x")]; ok {
		t.Fatal("self batch was merged")
	}
	if !m.RecordDone(c, 1) {
		t.Fatal("expected ready after the one real peer")
	}
}

func TestMonitor_PeersBeforeLocal(t *testing.T) {
	m := NewHeavyAccountsMonitor(0, 2) // peerTarget 1
	c := uuid.New()

	if m.RecordDone(c, 1) { // peer finished before this replica reached its EOF
		t.Fatal("ready without local merge")
	}
	if !m.MergeLocal(c, nil, nil) {
		t.Fatal("expected ready once the local merge completes the barrier")
	}
}

func TestMonitor_SealedIgnoresLateWrites(t *testing.T) {
	m := NewHeavyAccountsMonitor(0, 2) // peerTarget 1
	c := uuid.New()

	m.MergeLocal(c, set("s0"), nil)
	m.RecordDone(c, 1) // seals
	m.RecordHeavyBatch(c, 2, protocol.Q4HeavyRoleSource, []domain.Account{acc("late")})
	if _, ok := m.HeavySources(c)[acc("late")]; ok {
		t.Fatal("late batch mutated a sealed client")
	}
	if m.RecordDone(c, 3) {
		t.Fatal("post-seal done re-lifted the barrier")
	}
}

func TestMonitor_RolesSeparate(t *testing.T) {
	m := NewHeavyAccountsMonitor(0, 2)
	c := uuid.New()

	m.RecordHeavyBatch(c, 1, protocol.Q4HeavyRoleSource, []domain.Account{acc("s")})
	m.RecordHeavyBatch(c, 1, protocol.Q4HeavyRoleSink, []domain.Account{acc("d")})
	if _, ok := m.HeavySources(c)[acc("s")]; !ok {
		t.Fatal("source missing")
	}
	if _, ok := m.HeavySinks(c)[acc("d")]; !ok {
		t.Fatal("sink missing")
	}
	if _, ok := m.HeavySources(c)[acc("d")]; ok {
		t.Fatal("sink leaked into sources")
	}
}

// TestMonitor_SnapshotRestoreRoundTrip is the crux of crash recovery: a snapshot
// taken mid-barrier must restore enough state (heavy sets + donePeers + localDone)
// that the redelivered tail of the protocol still completes — without re-counting an
// already-recorded peer.
func TestMonitor_SnapshotRestoreRoundTrip(t *testing.T) {
	m := NewHeavyAccountsMonitor(0, 3) // peerTarget 2
	c := uuid.New()

	m.MergeLocal(c, set("s0"), set("d0"))
	m.RecordHeavyBatch(c, 1, protocol.Q4HeavyRoleSource, []domain.Account{acc("s1")})
	if m.RecordDone(c, 1) { // not ready yet (peer 2 still outstanding)
		t.Fatal("barrier lifted before peer 2")
	}

	snap, ok := m.Snapshot(c)
	if !ok {
		t.Fatal("expected a snapshot for an active client")
	}

	// Fresh monitor, as after a restart, restored from the snapshot.
	restored := NewHeavyAccountsMonitor(0, 3)
	restored.Restore(c, snap)

	// donePeers and localDone survived: only peer 2 remains, and recording it lifts
	// the barrier exactly once.
	if !restored.RecordDone(c, 2) {
		t.Fatal("expected the restored barrier to complete on the final peer")
	}
	if _, ok := restored.HeavySources(c)[acc("s1")]; !ok {
		t.Fatal("restored monitor lost a peer's heavy source")
	}
	if _, ok := restored.HeavySources(c)[acc("s0")]; !ok {
		t.Fatal("restored monitor lost the local heavy source")
	}
	// A redelivery of peer 1's done after restore must not matter (already sealed).
	if restored.RecordDone(c, 1) {
		t.Fatal("post-restore redelivery re-lifted the barrier")
	}
}

func TestMonitor_RestoreSealed(t *testing.T) {
	m := NewHeavyAccountsMonitor(0, 2)
	c := uuid.New()
	m.Restore(c, degreeStateSnapshot{
		HeavySrcs: []domain.Account{acc("s")},
		HeavySinks: []domain.Account{acc("d")},
		Sealed:    true,
		LocalDone: true,
	})
	if !m.IsSealed(c) {
		t.Fatal("expected restored client to be sealed")
	}
	if _, ok := m.HeavySources(c)[acc("s")]; !ok {
		t.Fatal("sealed restore lost heavy source")
	}
}

// TestMonitor_ConcurrentExchange keeps the monitor's locking honest under -race:
// peers and the local merge all hit it from separate goroutines, and the barrier
// must still lift exactly once.
func TestMonitor_ConcurrentExchange(t *testing.T) {
	const workers = 8
	m := NewHeavyAccountsMonitor(0, workers)
	c := uuid.New()

	var readyCount int32
	markReady := func(ready bool) {
		if ready {
			atomic.AddInt32(&readyCount, 1)
		}
	}

	var wg sync.WaitGroup
	for p := 1; p < workers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			m.RecordHeavyBatch(c, p, protocol.Q4HeavyRoleSource, []domain.Account{acc(fmt.Sprintf("s%d", p))})
			markReady(m.RecordDone(c, p))
		}(p)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		markReady(m.MergeLocal(c, set("s0"), nil))
	}()
	wg.Wait()

	if n := atomic.LoadInt32(&readyCount); n != 1 {
		t.Fatalf("expected exactly one ready, got %d", n)
	}
	if got := len(m.HeavySources(c)); got != workers {
		t.Fatalf("expected %d heavy sources (s0..s%d), got %d", workers, workers-1, got)
	}
}
