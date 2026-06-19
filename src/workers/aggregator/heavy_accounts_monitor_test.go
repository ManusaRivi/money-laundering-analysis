package aggregator

import (
	"fmt"
	"sync"
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
	var fired []uuid.UUID
	m := NewHeavyAccountsMonitor(0, 1, func(c uuid.UUID) { fired = append(fired, c) }) // peerTarget 0
	c := uuid.New()

	m.MergeLocal(c, set("s1"), set("d1"))

	if len(fired) != 1 || fired[0] != c {
		t.Fatalf("expected onReady once for %v, got %v", c, fired)
	}
	if _, ok := m.HeavySources(c)[acc("s1")]; !ok {
		t.Fatal("missing local heavy source")
	}
}

func TestMonitor_WaitsForAllPeersThenReadyOnce(t *testing.T) {
	var count int
	m := NewHeavyAccountsMonitor(0, 3, func(uuid.UUID) { count++ }) // peerTarget 2
	c := uuid.New()

	m.MergeLocal(c, set("s0"), nil)
	if count != 0 {
		t.Fatal("ready before any peer done")
	}
	m.RecordHeavyBatch(c, 1, protocol.Q4HeavyRoleSource, []domain.Account{acc("s1")})
	m.RecordDone(c, 1)
	if count != 0 {
		t.Fatal("ready after only one peer")
	}
	m.RecordDone(c, 2)
	if count != 1 {
		t.Fatalf("expected ready exactly once, got %d", count)
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
	var count int
	m := NewHeavyAccountsMonitor(0, 3, func(uuid.UUID) { count++ }) // peerTarget 2
	c := uuid.New()

	m.MergeLocal(c, nil, nil)
	m.RecordDone(c, 1)
	m.RecordDone(c, 1) // redelivery of the same sender must not count twice
	if count != 0 {
		t.Fatal("duplicate done lifted the barrier early")
	}
	m.RecordDone(c, 2)
	if count != 1 {
		t.Fatalf("expected ready exactly once, got %d", count)
	}
}

func TestMonitor_IgnoresSelf(t *testing.T) {
	var count int
	m := NewHeavyAccountsMonitor(0, 2, func(uuid.UUID) { count++ }) // peerTarget 1
	c := uuid.New()

	m.MergeLocal(c, nil, nil)
	m.RecordDone(c, 0)                                                   // self id -> ignored
	m.RecordHeavyBatch(c, 0, protocol.Q4HeavyRoleSource, []domain.Account{acc("x")}) // self -> ignored
	if count != 0 {
		t.Fatal("self message lifted the barrier")
	}
	if _, ok := m.HeavySources(c)[acc("x")]; ok {
		t.Fatal("self batch was merged")
	}
	m.RecordDone(c, 1)
	if count != 1 {
		t.Fatalf("expected ready exactly once, got %d", count)
	}
}

func TestMonitor_PeersBeforeLocal(t *testing.T) {
	var count int
	m := NewHeavyAccountsMonitor(0, 2, func(uuid.UUID) { count++ }) // peerTarget 1
	c := uuid.New()

	m.RecordDone(c, 1) // peer finished before this replica reached its EOF
	if count != 0 {
		t.Fatal("ready without local merge")
	}
	m.MergeLocal(c, nil, nil)
	if count != 1 {
		t.Fatalf("expected ready exactly once, got %d", count)
	}
}

func TestMonitor_SealedIgnoresLateWrites(t *testing.T) {
	m := NewHeavyAccountsMonitor(0, 2, func(uuid.UUID) {}) // peerTarget 1
	c := uuid.New()

	m.MergeLocal(c, set("s0"), nil)
	m.RecordDone(c, 1) // seals
	m.RecordHeavyBatch(c, 2, protocol.Q4HeavyRoleSource, []domain.Account{acc("late")})
	if _, ok := m.HeavySources(c)[acc("late")]; ok {
		t.Fatal("late batch mutated a sealed client")
	}
}

func TestMonitor_RolesSeparate(t *testing.T) {
	m := NewHeavyAccountsMonitor(0, 2, func(uuid.UUID) {})
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

// TestMonitor_ConcurrentExchange runs the realistic shape under -race: peers and
// the local merge all hit the monitor on separate goroutines.
func TestMonitor_ConcurrentExchange(t *testing.T) {
	const workers = 8
	var mu sync.Mutex
	fired := map[uuid.UUID]int{}
	m := NewHeavyAccountsMonitor(0, workers, func(c uuid.UUID) {
		mu.Lock()
		fired[c]++
		mu.Unlock()
	})
	c := uuid.New()

	var wg sync.WaitGroup
	for p := 1; p < workers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			m.RecordHeavyBatch(c, p, protocol.Q4HeavyRoleSource, []domain.Account{acc(fmt.Sprintf("s%d", p))})
			m.RecordDone(c, p)
		}(p)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.MergeLocal(c, set("s0"), nil)
	}()
	wg.Wait()

	mu.Lock()
	n := fired[c]
	mu.Unlock()
	if n != 1 {
		t.Fatalf("expected exactly one onReady, got %d", n)
	}
	if got := len(m.HeavySources(c)); got != workers {
		t.Fatalf("expected %d heavy sources (s0..s%d), got %d", workers, workers-1, got)
	}
}
