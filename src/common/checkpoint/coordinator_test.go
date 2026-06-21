package checkpoint

import (
	"bytes"
	"testing"

	"github.com/google/uuid"
)

type fakeCheckpointable struct {
	data     map[uuid.UUID][]byte
	restored map[uuid.UUID][]byte
}

func newFake() *fakeCheckpointable {
	return &fakeCheckpointable{
		data:     make(map[uuid.UUID][]byte),
		restored: make(map[uuid.UUID][]byte),
	}
}

func (f *fakeCheckpointable) SnapshotClient(clientID uuid.UUID) ([]byte, error) {
	return f.data[clientID], nil
}

func (f *fakeCheckpointable) RestoreClient(clientID uuid.UUID, data []byte) error {
	f.restored[clientID] = data
	return nil
}

func TestCoordinatorBuffersAcksUntilInterval(t *testing.T) {
	mgr, _ := NewManager(t.TempDir())
	dedup, eof := newFake(), newFake()
	cl := uuid.New()
	dedup.data[cl] = []byte("seen")
	eof.data[cl] = []byte("eofcounts")

	co := NewCoordinator(mgr, dedup, eof, nil, 3)

	acks := 0
	ack := func() { acks++ }

	for i := 0; i < 2; i++ {
		if err := co.Track(cl, ack); err != nil {
			t.Fatalf("Track: %v", err)
		}
	}
	if acks != 0 {
		t.Fatalf("acks fired before interval: %d", acks)
	}
	if _, ok, _ := mgr.Load(cl.String()); ok {
		t.Fatal("checkpoint persisted before interval")
	}

	if err := co.Track(cl, ack); err != nil {
		t.Fatalf("Track: %v", err)
	}
	if acks != 3 {
		t.Fatalf("acks after interval = %d, want 3", acks)
	}
	cp, ok, err := mgr.Load(cl.String())
	if err != nil || !ok {
		t.Fatalf("checkpoint not persisted: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(cp.Dedup, []byte("seen")) || !bytes.Equal(cp.EOF, []byte("eofcounts")) {
		t.Fatalf("persisted blob mismatch: %+v", cp)
	}
	if len(cp.State) != 0 {
		t.Fatalf("stateless worker should have empty State, got %d bytes", len(cp.State))
	}
}

func TestCoordinatorExplicitFlush(t *testing.T) {
	mgr, _ := NewManager(t.TempDir())
	dedup := newFake()
	cl := uuid.New()
	dedup.data[cl] = []byte("x")
	co := NewCoordinator(mgr, dedup, nil, nil, 100)

	acks := 0
	co.Track(cl, func() { acks++ })
	if acks != 0 {
		t.Fatal("should not have flushed (interval not reached)")
	}
	if err := co.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if acks != 1 {
		t.Fatalf("explicit Flush should fire buffered acks: %d", acks)
	}
	if _, ok, _ := mgr.Load(cl.String()); !ok {
		t.Fatal("explicit Flush should persist")
	}
}

func TestCoordinatorRecover(t *testing.T) {
	dir := t.TempDir()
	cl := uuid.New()

	mgr1, _ := NewManager(dir)
	d1, e1 := newFake(), newFake()
	d1.data[cl] = []byte("seen-bytes")
	e1.data[cl] = []byte("eof-bytes")
	co1 := NewCoordinator(mgr1, d1, e1, nil, 1)
	if err := co1.Track(cl, func() {}); err != nil {
		t.Fatalf("Track: %v", err)
	}

	mgr2, _ := NewManager(dir)
	d2, e2 := newFake(), newFake()
	co2 := NewCoordinator(mgr2, d2, e2, nil, 1)
	if err := co2.Recover(); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if !bytes.Equal(d2.restored[cl], []byte("seen-bytes")) {
		t.Fatalf("dedup not restored: %q", d2.restored[cl])
	}
	if !bytes.Equal(e2.restored[cl], []byte("eof-bytes")) {
		t.Fatalf("eof not restored: %q", e2.restored[cl])
	}
}

func TestCoordinatorNoFlushNoPersist(t *testing.T) {
	mgr, _ := NewManager(t.TempDir())
	co := NewCoordinator(mgr, newFake(), newFake(), nil, 5)
	co.Track(uuid.New(), func() {})
	keys, _ := mgr.Keys()
	if len(keys) != 0 {
		t.Fatalf("nothing should be persisted before a flush, got %v", keys)
	}
}

func TestCoordinatorDelete(t *testing.T) {
	mgr, _ := NewManager(t.TempDir())
	dedup := newFake()
	cl := uuid.New()
	dedup.data[cl] = []byte("x")
	co := NewCoordinator(mgr, dedup, nil, nil, 1)
	co.Track(cl, func() {})
	if _, ok, _ := mgr.Load(cl.String()); !ok {
		t.Fatal("expected persisted")
	}
	if err := co.Delete(cl); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok, _ := mgr.Load(cl.String()); ok {
		t.Fatal("expected gone after Delete")
	}
}
