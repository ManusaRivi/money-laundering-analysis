package checkpoint

import (
	"bytes"
	"path/filepath"
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

type fakeSeen struct {
	pending  map[uuid.UUID][]byte
	replayed map[uuid.UUID][]byte
}

func newFakeSeen() *fakeSeen {
	return &fakeSeen{
		pending:  make(map[uuid.UUID][]byte),
		replayed: make(map[uuid.UUID][]byte),
	}
}

func (f *fakeSeen) DrainClient(clientID uuid.UUID) ([]byte, error) {
	return f.pending[clientID], nil
}

func (f *fakeSeen) CommitClient(clientID uuid.UUID) error {
	delete(f.pending, clientID)
	return nil
}

func (f *fakeSeen) ReplayClient(clientID uuid.UUID, record []byte) error {
	f.replayed[clientID] = append(f.replayed[clientID], record...)
	return nil
}

func TestCoordinatorBuffersAcksUntilInterval(t *testing.T) {
	dir := t.TempDir()
	seen, eof := newFakeSeen(), newFake()
	cl := uuid.New()
	seen.pending[cl] = []byte("ids")
	eof.data[cl] = []byte("eofcounts")

	co, err := NewCoordinator(dir, seen, eof, nil, 3)
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}

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
	seenStore, _ := NewAppendStore(filepath.Join(dir, seenDir))
	if recs, _, _ := seenStore.Load(cl.String()); len(recs) != 0 {
		t.Fatal("checkpoint persisted before interval")
	}

	if err := co.Track(cl, ack); err != nil {
		t.Fatalf("Track: %v", err)
	}
	if acks != 3 {
		t.Fatalf("acks after interval = %d, want 3", acks)
	}

	recs, gen, err := seenStore.Load(cl.String())
	if err != nil {
		t.Fatalf("seen load: %v", err)
	}
	if gen != 1 || len(recs) != 1 || !bytes.Equal(recs[0].Payload, []byte("ids")) {
		t.Fatalf("seen frame mismatch: gen=%d recs=%v", gen, recs)
	}
	eofStore, _ := NewOverwriteStore(filepath.Join(dir, eofDir))
	data, _, ok, _ := eofStore.Load(cl.String())
	if !ok || !bytes.Equal(data, []byte("eofcounts")) {
		t.Fatalf("eof not persisted: %q ok=%v", data, ok)
	}
}

func TestCoordinatorExplicitFlush(t *testing.T) {
	dir := t.TempDir()
	seen := newFakeSeen()
	cl := uuid.New()
	seen.pending[cl] = []byte("x")
	co, err := NewCoordinator(dir, seen, nil, nil, 100)
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}

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
	seenStore, _ := NewAppendStore(filepath.Join(dir, seenDir))
	if _, gen, _ := seenStore.Load(cl.String()); gen != 1 {
		t.Fatal("explicit Flush should persist a seen frame")
	}
}

func TestCoordinatorRecover(t *testing.T) {
	dir := t.TempDir()
	cl := uuid.New()

	seen1, e1 := newFakeSeen(), newFake()
	seen1.pending[cl] = []byte("seen-bytes")
	e1.data[cl] = []byte("eof-bytes")
	co1, err := NewCoordinator(dir, seen1, e1, nil, 1)
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}
	if err := co1.Track(cl, func() {}); err != nil {
		t.Fatalf("Track: %v", err)
	}

	seen2, e2 := newFakeSeen(), newFake()
	co2, err := NewCoordinator(dir, seen2, e2, nil, 1)
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}
	if err := co2.Recover(); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if !bytes.Equal(seen2.replayed[cl], []byte("seen-bytes")) {
		t.Fatalf("seen not replayed: %q", seen2.replayed[cl])
	}
	if !bytes.Equal(e2.restored[cl], []byte("eof-bytes")) {
		t.Fatalf("eof not restored: %q", e2.restored[cl])
	}
}

func TestCoordinatorNoFlushNoPersist(t *testing.T) {
	dir := t.TempDir()
	co, err := NewCoordinator(dir, newFakeSeen(), newFake(), nil, 5)
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}
	co.Track(uuid.New(), func() {})
	seenStore, _ := NewAppendStore(filepath.Join(dir, seenDir))
	keys, _ := seenStore.Keys()
	if len(keys) != 0 {
		t.Fatalf("nothing should be persisted before a flush, got %v", keys)
	}
}

func TestCoordinatorDuplicateAcksWithoutCheckpoint(t *testing.T) {
	dir := t.TempDir()
	co, err := NewCoordinator(dir, newFakeSeen(), nil, nil, 1)
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}
	acked := false
	if err := co.Track(uuid.Nil, func() { acked = true }); err != nil {
		t.Fatalf("Track: %v", err)
	}
	if !acked {
		t.Fatal("a duplicate (uuid.Nil) must still be acked")
	}
	seenStore, _ := NewAppendStore(filepath.Join(dir, seenDir))
	if keys, _ := seenStore.Keys(); len(keys) != 0 {
		t.Fatalf("uuid.Nil must not be checkpointed, got %v", keys)
	}
}

func TestCoordinatorDelete(t *testing.T) {
	dir := t.TempDir()
	seen := newFakeSeen()
	cl := uuid.New()
	seen.pending[cl] = []byte("x")
	co, err := NewCoordinator(dir, seen, nil, nil, 1)
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}
	co.Track(cl, func() {})
	seenStore, _ := NewAppendStore(filepath.Join(dir, seenDir))
	if _, gen, _ := seenStore.Load(cl.String()); gen != 1 {
		t.Fatal("expected persisted")
	}
	if err := co.Delete(cl); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if recs, _, _ := seenStore.Load(cl.String()); len(recs) != 0 {
		t.Fatal("expected gone after Delete")
	}
}
