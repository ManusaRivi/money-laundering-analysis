package checkpoint

import (
	"bytes"
	"testing"
)

func TestCheckpointRoundTrip(t *testing.T) {
	cp := Checkpoint{
		Dedup: []byte("seen-set-bytes"),
		EOF:   []byte("eof-counts"),
		State: []byte("aggregator-state"),
	}
	got, err := Decode(cp.Encode())
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(got.Dedup, cp.Dedup) || !bytes.Equal(got.EOF, cp.EOF) || !bytes.Equal(got.State, cp.State) {
		t.Fatalf("round-trip mismatch: %+v vs %+v", got, cp)
	}
}

func TestCheckpointEmptyState(t *testing.T) {
	cp := Checkpoint{Dedup: []byte("seen"), EOF: []byte("eof")}
	got, err := Decode(cp.Encode())
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(got.Dedup, cp.Dedup) || !bytes.Equal(got.EOF, cp.EOF) || len(got.State) != 0 {
		t.Fatalf("stateless round-trip mismatch: %+v", got)
	}
}

func TestDecodeRejectsBadInput(t *testing.T) {
	if _, err := Decode(nil); err == nil {
		t.Error("expected error on empty data")
	}
	if _, err := Decode([]byte{99, 0, 0, 0, 0}); err == nil {
		t.Error("expected error on bad version")
	}
	full := Checkpoint{Dedup: []byte("abc"), EOF: []byte("d"), State: []byte("e")}.Encode()
	if _, err := Decode(full[:len(full)-2]); err == nil {
		t.Error("expected error on truncated blob")
	}
}

func TestManagerRoundTrip(t *testing.T) {
	m, err := NewManager(t.TempDir())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	cp := Checkpoint{Dedup: []byte("d"), EOF: []byte("e"), State: []byte("s")}
	if err := m.Save("client-1", cp); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, ok, err := m.Load("client-1")
	if err != nil || !ok {
		t.Fatalf("Load: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got.State, cp.State) {
		t.Fatalf("Load State = %q, want %q", got.State, cp.State)
	}

	keys, _ := m.Keys()
	if len(keys) != 1 || keys[0] != "client-1" {
		t.Fatalf("Keys = %v, want [client-1]", keys)
	}

	if err := m.Delete("client-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok, _ := m.Load("client-1"); ok {
		t.Fatal("expected gone after Delete")
	}
}

func TestManagerLoadMissing(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	if _, ok, err := m.Load("nope"); ok || err != nil {
		t.Fatalf("Load missing: ok=%v err=%v", ok, err)
	}
}
