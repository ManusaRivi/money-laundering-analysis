package checkpoint

import (
	"bytes"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestSaveLoadRoundTrip(t *testing.T) {
	s, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	want := []byte("snapshot-bytes")
	if err := s.Save("client-1", want); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, ok, err := s.Load("client-1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !ok {
		t.Fatal("expected checkpoint to exist")
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Load = %q, want %q", got, want)
	}
}

func TestLoadMissing(t *testing.T) {
	s, _ := NewFileStore(t.TempDir())
	_, ok, err := s.Load("nope")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false for missing key")
	}
}

func TestOverwrite(t *testing.T) {
	s, _ := NewFileStore(t.TempDir())
	if err := s.Save("c", []byte("v1")); err != nil {
		t.Fatalf("Save v1: %v", err)
	}
	if err := s.Save("c", []byte("v2")); err != nil {
		t.Fatalf("Save v2: %v", err)
	}
	got, _, _ := s.Load("c")
	if !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("Load = %q, want v2", got)
	}
}

func TestDelete(t *testing.T) {
	s, _ := NewFileStore(t.TempDir())
	s.Save("c", []byte("v"))
	if err := s.Delete("c"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok, _ := s.Load("c"); ok {
		t.Fatal("expected key to be gone after Delete")
	}
	if err := s.Delete("c"); err != nil {
		t.Fatalf("Delete of missing key should be a no-op: %v", err)
	}
}

func TestKeysExcludesTmp(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewFileStore(dir)
	s.Save("a", []byte("x"))
	s.Save("b", []byte("y"))
	if err := os.WriteFile(filepath.Join(dir, "orphan"+tmpExt), []byte("partial"), 0o644); err != nil {
		t.Fatalf("write stray tmp: %v", err)
	}
	keys, err := s.Keys()
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	sort.Strings(keys)
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
		t.Fatalf("Keys = %v, want [a b]", keys)
	}
}

func TestInvalidKey(t *testing.T) {
	s, _ := NewFileStore(t.TempDir())
	for _, k := range []string{"", ".", "..", "a/b", `a\b`} {
		if err := s.Save(k, []byte("x")); err == nil {
			t.Errorf("Save(%q) should have failed", k)
		}
	}
}

func TestRestoreAcrossInstances(t *testing.T) {
	dir := t.TempDir()
	first, _ := NewFileStore(dir)
	first.Save("c1", []byte("state-1"))
	first.Save("c2", []byte("state-2"))

	second, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("re-open store: %v", err)
	}
	keys, _ := second.Keys()
	sort.Strings(keys)
	if len(keys) != 2 || keys[0] != "c1" || keys[1] != "c2" {
		t.Fatalf("Keys after restart = %v, want [c1 c2]", keys)
	}
	got, ok, _ := second.Load("c1")
	if !ok || !bytes.Equal(got, []byte("state-1")) {
		t.Fatalf("Load c1 after restart = %q ok=%v, want state-1", got, ok)
	}
}
