package checkpoint

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	fileExt = ".ckpt"
	tmpExt  = ".ckpt.tmp"
)

type FileStore struct {
	dir string
	mu  sync.Mutex
}

func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating checkpoint dir: %w", err)
	}
	return &FileStore{dir: dir}, nil
}

func (s *FileStore) Save(key string, data []byte) error {
	if err := validKey(key); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	final := filepath.Join(s.dir, key+fileExt)
	tmp := filepath.Join(s.dir, key+tmpExt)

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("opening temp checkpoint: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("writing checkpoint: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("syncing checkpoint: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("closing checkpoint: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("renaming checkpoint: %w", err)
	}
	return s.syncDir()
}

func (s *FileStore) Load(key string) ([]byte, bool, error) {
	if err := validKey(key); err != nil {
		return nil, false, err
	}
	data, err := os.ReadFile(filepath.Join(s.dir, key+fileExt))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("reading checkpoint: %w", err)
	}
	return data, true, nil
}

func (s *FileStore) Delete(key string) error {
	if err := validKey(key); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.Remove(filepath.Join(s.dir, key+fileExt)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("deleting checkpoint: %w", err)
	}
	return s.syncDir()
}

func (s *FileStore) Keys() ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("listing checkpoints: %w", err)
	}
	keys := make([]string, 0, len(entries))
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, fileExt) {
			continue
		}
		keys = append(keys, strings.TrimSuffix(name, fileExt))
	}
	return keys, nil
}

func (s *FileStore) syncDir() error {
	d, err := os.Open(s.dir)
	if err != nil {
		return fmt.Errorf("opening dir for sync: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("syncing dir: %w", err)
	}
	return nil
}

func validKey(key string) error {
	if key == "" || key == "." || key == ".." || strings.ContainsAny(key, `/\`) {
		return fmt.Errorf("invalid checkpoint key: %q", key)
	}
	return nil
}
