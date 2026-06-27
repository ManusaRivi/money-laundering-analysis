package checkpoint

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
)

const (
	seenDir  = "seen"
	eofDir   = "eof"
	stateDir = "state"
)

type Coordinator struct {
	seen      AppendSource
	seenStore *AppendStore

	eof      Checkpointable
	eofStore *OverwriteStore

	state      Checkpointable
	stateStore *OverwriteStore

	interval int

	mu           sync.Mutex
	dirty        map[uuid.UUID]struct{}
	acks         []func()
	pending      int
	committedGen map[uuid.UUID]uint64
}

func NewCoordinator(dir string, seen AppendSource, eof, state Checkpointable, interval int) (*Coordinator, error) {
	if interval < 1 {
		interval = 1
	}
	seenStore, err := NewAppendStore(filepath.Join(dir, seenDir))
	if err != nil {
		return nil, err
	}
	co := &Coordinator{
		seen:         seen,
		seenStore:    seenStore,
		eof:          eof,
		state:        state,
		interval:     interval,
		dirty:        make(map[uuid.UUID]struct{}),
		committedGen: make(map[uuid.UUID]uint64),
	}
	if eof != nil {
		if co.eofStore, err = NewOverwriteStore(filepath.Join(dir, eofDir)); err != nil {
			return nil, err
		}
	}
	if state != nil {
		if co.stateStore, err = NewOverwriteStore(filepath.Join(dir, stateDir)); err != nil {
			return nil, err
		}
	}
	return co, nil
}

func (co *Coordinator) Track(clientID uuid.UUID, ack func()) error {
	co.mu.Lock()
	if clientID != uuid.Nil {
		co.dirty[clientID] = struct{}{}
	}
	co.acks = append(co.acks, ack)
	co.pending++
	flush := co.pending >= co.interval
	co.mu.Unlock()

	if flush {
		return co.Flush()
	}
	return nil
}

func (co *Coordinator) Flush() error {
	co.mu.Lock()
	dirty := co.dirty
	acks := co.acks
	co.dirty = make(map[uuid.UUID]struct{})
	co.acks = nil
	co.pending = 0
	co.mu.Unlock()

	for clientID := range dirty {
		if err := co.checkpoint(clientID); err != nil {
			return err
		}
	}
	for _, ack := range acks {
		ack()
	}
	return nil
}

func (co *Coordinator) SaveClient(clientID uuid.UUID) error {
	return co.checkpoint(clientID)
}

func (co *Coordinator) checkpoint(clientID uuid.UUID) error {
	co.mu.Lock()
	gen := co.committedGen[clientID] + 1
	co.mu.Unlock()

	key := clientID.String()

	if co.state != nil {
		data, err := co.state.SnapshotClient(clientID)
		if err != nil {
			return fmt.Errorf("snapshot state %s: %w", key, err)
		}
		if err := co.stateStore.Stage(key, gen, data); err != nil {
			return err
		}
	}
	if co.eof != nil {
		data, err := co.eof.SnapshotClient(clientID)
		if err != nil {
			return fmt.Errorf("snapshot eof %s: %w", key, err)
		}
		if err := co.eofStore.Stage(key, gen, data); err != nil {
			return err
		}
	}

	delta, err := co.seen.DrainClient(clientID)
	if err != nil {
		return fmt.Errorf("drain seen %s: %w", key, err)
	}
	if err := co.seenStore.Append(key, gen, delta); err != nil {
		return err
	}

	co.mu.Lock()
	co.committedGen[clientID] = gen
	co.mu.Unlock()
	if err := co.seen.CommitClient(clientID); err != nil {
		return fmt.Errorf("commit seen %s: %w", key, err)
	}

	if co.state != nil {
		if err := co.stateStore.Promote(key, gen); err != nil {
			return err
		}
	}
	if co.eof != nil {
		if err := co.eofStore.Promote(key, gen); err != nil {
			return err
		}
	}
	return nil
}

func (co *Coordinator) Recover() error {
	slog.Debug("Recovering checkpoint coordinator")
	keys, err := co.seenStore.Keys()
	if err != nil {
		return err
	}
	for _, key := range keys {
		clientID, err := uuid.Parse(key)
		if err != nil {
			continue
		}

		recs, committedGen, err := co.seenStore.Load(key)
		if err != nil {
			return fmt.Errorf("recover seen %s: %w", key, err)
		}
		if err := co.seenStore.Truncate(key, committedGen); err != nil {
			return fmt.Errorf("recover truncate seen %s: %w", key, err)
		}
		// Restore the overwrite slots before replaying the seen log: a slot whose
		// state carries a terminal marker (e.g. the SaG's completed tombstone) can
		// then have ReplayClient skip rebuilding append-only state it no longer needs.
		if co.state != nil {
			if err := co.restoreOverwrite(co.stateStore, co.state, clientID, key, committedGen); err != nil {
				return fmt.Errorf("recover state %s: %w", key, err)
			}
		}
		if co.eof != nil {
			if err := co.restoreOverwrite(co.eofStore, co.eof, clientID, key, committedGen); err != nil {
				return fmt.Errorf("recover eof %s: %w", key, err)
			}
		}

		for _, rec := range recs {
			if err := co.seen.ReplayClient(clientID, rec.Payload); err != nil {
				return fmt.Errorf("recover replay seen %s: %w", key, err)
			}
		}

		co.committedGen[clientID] = committedGen
	}
	return nil
}

func (co *Coordinator) restoreOverwrite(store *OverwriteStore, src Checkpointable, clientID uuid.UUID, key string, committedGen uint64) error {
	if err := store.Promote(key, committedGen); err != nil {
		return err
	}
	data, _, ok, err := store.Load(key)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	return src.RestoreClient(clientID, data)
}

func (co *Coordinator) Delete(clientID uuid.UUID) error {
	key := clientID.String()
	if err := co.seenStore.Delete(key); err != nil {
		return err
	}
	if co.eofStore != nil {
		if err := co.eofStore.Delete(key); err != nil {
			return err
		}
	}
	if co.stateStore != nil {
		if err := co.stateStore.Delete(key); err != nil {
			return err
		}
	}
	co.mu.Lock()
	delete(co.committedGen, clientID)
	delete(co.dirty, clientID)
	co.mu.Unlock()
	return nil
}
