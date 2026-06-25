package checkpoint

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/uuid"
)

type Coordinator struct {
	manager  *Manager
	dedup    Checkpointable
	eof      Checkpointable
	state    Checkpointable
	interval int

	mu          sync.Mutex
	seenClients map[uuid.UUID]struct{}
	acks        []func()
	pending     int
}

func NewCoordinator(manager *Manager, dedup, eof, state Checkpointable, interval int) *Coordinator {
	if interval < 1 {
		interval = 1
	}
	return &Coordinator{
		manager:     manager,
		dedup:       dedup,
		eof:         eof,
		state:       state,
		interval:    interval,
		seenClients: make(map[uuid.UUID]struct{}),
	}
}

func (co *Coordinator) Recover() error {
	slog.Debug("Recovering checkpoint coordinator")
	keys, err := co.manager.Keys()
	if err != nil {
		return err
	}
	for _, key := range keys {
		clientID, err := uuid.Parse(key)
		if err != nil {
			continue
		}
		cp, ok, err := co.manager.Load(key)
		if err != nil {
			return fmt.Errorf("recover %s: %w", key, err)
		}
		if !ok {
			continue
		}
		if err := co.restore(clientID, cp); err != nil {
			return fmt.Errorf("recover %s: %w", key, err)
		}
	}
	return nil
}

func (co *Coordinator) Track(clientID uuid.UUID, ack func()) error {
	co.mu.Lock()
	co.seenClients[clientID] = struct{}{}
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
	for clientID := range co.seenClients {
		cp, err := co.snapshot(clientID)
		if err != nil {
			co.mu.Unlock()
			return err
		}
		if err := co.manager.Save(clientID.String(), cp); err != nil {
			co.mu.Unlock()
			return err
		}
	}
	acks := co.acks
	co.seenClients = make(map[uuid.UUID]struct{})
	co.acks = nil
	co.pending = 0
	co.mu.Unlock()

	for _, ack := range acks {
		ack()
	}
	return nil
}

func (co *Coordinator) Delete(clientID uuid.UUID) error {
	co.mu.Lock()
	defer co.mu.Unlock()
	delete(co.seenClients, clientID)
	return co.manager.Delete(clientID.String())
}

// SaveClient durably persists one client's checkpoint immediately, outside the
// interval-batched Flush and without touching the pending-ack batch. Use it to
// capture state at a specific, irreversible point — e.g. the ScatterAndGather
// global heavy sets at barrier seal, before the cross-product consumes the
// adjacency — so a crash after that point recovers without re-running the
// consensus. The snapshot is a full overwrite (same as Flush's per-client save).
func (co *Coordinator) SaveClient(clientID uuid.UUID) error {
	cp, err := co.snapshot(clientID)
	if err != nil {
		return err
	}
	return co.manager.Save(clientID.String(), cp)
}

func (co *Coordinator) snapshot(clientID uuid.UUID) (Checkpoint, error) {
	dedup, err := co.dedup.SnapshotClient(clientID)
	if err != nil {
		return Checkpoint{}, err
	}
	cp := Checkpoint{Dedup: dedup}
	if co.eof != nil {
		if cp.EOF, err = co.eof.SnapshotClient(clientID); err != nil {
			return Checkpoint{}, err
		}
	}
	if co.state != nil {
		if cp.State, err = co.state.SnapshotClient(clientID); err != nil {
			return Checkpoint{}, err
		}
	}
	return cp, nil
}

func (co *Coordinator) restore(clientID uuid.UUID, cp Checkpoint) error {
	if err := co.dedup.RestoreClient(clientID, cp.Dedup); err != nil {
		return err
	}
	if co.eof != nil {
		if err := co.eof.RestoreClient(clientID, cp.EOF); err != nil {
			return err
		}
	}
	if co.state != nil {
		if err := co.state.RestoreClient(clientID, cp.State); err != nil {
			return err
		}
	}
	return nil
}
