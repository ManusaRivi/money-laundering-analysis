package checkpoint

import (
	"encoding/binary"
	"fmt"

	"github.com/google/uuid"
)

const checkpointVersion = 1

type Checkpointable interface {
	SnapshotClient(clientID uuid.UUID) ([]byte, error)
	RestoreClient(clientID uuid.UUID, data []byte) error
}

type Checkpoint struct {
	Dedup []byte
	EOF   []byte
	State []byte
}

func (c Checkpoint) Encode() []byte {
	buf := make([]byte, 0, 1+4*3+len(c.Dedup)+len(c.EOF)+len(c.State))
	buf = append(buf, checkpointVersion)
	buf = appendField(buf, c.Dedup)
	buf = appendField(buf, c.EOF)
	buf = appendField(buf, c.State)
	return buf
}

func Decode(data []byte) (Checkpoint, error) {
	if len(data) < 1 {
		return Checkpoint{}, fmt.Errorf("checkpoint: empty data")
	}
	if data[0] != checkpointVersion {
		return Checkpoint{}, fmt.Errorf("checkpoint: unsupported version %d", data[0])
	}
	rest := data[1:]

	dedup, rest, err := readField(rest)
	if err != nil {
		return Checkpoint{}, fmt.Errorf("checkpoint: dedup field: %w", err)
	}
	eof, rest, err := readField(rest)
	if err != nil {
		return Checkpoint{}, fmt.Errorf("checkpoint: eof field: %w", err)
	}
	state, _, err := readField(rest)
	if err != nil {
		return Checkpoint{}, fmt.Errorf("checkpoint: state field: %w", err)
	}
	return Checkpoint{Dedup: dedup, EOF: eof, State: state}, nil
}

func appendField(buf, field []byte) []byte {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(field)))
	buf = append(buf, length[:]...)
	return append(buf, field...)
}

func readField(data []byte) ([]byte, []byte, error) {
	if len(data) < 4 {
		return nil, nil, fmt.Errorf("truncated length prefix")
	}
	n := binary.BigEndian.Uint32(data[:4])
	data = data[4:]
	if uint32(len(data)) < n {
		return nil, nil, fmt.Errorf("truncated field: need %d, have %d", n, len(data))
	}
	return data[:n], data[n:], nil
}

type Manager struct {
	store *FileStore
}

func NewManager(dir string) (*Manager, error) {
	store, err := NewFileStore(dir)
	if err != nil {
		return nil, err
	}
	return &Manager{store: store}, nil
}

func (m *Manager) Save(clientID string, cp Checkpoint) error {
	return m.store.Save(clientID, cp.Encode())
}

func (m *Manager) Load(clientID string) (Checkpoint, bool, error) {
	data, ok, err := m.store.Load(clientID)
	if err != nil || !ok {
		return Checkpoint{}, ok, err
	}
	cp, err := Decode(data)
	if err != nil {
		return Checkpoint{}, false, err
	}
	return cp, true, nil
}

func (m *Manager) Delete(clientID string) error {
	return m.store.Delete(clientID)
}

func (m *Manager) Keys() ([]string, error) {
	return m.store.Keys()
}
