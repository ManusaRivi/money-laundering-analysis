package aggregator

import (
	"encoding/json"
	"log/slog"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/checkpoint"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
)

var _ checkpoint.Checkpointable = (*Aggregator)(nil)

type aggCheckpoint struct {
	Count int                             `json:"count"`
	State map[string]protocol.Transaction `json:"state,omitempty"`
	Avg   map[string]avgCheckpoint        `json:"avg,omitempty"`
}

type avgCheckpoint struct {
	Sum    float64              `json:"sum"`
	Count  int                  `json:"count"`
	Sample protocol.Transaction `json:"sample"`
}

func (a *Aggregator) SnapshotClient(clientID uuid.UUID) ([]byte, error) {
	cp := aggCheckpoint{
		Count: a.countState[clientID],
		State: a.state[clientID],
	}
	if avg := a.avgState[clientID]; len(avg) > 0 {
		cp.Avg = make(map[string]avgCheckpoint, len(avg))
		for key, st := range avg {
			cp.Avg[key] = avgCheckpoint{Sum: st.sum, Count: st.count, Sample: st.sample}
		}
	}
	return json.Marshal(cp)
}

func (a *Aggregator) RestoreClient(clientID uuid.UUID, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	slog.Debug("Restoring aggregator state for client", "clientID", clientID)
	var cp aggCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return err
	}
	if cp.Count != 0 {
		a.countState[clientID] = cp.Count
	}
	if len(cp.State) > 0 {
		a.state[clientID] = cp.State
	}
	if len(cp.Avg) > 0 {
		avg := make(map[string]avgState, len(cp.Avg))
		for key, st := range cp.Avg {
			avg[key] = avgState{sum: st.Sum, count: st.Count, sample: st.Sample}
		}
		a.avgState[clientID] = avg
	}
	return nil
}
