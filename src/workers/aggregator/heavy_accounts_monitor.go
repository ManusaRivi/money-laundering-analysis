package aggregator

import (
	"sync"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/google/uuid"
)

type HeavyAccountsMonitor struct {
	mu         sync.Mutex
	perClient  map[uuid.UUID]*clientDegreeState
	selfID     int
	peerTarget int // WORKER_AMOUNT - 1
}

type clientDegreeState struct {
	heavySrcs  map[domain.Account]struct{}
	heavySinks map[domain.Account]struct{}
	donePeers  map[int]struct{} // sender ids seen "done" (set => dedups redelivery)
	localDone  bool
	sealed     bool
}

func NewHeavyAccountsMonitor(selfID, workerAmount int) *HeavyAccountsMonitor {
	peerTarget := workerAmount - 1
	peerTarget = max(peerTarget, 0)
	return &HeavyAccountsMonitor{
		perClient:  make(map[uuid.UUID]*clientDegreeState),
		selfID:     selfID,
		peerTarget: peerTarget,
	}
}

// getLocked returns (creating if needed) the exchange state for a client.
func (m *HeavyAccountsMonitor) getLocked(clientID uuid.UUID) *clientDegreeState {
	ce, ok := m.perClient[clientID]
	if !ok {
		ce = &clientDegreeState{
			heavySrcs:  make(map[domain.Account]struct{}),
			heavySinks: make(map[domain.Account]struct{}),
			donePeers:  make(map[int]struct{}),
		}
		m.perClient[clientID] = ce
	}
	return ce
}

func (m *HeavyAccountsMonitor) MergeLocal(clientID uuid.UUID, srcs, sinks map[domain.Account]struct{}) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	ce := m.getLocked(clientID)
	if ce.sealed {
		return false
	}
	for a := range srcs {
		ce.heavySrcs[a] = struct{}{}
	}
	for a := range sinks {
		ce.heavySinks[a] = struct{}{}
	}
	ce.localDone = true
	return m.isReady(ce)
}

func (m *HeavyAccountsMonitor) RecordHeavyBatch(clientID uuid.UUID, senderID int, role uint8, accounts []domain.Account) {
	if senderID == m.selfID {
		return
	}
	m.mu.Lock()
	ce := m.getLocked(clientID)
	if !ce.sealed {
		set := ce.heavySrcs
		if role == protocol.Q4HeavyRoleSink {
			set = ce.heavySinks
		}
		for _, a := range accounts {
			set[a] = struct{}{}
		}
	}
	m.mu.Unlock()
}

func (m *HeavyAccountsMonitor) RecordDone(clientID uuid.UUID, senderID int) bool {
	if senderID == m.selfID {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	ce := m.getLocked(clientID)
	if ce.sealed {
		return false
	}
	ce.donePeers[senderID] = struct{}{}
	return m.isReady(ce)
}

func (m *HeavyAccountsMonitor) isReady(ce *clientDegreeState) bool {
	if ce.sealed || !ce.localDone || len(ce.donePeers) < m.peerTarget {
		return false
	}
	ce.sealed = true
	return true
}

func (m *HeavyAccountsMonitor) HeavySources(clientID uuid.UUID) map[domain.Account]struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ce, ok := m.perClient[clientID]; ok {
		return ce.heavySrcs
	}
	return nil
}

func (m *HeavyAccountsMonitor) HeavySinks(clientID uuid.UUID) map[domain.Account]struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ce, ok := m.perClient[clientID]; ok {
		return ce.heavySinks
	}
	return nil
}

func (m *HeavyAccountsMonitor) IsSealed(clientID uuid.UUID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	ce, ok := m.perClient[clientID]
	return ok && ce.sealed
}

type degreeStateSnapshot struct {
	HeavySrcs  []domain.Account
	HeavySinks []domain.Account
	DonePeers  []int
	LocalDone  bool
	Sealed     bool
}

func (m *HeavyAccountsMonitor) Snapshot(clientID uuid.UUID) (degreeStateSnapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ce, ok := m.perClient[clientID]
	if !ok {
		return degreeStateSnapshot{}, false
	}
	donePeers := make([]int, 0, len(ce.donePeers))
	for id := range ce.donePeers {
		donePeers = append(donePeers, id)
	}
	return degreeStateSnapshot{
		HeavySrcs:  accountSetToSlice(ce.heavySrcs),
		HeavySinks: accountSetToSlice(ce.heavySinks),
		DonePeers:  donePeers,
		LocalDone:  ce.localDone,
		Sealed:     ce.sealed,
	}, true
}

// Restore rebuilds a client's barrier state from a snapshot after recovery.
func (m *HeavyAccountsMonitor) Restore(clientID uuid.UUID, snap degreeStateSnapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ce := m.getLocked(clientID)
	ce.heavySrcs = accountSetFromSlice(snap.HeavySrcs)
	ce.heavySinks = accountSetFromSlice(snap.HeavySinks)
	ce.donePeers = make(map[int]struct{}, len(snap.DonePeers))
	for _, id := range snap.DonePeers {
		ce.donePeers[id] = struct{}{}
	}
	ce.localDone = snap.LocalDone
	ce.sealed = snap.Sealed
}

// Forget drops a client's exchange state once its cross-product is done.
func (m *HeavyAccountsMonitor) Forget(clientID uuid.UUID) {
	m.mu.Lock()
	delete(m.perClient, clientID)
	m.mu.Unlock()
}
