package aggregator

import (
	"sync"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/google/uuid"
)

// HeavyAccountsMonitor coordinates the per-client degree exchange among the
// aggregator replicas. Each replica folds in its own heavy sets (MergeLocal),
// publishes them to the fanout, and receives peers' sets via RecordHeavyBatch /
// RecordDone on the degree-broker goroutine. When a client has heard "done" from
// all N-1 peers AND merged its own, the barrier lifts exactly once and onReady
// runs the pruned cross-product.
//
// onReady fires on whichever goroutine completes the barrier: usually the degree
// goroutine (a peer's done arrives last), but the main loop if this replica is
// the last to reach EOF (peers already done). Either way there is no circular
// wait, so no deadlock — it is at worst one cross-product run inline.
type HeavyAccountsMonitor struct {
	mu         sync.Mutex
	perClient  map[uuid.UUID]*clientExchange
	selfID     int
	peerTarget int // WORKER_AMOUNT - 1
	onReady    func(clientID uuid.UUID)
}

type clientExchange struct {
	heavySrcs  map[domain.Account]struct{}
	heavySinks map[domain.Account]struct{}
	donePeers  map[int]struct{} // sender ids seen "done" (set => dedups redelivery)
	localDone  bool
	sealed     bool
}

func NewHeavyAccountsMonitor(selfID, workerAmount int, onReady func(uuid.UUID)) *HeavyAccountsMonitor {
	peerTarget := workerAmount - 1
	if peerTarget < 0 {
		peerTarget = 0
	}
	return &HeavyAccountsMonitor{
		perClient:  make(map[uuid.UUID]*clientExchange),
		selfID:     selfID,
		peerTarget: peerTarget,
		onReady:    onReady,
	}
}

// getLocked returns (creating if needed) the exchange state for a client.
func (m *HeavyAccountsMonitor) getLocked(clientID uuid.UUID) *clientExchange {
	ce, ok := m.perClient[clientID]
	if !ok {
		ce = &clientExchange{
			heavySrcs:  make(map[domain.Account]struct{}),
			heavySinks: make(map[domain.Account]struct{}),
			donePeers:  make(map[int]struct{}),
		}
		m.perClient[clientID] = ce
	}
	return ce
}

// MergeLocal folds this replica's own heavy sets in and marks it locally done.
// Called from the main loop at EOF, before publishing to peers. May complete the
// barrier (e.g. N==1, or this replica finished last).
func (m *HeavyAccountsMonitor) MergeLocal(clientID uuid.UUID, srcs, sinks map[domain.Account]struct{}) {
	m.mu.Lock()
	ce := m.getLocked(clientID)
	ready := false
	if !ce.sealed {
		for a := range srcs {
			ce.heavySrcs[a] = struct{}{}
		}
		for a := range sinks {
			ce.heavySinks[a] = struct{}{}
		}
		ce.localDone = true
		ready = m.maybeSealLocked(ce)
	}
	m.mu.Unlock()
	if ready {
		m.onReady(clientID)
	}
}

// RecordHeavyBatch merges a peer's heavy accounts for one role. Own echoes from
// the fanout are ignored. AMQP preserves per-publisher order, so a peer's batches
// always arrive before its done.
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

// RecordDone marks a peer finished. Completing the barrier triggers onReady once.
func (m *HeavyAccountsMonitor) RecordDone(clientID uuid.UUID, senderID int) {
	if senderID == m.selfID {
		return
	}
	m.mu.Lock()
	ce := m.getLocked(clientID)
	ready := false
	if !ce.sealed {
		ce.donePeers[senderID] = struct{}{}
		ready = m.maybeSealLocked(ce)
	}
	m.mu.Unlock()
	if ready {
		m.onReady(clientID)
	}
}

// maybeSealLocked seals and returns true exactly on the transition that lifts the
// barrier (own merged + all N-1 peers done). Caller holds m.mu.
func (m *HeavyAccountsMonitor) maybeSealLocked(ce *clientExchange) bool {
	if ce.sealed || !ce.localDone || len(ce.donePeers) < m.peerTarget {
		return false
	}
	ce.sealed = true
	return true
}

// HeavySources / HeavySinks expose the global sets. Only call after this client's
// onReady has fired: the client is then sealed, so there are no concurrent
// writers and the returned map can be iterated without the lock.
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

// Forget drops a client's exchange state once its cross-product is done.
func (m *HeavyAccountsMonitor) Forget(clientID uuid.UUID) {
	m.mu.Lock()
	delete(m.perClient, clientID)
	m.mu.Unlock()
}
