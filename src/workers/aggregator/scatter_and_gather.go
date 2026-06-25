package aggregator

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/checkpoint"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/messaging"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
	"github.com/ManusaRivi/money-laundering-analysis/src/workers/scattergather"

	"github.com/google/uuid"
)

// Stateful
// Por cada acc almacena y acumula:
// scatter[acc]: {bridge_acc_1, ...}
// gather[acc]:{bridge_acc_3, ...}

// scatter = Map<acc, Set< bridge_acc's >>
// gather = Map<acc, Set< bridge_acc's >>

// en EOF:
//  scatter-gahter = Map<(src_acc,dst_acc), int >>
//  for bridge_acc, destinos in scatter
//    origenes = gather[bridge_acc]
//    for src in origenes
//      for dst in destinos
//        scatter-gather[src, dst] += 1
//  Y envía scatter-gather
//  keys: hash[ (src_acc,dst_acc) ] % next_workers_amount

const maxPairsBuffered = 100_000

// q4DegreesExchange is the fanout the ScatterAndGather replicas use to share heavy
// accounts. The worker declares it itself (raw AMQP), so no topology wiring.
const q4DegreesExchange = "e_q4_degrees"

type accountSet map[domain.Account]struct{}

type client struct {
	ID            uuid.UUID
	scatterGroups map[domain.Account]accountSet // key: src_acc, value: set of dest_acc seen in scatter phase
	gatherGroups  map[domain.Account]accountSet // key: dst_acc, value: set of src_acc seen in gather phase
	heavySources  map[domain.Account]struct{}   // this replica's owned accounts with out-degree >= threshold
	heavySinks    map[domain.Account]struct{}   // this replica's owned accounts with in-degree >= threshold
}

var _ checkpoint.Checkpointable = (*ScatterAndGather)(nil)

// accountAdjacency is the JSON form of one `Account -> set of Accounts` entry.
// encoding/json can't use a struct (domain.Account) as a map key, so the
// adjacency maps are persisted as slices and rebuilt on restore.
type accountAdjacency struct {
	Account   domain.Account   `json:"account"`
	Neighbors []domain.Account `json:"neighbors"`
}

type scatterGatherCheckpoint struct {
	ScatterGroups []accountAdjacency `json:"scatter_groups,omitempty"`
	GatherGroups  []accountAdjacency `json:"gather_groups,omitempty"`
	HeavySources  []domain.Account   `json:"heavy_sources,omitempty"`
	HeavySinks    []domain.Account   `json:"heavy_sinks,omitempty"`
	Sealed        bool               `json:"sealed,omitempty"`
}

type ScatterAndGather struct {
	pub    *messaging.Publisher
	cfg    config.WorkerConfig
	broker broker.Broker
	coord  *checkpoint.Coordinator

	clientsMu sync.Mutex // guards clients (touched by the main loop and the degree goroutine)
	clients   map[uuid.UUID]*client

	nextWorkerPrefix string
	nextWorkerAmount int
	threshold        int
	workerID         int

	exchange        *scattergather.HeavyAccountsExchange
	heavyAccountsCh chan uuid.UUID
	monitor         *HeavyAccountsMonitor

	acksMu      sync.Mutex
	pendingAcks map[uuid.UUID]func() // upstream-EOF acks, fired after the cross-product
}

func NewScatterAndGather(cfg config.WorkerConfig, b broker.Broker, rabbitURL string) (*ScatterAndGather, error) {
	// Shared with the Q4 filter via SCATTER_GATHER_THRESHOLD: the prune drops
	// endpoints whose degree < threshold, which is only sound if it matches the
	// count threshold the filter applies downstream.
	if cfg.Threshold <= 0 {
		return nil, fmt.Errorf("ScatterAndGather requires SCATTER_GATHER_THRESHOLD > 0 (got %d)", cfg.Threshold)
	}

	exchange, err := scattergather.NewHeavyAccountsExchange(rabbitURL, q4DegreesExchange, cfg.WorkerID)
	if err != nil {
		return nil, fmt.Errorf("creating heavy accounts exchange: %w", err)
	}

	slog.Debug("ScatterAndGather created", "threshold", cfg.Threshold, "workerID", cfg.WorkerID, "workerAmount", cfg.WorkerAmount)

	a := &ScatterAndGather{
		pub:              messaging.New(codec.New(), b),
		cfg:              cfg,
		broker:           b,
		clients:          make(map[uuid.UUID]*client),
		nextWorkerPrefix: cfg.NextWorkerPrefix,
		nextWorkerAmount: cfg.NextWorkerAmount,
		threshold:        cfg.Threshold,
		workerID:         cfg.WorkerID,
		exchange:         exchange,
		heavyAccountsCh:  make(chan uuid.UUID, cfg.WorkerAmount),
		pendingAcks:      make(map[uuid.UUID]func()),
	}
	a.monitor = NewHeavyAccountsMonitor(cfg.WorkerID, cfg.WorkerAmount, func(clientID uuid.UUID) { a.heavyAccountsCh <- clientID })
	return a, nil
}

type message struct {
	msg  broker.Message
	ack  func()
	nack func()
}

func (a *ScatterAndGather) Run() error {
	defer func() {
		a.broker.StopConsuming()
		a.exchange.StopConsuming()
	}()

	checkpointManager, err := checkpoint.NewManager(a.cfg.CheckpointDir)
	if err != nil {
		slog.Error("Error creating checkpoint manager", "error", err)
		return err
	}
	a.coord = checkpoint.NewCoordinator(checkpointManager, a.pub, nil, a, a.cfg.CheckpointInterval)
	if err := a.coord.Recover(); err != nil {
		return err
	}

	msgCh := make(chan message)
	errCh := make(chan error, 1)

	// Degree exchange runs on its own goroutine, feeding peer heavy sets / dones
	// into the monitor; barrier completion triggers onClientReady from here.
	go func() {
		if err := a.exchange.StartConsuming(a.monitor.RecordHeavyBatch, a.monitor.RecordDone); err != nil {
			slog.Error("Degree exchange consume loop stopped", "error", err)
		}
	}()

	go func() {
		errCh <- a.broker.StartConsuming(func(msg broker.Message, ack, nack func()) {
			msgCh <- message{msg, ack, nack}
		})
	}()

	for {
		select {
		case msg := <-msgCh:
			a.processMessage(msg.msg, msg.ack, msg.nack)
		case clientID := <-a.heavyAccountsCh:
			a.onClientReady(clientID)
		case err := <-errCh:
			return err
		}
	}
}

func (a *ScatterAndGather) Stop() {
	a.broker.StopConsuming()
	a.broker.Close()
	a.exchange.Close()
}

// Private Methods

func (a *ScatterAndGather) processMessage(msg broker.Message, ack, nack func()) {
	envelope, err := a.pub.DecodeInternalEnvelope(msg.Body)
	if err != nil {
		slog.Error("Error decoding internal envelope", "error", err)
		nack()
		return
	}
	switch envelope.MsgType {
	case protocol.MsgTxQ4:
		if err := a.handleTxQ4Message(envelope); err != nil {
			slog.Error("Error handling TxQ4 message", "error", err)
			nack()
			return
		}
		// Persist-then-ack the adjacency accumulated from this batch.
		a.coord.Track(envelope.ClientId, ack)
	case protocol.MsgTransactionsEOF:
		// Deferred ack: handleEOFMessage starts the degree exchange and stores
		// the ack; onClientReady fires it after the cross-product. Deliberately
		// NOT tracked here — handing it to coord.Track would let the coordinator
		// ack it on the next flush, before the cross-product runs (and would
		// double-own the ack). nack+return only if we couldn't even start
		// (publish failed) — a clean retry via redelivery.
		if err := a.handleEOFMessage(envelope, ack); err != nil {
			slog.Error("Error handling EOF message", "error", err)
			nack()
			return
		}
	default:
		slog.Error("Unexpected inbound packet type", "type", envelope.MsgType)
		nack()
		return
	}
}

// Checkpoint methods

func (a *ScatterAndGather) SnapshotClient(clientID uuid.UUID) ([]byte, error) {
	/* 	if a.monitor.IsSealed(clientID) {
		return nil, nil
	} */
	a.clientsMu.Lock()
	client, ok := a.clients[clientID]
	a.clientsMu.Unlock()
	if !ok {
		return nil, nil
	}
	cp := scatterGatherCheckpoint{
		ScatterGroups: adjacencyToSlice(client.scatterGroups),
		GatherGroups:  adjacencyToSlice(client.gatherGroups),
		HeavySources:  accountSetToSlice(a.monitor.HeavySources(clientID)),
		HeavySinks:    accountSetToSlice(a.monitor.HeavySinks(clientID)),
		Sealed:        a.monitor.IsSealed(clientID),
	}
	return json.Marshal(cp)
}

func (a *ScatterAndGather) RestoreClient(clientID uuid.UUID, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	slog.Debug("Restoring scatter and gather state for client", "clientID", clientID)
	var cp scatterGatherCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return err
	}
	client := a.getClient(clientID)
	client.scatterGroups = adjacencyFromSlice(cp.ScatterGroups)
	client.gatherGroups = adjacencyFromSlice(cp.GatherGroups)
	client.heavySources = accountSetFromSlice(cp.HeavySources)
	client.heavySinks = accountSetFromSlice(cp.HeavySinks)

	if cp.Sealed {
		a.monitor.RestoreSealed(clientID, client.heavySources, client.heavySinks)
	}
	return nil
}

func adjacencyToSlice(groups map[domain.Account]accountSet) []accountAdjacency {
	if len(groups) == 0 {
		return nil
	}
	out := make([]accountAdjacency, 0, len(groups))
	for acc, set := range groups {
		neighbors := make([]domain.Account, 0, len(set))
		for n := range set {
			neighbors = append(neighbors, n)
		}
		out = append(out, accountAdjacency{Account: acc, Neighbors: neighbors})
	}
	return out
}

func adjacencyFromSlice(entries []accountAdjacency) map[domain.Account]accountSet {
	groups := make(map[domain.Account]accountSet, len(entries))
	for _, e := range entries {
		set := make(accountSet, len(e.Neighbors))
		for _, n := range e.Neighbors {
			set[n] = struct{}{}
		}
		groups[e.Account] = set
	}
	return groups
}

func accountSetToSlice(set map[domain.Account]struct{}) []domain.Account {
	if len(set) == 0 {
		return nil
	}
	out := make([]domain.Account, 0, len(set))
	for acc := range set {
		out = append(out, acc)
	}
	return out
}

func accountSetFromSlice(accs []domain.Account) map[domain.Account]struct{} {
	set := make(map[domain.Account]struct{}, len(accs))
	for _, acc := range accs {
		set[acc] = struct{}{}
	}
	return set
}

// stage seeds StageMsgID; includes WorkerID because every replica emits its own
// phase-two pairs and EOF (each owns a disjoint shard of accounts).
func (a *ScatterAndGather) stage() string {
	return fmt.Sprintf("%s#%d", a.cfg.WorkerPrefix, a.workerID)
}

func (a *ScatterAndGather) getClient(clientID uuid.UUID) *client {
	a.clientsMu.Lock()
	defer a.clientsMu.Unlock()
	if c, exists := a.clients[clientID]; exists {
		return c
	}
	c := &client{
		ID:            clientID,
		scatterGroups: make(map[domain.Account]accountSet),
		gatherGroups:  make(map[domain.Account]accountSet),
		heavySources:  make(map[domain.Account]struct{}),
		heavySinks:    make(map[domain.Account]struct{}),
	}
	a.clients[clientID] = c
	return c
}

func (a *ScatterAndGather) deleteClient(clientID uuid.UUID) error {
	a.clientsMu.Lock()
	delete(a.clients, clientID)
	a.clientsMu.Unlock()
	a.monitor.Forget(clientID)
	if err := a.coord.Delete(clientID); err != nil {
		slog.Error("Error deleting client", "clientID", clientID, "error", err)
		return err
	}
	return nil
}

// func (a *ScatterAndGather) handleScatterTx(tx *domain.Transaction, clientID uuid.UUID) error {
func (a *ScatterAndGather) handleScatterTx(tx *protocol.Transaction, clientID uuid.UUID) error {
	slog.Debug("Handling scatter transaction", "clientID", clientID)
	client := a.getClient(clientID)
	srcAcc := domain.Account{
		ID:     tx.FromAccount,
		BankID: tx.FromBank,
	}
	dstAcc := domain.Account{
		ID:     tx.ToAccount,
		BankID: tx.ToBank,
	}
	slog.Debug("Scatter transaction details", "srcId", srcAcc.ID, "dstId", dstAcc.ID)
	set := client.scatterGroups[srcAcc]
	if set == nil {
		set = make(accountSet)
		client.scatterGroups[srcAcc] = set
	}
	set[dstAcc] = struct{}{}
	return nil
}

func (a *ScatterAndGather) handleGatherTx(tx *protocol.Transaction, clientID uuid.UUID) error {
	slog.Debug("Handling gather transaction", "clientID", clientID)
	client := a.getClient(clientID)
	srcAcc := domain.Account{
		ID:     tx.FromAccount,
		BankID: tx.FromBank,
	}
	dstAcc := domain.Account{
		ID:     tx.ToAccount,
		BankID: tx.ToBank,
	}
	slog.Debug("Gather transaction details", "srcId", srcAcc.ID, "dstId", dstAcc.ID)
	set := client.gatherGroups[dstAcc]
	if set == nil {
		set = make(accountSet)
		client.gatherGroups[dstAcc] = set
	}
	set[srcAcc] = struct{}{}
	return nil
}

func (a *ScatterAndGather) handleTxQ4Message(envelope protocol.InternalEnvelope) error {
	clientID := envelope.ClientId
	txType, txs, err := a.pub.DecodeTxQ4PhaseOneBatch(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding TxQ4 data", "error", err)
		return err
	}

	slog.Debug("Received TxQ4 batch", "type", txType, "batchSize", len(txs))

	var handle func(tx *protocol.Transaction, clientID uuid.UUID) error
	switch txType {
	case domain.TxQ4Scatter:
		handle = a.handleScatterTx
	case domain.TxQ4Gather:
		handle = a.handleGatherTx
	default:
		slog.Warn("Received TxQ4 message with unknown type", "type", txType)
		return fmt.Errorf("unknown TxQ4 type: %v", txType)
	}

	for i := range txs {
		if err := handle(&txs[i], clientID); err != nil {
			return err
		}
	}
	return nil
}

// sendScatterGatherPhaseTwo ships one flush worth of pairs to the accumulators,
// one message per pair.
func (a *ScatterAndGather) sendScatterGatherPhaseTwo(scatterGather map[domain.TxQ4PairKey]int, clientId uuid.UUID) int {
	msgSent := 0
	for pk, count := range scatterGather {
		routingKey := a.shardByValue(pk.Src + "::" + pk.Dst)
		envelope, err := a.pub.EncodeTxQ4PhaseTwoBatchEnvelope(clientId, []domain.TxQ4PairCount{{Key: pk, Count: count}})
		if err != nil {
			slog.Error("Error encoding TxQ4 pair for phase two", "error", err, "routing_key", routingKey)
			continue
		}
		id := protocol.StageMsgID(clientId, a.stage(), pk.Key(), 0)
		if err := a.pub.PublishRawWithID(broker.KeyType(routingKey), envelope, id); err != nil {
			slog.Error("Error sending Scatter-Gather pair to phase two", "error", err, "routing_key", routingKey)
			continue
		}
		msgSent++
	}
	return msgSent
}

func (a *ScatterAndGather) streamScatterGatherPhaseTwo(clientID uuid.UUID) int {
	client := a.getClient(clientID)
	scatter := client.scatterGroups
	gather := client.gatherGroups
	heavySrcs := a.monitor.HeavySources(clientID)
	heavySinks := a.monitor.HeavySinks(clientID)

	msgSent := 0
	batch := make(map[domain.TxQ4PairKey]int, maxPairsBuffered)
	flush := func() {
		msgSent += a.sendScatterGatherPhaseTwo(batch, clientID)
		batch = make(map[domain.TxQ4PairKey]int, maxPairsBuffered)
	}

	// Prune against the GLOBAL heavy sets gathered by the degree exchange: a pair
	// can qualify only if its source is a heavy source and its sink a heavy sink.
	for bridgeAcc, dstAccounts := range scatter {
		if srcAccounts, exists := gather[bridgeAcc]; exists {
			for srcAcc := range srcAccounts {
				if _, heavy := heavySrcs[srcAcc]; !heavy {
					continue // not a heavy source: can never be an A1
				}
				for dstAcc := range dstAccounts {
					if _, heavy := heavySinks[dstAcc]; !heavy {
						continue // not a heavy sink: can never be an A2
					}
					pk := domain.TxQ4PairKey{Src: srcAcc.GetID(), Dst: dstAcc.GetID()}
					batch[pk]++
					if len(batch) >= maxPairsBuffered {
						flush()
					}
				}
			}
		}
		delete(scatter, bridgeAcc)
		delete(gather, bridgeAcc)
	}
	if len(batch) > 0 {
		flush()
	}
	return msgSent
}

// FindHeavyAccountsForClient records which of this replica's owned accounts are
// heavy sources (out-degree >= threshold) and heavy sinks (in-degree >= threshold).
// Must run before streamScatterGatherPhaseTwo, which frees the groups as it goes.
func (a *ScatterAndGather) FindHeavyAccountsForClient(clientID uuid.UUID) {
	client := a.getClient(clientID)
	for acc, dsts := range client.scatterGroups {
		if len(dsts) >= a.threshold {
			client.heavySources[acc] = struct{}{}
		}
	}
	for acc, srcs := range client.gatherGroups {
		if len(srcs) >= a.threshold {
			client.heavySinks[acc] = struct{}{}
		}
	}
}

// handleEOFMessage starts the degree exchange for a client: compute this replica's
// heavy sets, publish them to peers, defer the upstream-EOF ack, and merge our own
// sets into the monitor. The cross-product runs later, in onClientReady, when the
// barrier lifts. Returns an error only if publishing fails (a clean retry).
func (a *ScatterAndGather) handleEOFMessage(envelope protocol.InternalEnvelope, ack func()) error {
	clientID := envelope.ClientId
	slog.Debug("Received EOF", "clientID", clientID)
	if a.monitor.IsSealed(clientID) {
		slog.Debug("Client already sealed, ignoring EOF", "clientID", clientID)
		a.storePendingAck(clientID, ack)
		if err := a.onClientReady(clientID); err != nil {
			return err
		}
		return nil
	}

	slog.Debug("Computing local heavy accounts for client", "clientID", clientID)
	a.FindHeavyAccountsForClient(clientID)
	client := a.getClient(clientID)
	slog.Info("Q4 local degree distribution",
		"clientID", clientID,
		"threshold", a.threshold,
		"source_accounts", len(client.scatterGroups),
		"heavy_sources", len(client.heavySources),
		"sink_accounts", len(client.gatherGroups),
		"heavy_sinks", len(client.heavySinks),
	)

	// Publish our heavy sets BEFORE the barrier can complete locally, so no peer
	// waits on a message we never sent.
	if err := a.exchange.PublishHeavy(clientID, client.heavySources, client.heavySinks); err != nil {
		return fmt.Errorf("publishing heavy accounts: %w", err)
	}

	a.storePendingAck(clientID, ack)

	if a.monitor.MergeLocal(clientID, client.heavySources, client.heavySinks) {
		if err := a.onClientReady(clientID); err != nil {
			return err
		}
	}

	return a.coord.Flush()
}

// onClientReady runs once per client when the degree barrier lifts: the global
// heavy sets are final, so run the pruned cross-product, forward the downstream
// EOF, ack the upstream EOF, and release the client's state. Fires on the degree
// goroutine (a peer's done arrived last) or the main loop (this replica finished
// last) — never under a circular wait.
func (a *ScatterAndGather) onClientReady(clientID uuid.UUID) error {
	err := a.coord.SaveClient(clientID)
	if err != nil {
		slog.Error("Error saving client", "clientID", clientID, "error", err)
		return err
	}
	msgSent := a.streamScatterGatherPhaseTwo(clientID)
	slog.Debug("Finished sending Scatter-Gather to phase two", "clientID", clientID, "messages_sent", msgSent)

	eofEnvelope, err := a.pub.EncodeEOFCountsEnvelope(clientID, map[broker.KeyType]int{broker.KeyNil: msgSent})
	if err != nil {
		slog.Error("Error encoding EOF counts envelope", "error", err, "clientID", clientID)
	} else {
		eofID := protocol.StageMsgID(clientID, a.stage(), "eof", 0)
		if err := a.pub.PublishRawWithID(broker.KeyControlEOF, eofEnvelope, eofID); err != nil {
			slog.Error("Error sending downstream EOF", "error", err, "clientID", clientID)
		}
	}

	if ack := a.takePendingAck(clientID); ack != nil {
		ack()
	}
	if err := a.deleteClient(clientID); err != nil {
		slog.Error("Error deleting client", "clientID", clientID, "error", err)
		return err
	}
	return nil
}

func (a *ScatterAndGather) storePendingAck(clientID uuid.UUID, ack func()) {
	a.acksMu.Lock()
	a.pendingAcks[clientID] = ack
	a.acksMu.Unlock()
}

func (a *ScatterAndGather) takePendingAck(clientID uuid.UUID) func() {
	a.acksMu.Lock()
	defer a.acksMu.Unlock()
	ack := a.pendingAcks[clientID]
	delete(a.pendingAcks, clientID)
	return ack
}

func (a *ScatterAndGather) shardByValue(value string) string {
	h := fnv.New32a()
	h.Write([]byte(value))
	index := int(h.Sum32()) % a.nextWorkerAmount
	if index < 0 {
		index += a.nextWorkerAmount
	}
	return fmt.Sprintf("%s_%d", a.nextWorkerPrefix, index)
}
