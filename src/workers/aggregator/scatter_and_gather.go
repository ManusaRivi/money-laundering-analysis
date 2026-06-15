package aggregator

import (
	"fmt"
	"hash/fnv"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/messaging"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"

	// "github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
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

type accountSet map[domain.Account]struct{}

type client struct {
	ID            uuid.UUID
	scatterGroups map[domain.Account]accountSet // key: src_acc, value: set of dest_acc seen in scatter phase
	gatherGroups  map[domain.Account]accountSet // key: dst_acc, value: set of src_acc seen in gather phase
}

type ScatterAndGather struct {
	pub    *messaging.Publisher
	cfg    config.WorkerConfig
	broker broker.Broker

	clients          map[uuid.UUID]*client
	nextWorkerPrefix string
	nextWorkerAmount int
}

func NewScatterAndGather(cfg config.WorkerConfig, b broker.Broker) (*ScatterAndGather, error) {

	slog.Debug("ScatterAndGather created")

	return &ScatterAndGather{
		pub:              messaging.New(codec.New(), b),
		cfg:              cfg,
		broker:           b,
		clients:          make(map[uuid.UUID]*client),
		nextWorkerPrefix: cfg.NextWorkerPrefix,
		nextWorkerAmount: cfg.NextWorkerAmount,
	}, nil
}

func (a *ScatterAndGather) Run() error {
	defer a.broker.StopConsuming()

	return a.broker.StartConsuming(func(msg broker.Message, ack, nack func()) {
		if err := a.handleMessage(msg); err != nil {
			slog.Error("Error handling message", "error", err)
			nack()
			return
		}
		ack()
	})
}

func (a *ScatterAndGather) Stop() {
	a.broker.StopConsuming()
	a.broker.Close()
}

// Private Methods

func (a *ScatterAndGather) getClient(clientID uuid.UUID) *client {
	if c, exists := a.clients[clientID]; exists {
		return c
	}
	c := &client{
		ID:            clientID,
		scatterGroups: make(map[domain.Account]accountSet),
		gatherGroups:  make(map[domain.Account]accountSet),
	}
	a.clients[clientID] = c
	return c
}

func (a *ScatterAndGather) deleteClient(clientID uuid.UUID) {
	delete(a.clients, clientID)
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
	if _, exists := client.scatterGroups[srcAcc]; !exists {
		client.scatterGroups[srcAcc] = make(accountSet)
	}
	client.scatterGroups[srcAcc][dstAcc] = struct{}{}
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
	if _, exists := client.gatherGroups[dstAcc]; !exists {
		client.gatherGroups[dstAcc] = make(accountSet)
	}
	client.gatherGroups[dstAcc][srcAcc] = struct{}{}
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
		if err := a.pub.PublishRaw(broker.KeyType(routingKey), envelope); err != nil {
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

	msgSent := 0
	batch := make(map[domain.TxQ4PairKey]int, maxPairsBuffered)
	flush := func() {
		msgSent += a.sendScatterGatherPhaseTwo(batch, clientID)
		batch = make(map[domain.TxQ4PairKey]int, maxPairsBuffered)
	}

	// MAGIA :sparkles:
	for bridgeAcc, dstAccounts := range scatter {
		if srcAccounts, exists := gather[bridgeAcc]; exists {
			for srcAcc := range srcAccounts {
				for dstAcc := range dstAccounts {
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

func (a *ScatterAndGather) handleEOFMessage(envelope protocol.InternalEnvelope) error {
	clientID := envelope.ClientId
	slog.Debug("Received EOF packet, processing scatter and gather groups", "clientID", clientID)

	msgSent := a.streamScatterGatherPhaseTwo(clientID)

	slog.Debug("Finished sending Scatter-Gather to phase two messages", "clientID", clientID, "messages_sent", msgSent)

	eofCounts := map[broker.KeyType]int{broker.KeyNil: msgSent}
	eofEnvelope, err := a.pub.EncodeEOFCountsEnvelope(clientID, eofCounts)
	if err != nil {
		slog.Error("Error encoding EOF counts envelope", "error", err)
		return err
	}
	slog.Debug("Sending EOF packet after processing scatter and gather", "clientID", clientID, "msg_sent", msgSent)
	if err := a.pub.PublishRaw(broker.KeyControlEOF, eofEnvelope); err != nil {
		slog.Error("Error sending EOF packet", "error", err)
		return err
	}

	a.deleteClient(clientID)
	return nil
}

func (a *ScatterAndGather) handleMessage(msg broker.Message) error {
	return a.pub.Dispatch(msg, map[protocol.MsgType]messaging.Handler{
		protocol.MsgTxQ4:            a.handleTxQ4Message,
		protocol.MsgTransactionsEOF: a.handleEOFMessage,
	})
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
