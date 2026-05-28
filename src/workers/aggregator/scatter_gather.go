package aggregator

import (
	"fmt"
	"hash/fnv"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
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

type pairKey struct {
	src, dst string
}

type accountSet map[domain.Account]struct{}

type client struct {
	ID uuid.UUID
	scatterGroups map[domain.Account]accountSet // key: src_acc, value: set of dest_acc seen in scatter phase
	gatherGroups  map[domain.Account]accountSet // key: dst_acc, value: set of src_acc seen in gather phase
}


type ScatterAndGather struct {
	cfg    config.WorkerConfig
	broker broker.Broker

	clients map[uuid.UUID]*client
	nextWorkerAmount int
}

func NewScatterAndGather(cfg config.WorkerConfig, b broker.Broker) (*ScatterAndGather, error) {

	slog.Debug("ScatterAndGather created")

	return &ScatterAndGather{
		cfg:         cfg,
		broker:      b,
		clients:      make(map[uuid.UUID]*client),
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
		ID: clientID,
		scatterGroups: make(map[domain.Account]accountSet),
		gatherGroups: make(map[domain.Account]accountSet),
	}
	a.clients[clientID] = c
	return c
}

func (a *ScatterAndGather) deleteClient(clientID uuid.UUID) {
	delete(a.clients, clientID)
}

func (a *ScatterAndGather) handleScatterTx(tx *domain.Transaction, clientID uuid.UUID) error {
	slog.Debug("Handling scatter transaction", "clientID", clientID)
	client := a.getClient(clientID)
	srcAcc := *tx.Origin
	dstAcc := *tx.Dest
	slog.Debug("Scatter transaction details", "srcId", srcAcc.ID, "dstId", dstAcc.ID)
	if _, exists := client.scatterGroups[srcAcc]; !exists {
		client.scatterGroups[srcAcc] = make(accountSet)
	}
	client.scatterGroups[srcAcc][dstAcc] = struct{}{}
	return nil
}

func (a *ScatterAndGather) handleGatherTx(tx *domain.Transaction, clientID uuid.UUID) error {
	slog.Debug("Handling gather transaction", "clientID", clientID)
	client := a.getClient(clientID)
	srcAcc := *tx.Origin
	dstAcc := *tx.Dest
	slog.Debug("Gather transaction details", "srcId", srcAcc.ID, "dstId", dstAcc.ID)
	if _, exists := client.gatherGroups[dstAcc]; !exists {
		client.gatherGroups[dstAcc] = make(accountSet)
	}
	client.gatherGroups[dstAcc][srcAcc] = struct{}{}
	return nil
}

func (a *ScatterAndGather) handleTxQ4Message(pkt inner.Packet) error {
	var txQ4 domain.TxQ4
	if err := pkt.UnmarshalData(&txQ4); err != nil {
		slog.Error("Error unmarshalling TxQ4 data", "error", err)
		return err
	}

	slog.Debug("Received TxQ4 message", "type", txQ4.Type)

	switch txQ4.Type {
	case domain.TxQ4Scatter:
		return a.handleScatterTx(txQ4.Transaction, pkt.ClientID)
	case domain.TxQ4Gather:
		return a.handleGatherTx(txQ4.Transaction, pkt.ClientID)
	default:
		slog.Warn("Received TxQ4 message with unknown type", "type", txQ4.Type)
		return fmt.Errorf("unknown TxQ4 type: %v", txQ4.Type)
	}
}

// en EOF:
//  scatter-gahter = Map<(src_acc,dst_acc), int >>
//  for bridge_acc, destinos in scatter
//    origenes = gather[bridge_acc]
//    for src in origenes
//      for dst in destinos
//        scatter-gather[src, dst] += 1
//  Y envía scatter-gather
//  keys: hash[ (src_acc,dst_acc) ] % num_workers_next_stage
func (a *ScatterAndGather) handleEOFMessage(pkt inner.Packet) error {
	slog.Debug("Received EOF packet, processing scatter and gather groups", "clientID", pkt.ClientID)
	client := a.getClient(pkt.ClientID)
	scatter := client.scatterGroups
	gather := client.gatherGroups

	estimatedPairs := 0
	for bridgeAcc, dstAccounts := range scatter {
		if srcAccounts, ok := gather[bridgeAcc]; ok {
			estimatedPairs += len(srcAccounts) * len(dstAccounts)
		}
	}

	scatterGather := make(map[pairKey]int, estimatedPairs)
	globalAccountSet := make(map[domain.Account]struct{}, estimatedPairs)

	for bridgeAcc, dstAccounts := range scatter {
		srcAccounts, exists := gather[bridgeAcc]
		if !exists {
			continue
		}
		for srcAcc := range srcAccounts {
			globalAccountSet[srcAcc] = struct{}{}
			for dstAcc := range dstAccounts {
				globalAccountSet[dstAcc] = struct{}{}
				scatterGather[pairKey{src: srcAcc.GetID(), dst: dstAcc.GetID()}]++
			}
		}
	}
	slog.Debug("Completed processing scatter and gather groups", "clientID", pkt.ClientID, "scatter_gather_pairs", len(scatterGather), "unique_accounts", len(globalAccountSet))

	accountList := make([]domain.Account, 0, len(globalAccountSet))
	for acc := range globalAccountSet {
		accountList = append(accountList, acc)
	}

	msgSent := 0
	for pk, count := range scatterGather {
		srcDestKey := fmt.Sprintf("%s-%s", pk.src, pk.dst)
		txQ4Phase2 := domain.TxQ4Phase2{
			ScatterGather: map[string]int{srcDestKey: count},
			Accounts: accountList,
		}

		routingKey := a.shardByValue(srcDestKey)

		msg, err := inner.MarshalTxQ4Phase2Packet(pkt.ClientID, broker.KeyType(routingKey), txQ4Phase2)
		if err != nil {
			slog.Error("Error marshalling Scatter-Gather phase 2", "error", err, "routing_key", routingKey)
			return err
		}
		slog.Debug("Sending Scatter-Gather phase 2", "clientID", pkt.ClientID, "routing_key", routingKey)
		if err := a.broker.Send(*msg); err != nil {
			slog.Error("Error sending Scatter-Gather phase 2", "error", err, "routing_key", routingKey)
			return err
		}
		msgSent++
	}
	slog.Debug("Finished sending Scatter-Gather phase 2 messages", "clientID", pkt.ClientID, "messages_sent", msgSent)
	
	// hacer eof packet especial para poder enviar una sola vez:
	// globalAccountSet
	
	eofCounts := domain.EOFCounts{
		Counts: map[broker.KeyType]int{broker.KeyNil: msgSent},
	}
	eofMsg, err := inner.MarshalEOFPacket(pkt.ClientID, eofCounts)
	if err != nil {
		slog.Error("Error marshalling EOF packet", "error", err)
		return err
	}
	slog.Debug("Sending EOF packet after processing scatter and gather", "clientID", pkt.ClientID, "msg_sent", msgSent)
	if err := a.broker.Send(*eofMsg); err != nil {
		slog.Error("Error sending EOF packet", "error", err)
		return err
	}

	a.deleteClient(pkt.ClientID)
	return nil
}

func (a *ScatterAndGather) handleMessage(msg broker.Message) error {
	pkt, err := inner.UnmarshalPacket(msg)
	if err != nil {
		slog.Error("Error unmarshalling message", "error", err)
		return err
	}

	switch pkt.Type {
	case inner.TypeTxQ4:
		return a.handleTxQ4Message(*pkt)
	case inner.TypeEOF:
		return a.handleEOFMessage(*pkt)
	default:
		slog.Warn("Received message with unknown type", "type", pkt.Type)
		return fmt.Errorf("unknown packet type: %v", pkt.Type)
	}
}

// func (a *ScatterAndGather) splitSrcDestKey(key string) (string, string) {
// 	// key format is "srcAccID-dstAccID"
// 	var src, dst string
// 	fmt.Sscanf(key, "%[^-]-%s", &src, &dst)
// 	return src, dst
// }

func (a *ScatterAndGather) shardByValue(value string) string {
	h := fnv.New32a()
	h.Write([]byte(value))
	index := int(h.Sum32()) % a.nextWorkerAmount
	if index < 0 {
		index += a.nextWorkerAmount
	}
	return fmt.Sprintf("%s_%d", a.cfg.NextWorkerPrefix, index)
}
