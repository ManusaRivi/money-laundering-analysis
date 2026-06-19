package aggregator

import (
	"log/slog"
	"strings"

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
// Va almacenando y acumulando en memoria los datos:
//  accumulator = Map<(src_acc,dst_acc), int >>
//  for key in scatter-gather:
//    accumulator[key] += scatter-gather[key]

// Al recibir EOF de etapa anterior envian accumulator

const BATCH_SIZE = 1000

type clientScattergather struct {
	ID          uuid.UUID
	accumulator map[domain.TxQ4PairKey]int
	intern      map[string]string
}

// internStr returns a single canonical copy of s, so repeated account IDs across
// many pair keys share their backing bytes instead of each holding a copy.
func (c *clientScattergather) internStr(s string) string {
	if canon, ok := c.intern[s]; ok {
		return canon
	}
	c.intern[s] = s
	return s
}

// accountFromID reverses Account.GetID ("BankID-ID"), splitting on the first
// '-'. Bank IDs in the dataset contain no '-', so this round-trips exactly.
func accountFromID(id string) domain.Account {
	bankID, accID, _ := strings.Cut(id, "-")
	return domain.Account{BankID: bankID, ID: accID}
}

type ScatterGather struct {
	pub    *messaging.Publisher
	cfg    config.WorkerConfig
	broker broker.Broker

	clients          map[uuid.UUID]*clientScattergather
	nextWorkerAmount int
	prevWorkerAmount int
	eofCounters      map[uuid.UUID]int
}

func NewScatterGather(cfg config.WorkerConfig, b broker.Broker) (*ScatterGather, error) {
	slog.Debug("ScatterGather created", "prevWorkerAmount", cfg.PrevWorkerAmount)

	return &ScatterGather{
		pub:              messaging.New(codec.New(), b),
		cfg:              cfg,
		broker:           b,
		clients:          make(map[uuid.UUID]*clientScattergather),
		nextWorkerAmount: cfg.NextWorkerAmount,
		prevWorkerAmount: cfg.PrevWorkerAmount,
		eofCounters:      make(map[uuid.UUID]int),
	}, nil
}

func (a *ScatterGather) Run() error {
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

func (a *ScatterGather) Stop() {
	a.broker.StopConsuming()
	a.broker.Close()
}

// Private Methods

func (a *ScatterGather) getClient(clientID uuid.UUID) *clientScattergather {
	if c, exists := a.clients[clientID]; exists {
		return c
	}
	c := &clientScattergather{
		ID:          clientID,
		accumulator: make(map[domain.TxQ4PairKey]int),
		intern:      make(map[string]string),
	}
	a.clients[clientID] = c
	return c
}

func (a *ScatterGather) deleteClient(clientID uuid.UUID) {
	delete(a.clients, clientID)
}

func (a *ScatterGather) handleTxQ4Message(envelope protocol.InternalEnvelope) error {
	pairs, err := a.pub.DecodeTxQ4PhaseTwoBatch(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding TxQ4 batch", "error", err)
		return err
	}
	slog.Debug("Received TxQ4 batch", "clientID", envelope.ClientId, "batchSize", len(pairs))

	client := a.getClient(envelope.ClientId)
	for _, pc := range pairs {
		key := domain.TxQ4PairKey{
			Src: client.internStr(pc.Key.Src),
			Dst: client.internStr(pc.Key.Dst),
		}
		client.accumulator[key] += pc.Count
	}

	return nil
}

func (a *ScatterGather) handleEOFMessage(envelope protocol.InternalEnvelope) error {
	clientID := envelope.ClientId
	a.eofCounters[clientID]++
	slog.Debug("Received EOF packet", "clientID", clientID, "counter", a.eofCounters[clientID], "target", a.prevWorkerAmount)

	if a.eofCounters[clientID] < a.prevWorkerAmount {
		return nil
	}

	scatterGather := a.getClient(clientID).accumulator
	msgSent := 0
	if len(scatterGather) > 0 {
		slog.Debug("Sending Scatter-Gather to phase three", "clientID", clientID, "scatter_gather_count", len(scatterGather))

		batch := make(map[string]*domain.TxQ4PairEntry, BATCH_SIZE)
		for pk, count := range scatterGather {
			batch[pk.Key()] = &domain.TxQ4PairEntry{
				Count:      count,
				SrcAccount: accountFromID(pk.Src),
				DstAccount: accountFromID(pk.Dst),
			}
			if len(batch) >= BATCH_SIZE {
				txQ4PhaseThree := domain.TxQ4PhaseThree{ScatterGather: batch}
				envelope, err := a.pub.EncodeTxQ4PhaseThreeEnvelope(clientID, txQ4PhaseThree)
				if err != nil {
					slog.Error("Error encoding Scatter-Gather batch", "error", err)
					return err
				}
				if err := a.pub.PublishRaw(broker.KeyNil, envelope); err != nil {
					slog.Error("Error sending Scatter-Gather batch", "error", err)
					return err
				}
				msgSent++
				batch = make(map[string]*domain.TxQ4PairEntry, BATCH_SIZE)
			}
		}
		if len(batch) > 0 {
			txQ4PhaseThree := domain.TxQ4PhaseThree{ScatterGather: batch}
			envelope, err := a.pub.EncodeTxQ4PhaseThreeEnvelope(clientID, txQ4PhaseThree)
			if err != nil {
				slog.Error("Error encoding Scatter-Gather final batch", "error", err)
				return err
			}
			if err := a.pub.PublishRaw(broker.KeyNil, envelope); err != nil {
				slog.Error("Error sending Scatter-Gather final batch", "error", err)
				return err
			}
			msgSent++
		}
	}

	slog.Debug("All upstream EOFs received, forwarding EOF downstream", "clientID", clientID, "msg_sent", msgSent)
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
	delete(a.eofCounters, clientID)
	return nil
}

func (a *ScatterGather) handleMessage(msg broker.Message) error {
	return a.pub.Dispatch(msg, map[protocol.MsgType]messaging.Handler{
		protocol.MsgTxQ4:            a.handleTxQ4Message,
		protocol.MsgTransactionsEOF: a.handleEOFMessage,
	})
}
