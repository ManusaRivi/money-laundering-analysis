package aggregator

import (
	"fmt"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/eof"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
	"github.com/google/uuid"
)

type aggOp string

const (
	opMax   aggOp = "max"
	opMin   aggOp = "min"
	opSum   aggOp = "sum"
	opCount aggOp = "count"
)

type Aggregator struct {
	cfg               config.WorkerConfig
	Broker            broker.Broker
	syncEOFController *eof.SyncEOFController

	op          aggOp
	field       string // field used for aggregation comparison (e.g., "Amount")
	groupSource string // "origin" or "dest"
	groupField  string // "BankID" or "ID"

	state map[uuid.UUID]map[string]domain.Transaction
}

func NewAggregator(cfg config.WorkerConfig, b broker.Broker) (*Aggregator, error) {
	op, field, groupSource, groupField, err := parseParams(cfg.Params)
	if err != nil {
		return nil, err
	}

	slog.Debug("Aggregator created",
		"op", op,
		"field", field,
		"group_source", groupSource,
		"group_field", groupField,
	)

	return &Aggregator{
		cfg:               cfg,
		Broker:            b,
		syncEOFController: nil,
		op:                op,
		field:             field,
		groupSource:       groupSource,
		groupField:        groupField,
		state:             make(map[uuid.UUID]map[string]domain.Transaction),
	}, nil
}

func (a *Aggregator) Run() error {
	defer a.Broker.StopConsuming()

	var err error
	a.syncEOFController, err = eof.NewSyncEOFController(
		a.cfg.SyncEOFConfig,
		a.onflush,
		a.onLeaderFlush,
		a.onRetryExceeded,
	)
	if err != nil {
		slog.Error("Error creating SyncEOFController", "error", err)
		return err
	}

	go a.syncEOFController.Start()

	return a.Broker.StartConsuming(func(msg broker.Message, ack, nack func()) {
		if err := a.handleMessage(msg); err != nil {
			slog.Error("Error handling message", "error", err)
			nack()
			return
		}
		ack()
	})
}

func (a *Aggregator) Stop() {}

// Private Methods

func (a *Aggregator) onflush(clientID uuid.UUID) error {
	slog.Info("Flush triggered in Aggregator", "client_id", clientID)

	groups := a.state[clientID]
	for key, tx := range groups {
		msg, err := inner.MarshalTransactionPacket(clientID, broker.KeyNil, tx)
		if err != nil {
			slog.Error("Error marshalling aggregated transaction", "error", err, "group_key", key)
			return err
		}
		if err := a.Broker.Send(*msg); err != nil {
			slog.Error("Error sending aggregated transaction", "error", err, "group_key", key)
			return err
		}
		a.syncEOFController.MessageSent(clientID)
	}

	delete(a.state, clientID)
	return nil
}

func (a *Aggregator) onLeaderFlush(clientID uuid.UUID, finalSent int) error {
	slog.Info("Leader flush triggered in Aggregator", "client_id", clientID, "final_sent", finalSent)

	eofMsg, err := inner.MarshalEOFPacket(clientID, domain.EOFCounts{
		Counts: map[broker.KeyType]int{broker.KeyNil: finalSent},
	})
	if err != nil {
		slog.Error("Error marshalling EOF packet", "error", err)
		return err
	}
	if err := a.Broker.Send(*eofMsg); err != nil {
		slog.Error("Error sending EOF packet", "error", err)
		return err
	}
	return nil
}

func (a *Aggregator) onRetryExceeded(clientID uuid.UUID) error {
	slog.Warn("Retry limit exceeded in Aggregator", "client_id", clientID)
	delete(a.state, clientID)
	return nil
}

func (a *Aggregator) handleTransactionMessage(pkt inner.Packet) error {
	var tx domain.Transaction
	if err := pkt.UnmarshalData(&tx); err != nil {
		slog.Error("Error unmarshalling transaction data", "error", err)
		return err
	}

	key, err := a.extractGroupKey(tx)
	if err != nil {
		slog.Error("Error extracting group key", "error", err)
		a.syncEOFController.MessageReceived(pkt.ClientID)
		return err
	}

	if _, exists := a.state[pkt.ClientID]; !exists {
		a.state[pkt.ClientID] = make(map[string]domain.Transaction)
	}
	current, exists := a.state[pkt.ClientID][key]
	a.state[pkt.ClientID][key] = a.combine(current, tx, exists)

	a.syncEOFController.MessageReceived(pkt.ClientID)
	return nil
}

func (a *Aggregator) handleEOFMessage(pkt inner.Packet) error {
	var counts domain.EOFCounts
	if err := pkt.UnmarshalData(&counts); err != nil {
		slog.Error("Error unmarshalling EOF counts", "error", err)
		return err
	}
	expected := 0
	for _, c := range counts.Counts {
		expected += c
	}
	a.syncEOFController.SyncEof(pkt.ClientID, expected)
	return nil
}

func (a *Aggregator) handleMessage(msg broker.Message) error {
	pkt, err := inner.UnmarshalPacket(msg)
	if err != nil {
		slog.Error("Error unmarshalling message", "error", err)
		return err
	}

	switch pkt.Type {
	case inner.TypeTransaction:
		return a.handleTransactionMessage(*pkt)
	case inner.TypeEOF:
		return a.handleEOFMessage(*pkt)
	default:
		slog.Warn("Received message with unknown type", "type", pkt.Type)
		return fmt.Errorf("unknown packet type: %v", pkt.Type)
	}
}

// combine merges the incoming transaction into the stored aggregate for its group.
// If no prior entry exists, the incoming tx becomes the seed.
func (a *Aggregator) combine(current, incoming domain.Transaction, hasCurrent bool) domain.Transaction {
	if !hasCurrent {
		switch a.op {
		case opCount:
			seed := incoming
			if seed.Paid == nil {
				seed.Paid = &domain.Money{}
			}
			seed.Paid.Amount = 1
			return seed
		case opSum:
			seed := incoming
			if seed.Paid == nil {
				seed.Paid = &domain.Money{}
			}
			return seed
		default:
			return incoming
		}
	}

	switch a.op {
	case opMax:
		if a.fieldValue(incoming) > a.fieldValue(current) {
			return incoming
		}
		return current
	case opMin:
		if a.fieldValue(incoming) < a.fieldValue(current) {
			return incoming
		}
		return current
	case opSum:
		current.Paid.Amount += a.fieldValue(incoming)
		return current
	case opCount:
		current.Paid.Amount++
		return current
	default:
		return current
	}
}

func (a *Aggregator) fieldValue(tx domain.Transaction) float64 {
	switch a.field {
	case "Amount":
		if tx.Paid == nil {
			return 0
		}
		return tx.Paid.Amount
	default:
		return 0
	}
}

func (a *Aggregator) extractGroupKey(tx domain.Transaction) (string, error) {
	var acct *domain.Account
	switch a.groupSource {
	case "origin":
		acct = tx.Origin
	case "dest":
		acct = tx.Dest
	default:
		return "", fmt.Errorf("Invalid group source: %q", a.groupSource)
	}
	if acct == nil {
		return "", fmt.Errorf("Transaction missing %q account", a.groupSource)
	}
	switch a.groupField {
	case "BankID":
		if acct.BankID == "" {
			return "", fmt.Errorf("Account missing BankID")
		}
		return acct.BankID, nil
	case "ID":
		if acct.ID == "" {
			return "", fmt.Errorf("Account missing ID")
		}
		return acct.ID, nil
	default:
		return "", fmt.Errorf("Invalid group field: %q", a.groupField)
	}
}

func parseParams(params map[string]any) (aggOp, string, string, string, error) {
	rawType, ok := params["type"].(string)
	if !ok {
		return "", "", "", "", fmt.Errorf("aggregator params: missing or invalid 'type'")
	}
	op := aggOp(rawType)
	switch op {
	case opMax, opMin, opSum, opCount:
	default:
		return "", "", "", "", fmt.Errorf("aggregator params: unsupported type %q", rawType)
	}

	field, _ := params["field"].(string)
	if field == "" && op != opCount {
		return "", "", "", "", fmt.Errorf("aggregator params: 'field' is required for op %q", op)
	}

	groupRaw, ok := params["group"].(map[string]any)
	if !ok || len(groupRaw) == 0 {
		return "", "", "", "", fmt.Errorf("aggregator params: missing or invalid 'group'")
	}
	if len(groupRaw) > 1 {
		return "", "", "", "", fmt.Errorf("aggregator params: 'group' supports a single source")
	}
	var groupSource, groupField string
	for k, v := range groupRaw {
		groupSource = k
		s, ok := v.(string)
		if !ok {
			return "", "", "", "", fmt.Errorf("aggregator params: group value must be a string")
		}
		groupField = s
	}

	return op, field, groupSource, groupField, nil
}
