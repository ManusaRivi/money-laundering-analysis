package aggregator

import (
	"fmt"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
	"github.com/google/uuid"
)

type aggOp string

const (
	opMax   aggOp = "max"
	opMin   aggOp = "min"
	opSum   aggOp = "sum"
	opCount aggOp = "count"
	opAvg   aggOp = "avg"
)

type avgState struct {
	sum    float64
	count  int
	sample domain.Transaction
}

type Aggregator struct {
	cfg    config.WorkerConfig
	Broker broker.Broker

	op          aggOp
	field       string // field used for aggregation comparison (e.g., "Amount")
	groupSource string // "origin" or "dest"
	groupField  string // "BankID" or "ID"

	state    map[uuid.UUID]map[string]domain.Transaction
	avgState map[uuid.UUID]map[string]avgState
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
		cfg:         cfg,
		Broker:      b,
		op:          op,
		field:       field,
		groupSource: groupSource,
		groupField:  groupField,
		state:       make(map[uuid.UUID]map[string]domain.Transaction),
		avgState:    make(map[uuid.UUID]map[string]avgState),
	}, nil
}

func (a *Aggregator) Run() error {
	defer a.Broker.StopConsuming()

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

func (a *Aggregator) handleTransactionMessage(pkt inner.Packet) error {
	var tx domain.Transaction
	if err := pkt.UnmarshalData(&tx); err != nil {
		slog.Error("Error unmarshalling transaction data", "error", err)
		return err
	}

	key, err := a.extractGroupKey(tx)
	if err != nil {
		slog.Error("Error extracting group key", "error", err)
		return err
	}
	if a.op == opAvg {
		if _, exists := a.avgState[pkt.ClientID]; !exists {
			a.avgState[pkt.ClientID] = make(map[string]avgState)
		}
		current := a.avgState[pkt.ClientID][key]
		amount := a.fieldValue(tx)
		current.sum += amount
		current.count++
		if current.count == 1 {
			current.sample = tx
		}
		a.avgState[pkt.ClientID][key] = current
		return nil
	}

	if _, exists := a.state[pkt.ClientID]; !exists {
		a.state[pkt.ClientID] = make(map[string]domain.Transaction)
	}
	current, exists := a.state[pkt.ClientID][key]
	a.state[pkt.ClientID][key] = a.combine(current, tx, exists)

	return nil
}

func (a *Aggregator) handleEOFMessage(pkt inner.Packet) error {
	slog.Debug("Received EOF packet, processing aggregation results", "clientID", pkt.ClientID)
	msgSent := 0
	if a.op == opAvg {
		groups := a.avgState[pkt.ClientID]
		for key, st := range groups {
			avg := 0.0
			if st.count > 0 {
				avg = st.sum / float64(st.count)
			}
			out := st.sample
			if out.Paid == nil {
				out.Paid = &domain.Money{}
			}
			out.Paid.Amount = avg
			out.Format = key
			msg, err := inner.MarshalTransactionPacket(pkt.ClientID, broker.KeyNil, out)
			if err != nil {
				slog.Error("Error marshalling aggregated transaction", "error", err, "group_key", key)
				return err
			}
			slog.Debug("Sending aggregated transaction", "clientID", pkt.ClientID, "group_key", key)
			if err := a.Broker.Send(*msg); err != nil {
				slog.Error("Error sending aggregated transaction", "error", err, "group_key", key)
				return err
			}
			msgSent++
		}
		delete(a.avgState, pkt.ClientID)
	} else {
		groups := a.state[pkt.ClientID]
		for key, tx := range groups {
			msg, err := inner.MarshalTransactionPacket(pkt.ClientID, broker.KeyNil, tx)
			if err != nil {
				slog.Error("Error marshalling aggregated transaction", "error", err, "group_key", key)
				return err
			}
			slog.Debug("Sending aggregated transaction", "clientID", pkt.ClientID, "group_key", key)
			if err := a.Broker.Send(*msg); err != nil {
				slog.Error("Error sending aggregated transaction", "error", err, "group_key", key)
				return err
			}
			msgSent++
		}
		delete(a.state, pkt.ClientID)
	}

	eofMsg, err := inner.MarshalEOFPacket(pkt.ClientID, domain.EOFCounts{
		Counts: map[broker.KeyType]int{broker.KeyNil: msgSent},
	})
	if err != nil {
		slog.Error("Error marshalling EOF packet", "error", err)
		return err
	}
	slog.Debug("Sending EOF packet after processing aggregation results", "clientID", pkt.ClientID, "msg_sent", msgSent)
	if err := a.Broker.Send(*eofMsg); err != nil {
		slog.Error("Error sending EOF packet", "error", err)
		return err
	}
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
	case "format":
		if tx.Format == "" {
			return "", fmt.Errorf("Transaction missing format")
		}
		return tx.Format, nil
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
	case opMax, opMin, opSum, opCount, opAvg:
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
