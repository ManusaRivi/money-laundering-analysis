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
)

type Aggregator struct {
	cfg    config.WorkerConfig
	Broker broker.Broker

	op          aggOp
	field       string // field used for aggregation comparison (e.g., "Amount")
	grouped     bool   // false => single-bucket aggregation across all received transactions
	groupSource string // "origin" or "dest" (only meaningful when grouped)
	groupField  string // "BankID" or "ID"  (only meaningful when grouped)

	// state for grouped aggregations: clientID -> groupKey -> running aggregate
	state map[uuid.UUID]map[string]domain.Transaction

	// countState is the running counter for the ungrouped count aggregation.
	// Indexed by clientID so concurrent clients each accumulate independently.
	countState map[uuid.UUID]int
}

func NewAggregator(cfg config.WorkerConfig, b broker.Broker) (*Aggregator, error) {
	op, field, grouped, groupSource, groupField, err := parseParams(cfg.Params)
	if err != nil {
		return nil, err
	}

	slog.Debug("Aggregator created",
		"op", op,
		"field", field,
		"grouped", grouped,
		"group_source", groupSource,
		"group_field", groupField,
		"query", cfg.Query,
	)

	return &Aggregator{
		cfg:         cfg,
		Broker:      b,
		op:          op,
		field:       field,
		grouped:     grouped,
		groupSource: groupSource,
		groupField:  groupField,
		state:       make(map[uuid.UUID]map[string]domain.Transaction),
		countState:  make(map[uuid.UUID]int),
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
	if !a.grouped {
		// Ungrouped path. Only "count" needs no payload inspection; for any
		// other ungrouped op we'd still need to unmarshal the transaction.
		if a.op == opCount {
			a.countState[pkt.ClientID]++
			return nil
		}
	}

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

	if _, exists := a.state[pkt.ClientID]; !exists {
		a.state[pkt.ClientID] = make(map[string]domain.Transaction)
	}
	current, exists := a.state[pkt.ClientID][key]
	a.state[pkt.ClientID][key] = a.combine(current, tx, exists)

	return nil
}

func (a *Aggregator) handleEOFMessage(pkt inner.Packet) error {
	slog.Debug("Received EOF packet, processing aggregation results", "clientID", pkt.ClientID)

	if !a.grouped && a.op == opCount {
		return a.emitUngroupedCount(pkt.ClientID)
	}

	groups := a.state[pkt.ClientID]
	msgSent := 0
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

// emitUngroupedCount emits the running count for clientID as a query-result
// packet plus the matching query-EOF packet, then clears the per-client
// counter. Only query 5 is wired today; extend the switch as other queries
// adopt ungrouped count.
func (a *Aggregator) emitUngroupedCount(clientID uuid.UUID) error {
	count := a.countState[clientID]
	delete(a.countState, clientID)

	resultMsg, err := a.marshalCountResult(clientID, count)
	if err != nil {
		slog.Error("Error marshalling count result", "error", err, "query", a.cfg.Query)
		return err
	}
	if err := a.Broker.Send(*resultMsg); err != nil {
		slog.Error("Error sending count result", "error", err)
		return err
	}
	slog.Debug("Sent count result", "clientID", clientID, "count", count, "query", a.cfg.Query)

	eofMsg, err := a.marshalCountEOF(clientID)
	if err != nil {
		slog.Error("Error marshalling count EOF", "error", err, "query", a.cfg.Query)
		return err
	}
	if err := a.Broker.Send(*eofMsg); err != nil {
		slog.Error("Error sending count EOF", "error", err)
		return err
	}
	return nil
}

func (a *Aggregator) marshalCountResult(clientID uuid.UUID, count int) (*broker.Message, error) {
	switch a.cfg.Query {
	case 5:
		return inner.MarshalQuery5ResultPacket(clientID, domain.Query5Result{Count: count})
	default:
		return nil, fmt.Errorf("ungrouped count aggregation not wired for query %d", a.cfg.Query)
	}
}

func (a *Aggregator) marshalCountEOF(clientID uuid.UUID) (*broker.Message, error) {
	switch a.cfg.Query {
	case 5:
		return inner.MarshalQuery5EOFPacket(clientID)
	default:
		return nil, fmt.Errorf("ungrouped count EOF not wired for query %d", a.cfg.Query)
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

func parseParams(params map[string]any) (aggOp, string, bool, string, string, error) {
	rawType, ok := params["type"].(string)
	if !ok {
		return "", "", false, "", "", fmt.Errorf("aggregator params: missing or invalid 'type'")
	}
	op := aggOp(rawType)
	switch op {
	case opMax, opMin, opSum, opCount:
	default:
		return "", "", false, "", "", fmt.Errorf("aggregator params: unsupported type %q", rawType)
	}

	field, _ := params["field"].(string)
	if field == "" && op != opCount {
		return "", "", false, "", "", fmt.Errorf("aggregator params: 'field' is required for op %q", op)
	}

	// 'group' is optional. When absent we aggregate over a single bucket per
	// clientID; today this is only meaningful for 'count' (used by query 5).
	groupRaw, hasGroup := params["group"].(map[string]any)
	if !hasGroup || len(groupRaw) == 0 {
		if op != opCount {
			return "", "", false, "", "", fmt.Errorf("aggregator params: ungrouped aggregation only supported for op %q (got %q)", opCount, op)
		}
		return op, field, false, "", "", nil
	}
	if len(groupRaw) > 1 {
		return "", "", false, "", "", fmt.Errorf("aggregator params: 'group' supports a single source")
	}
	var groupSource, groupField string
	for k, v := range groupRaw {
		groupSource = k
		s, ok := v.(string)
		if !ok {
			return "", "", false, "", "", fmt.Errorf("aggregator params: group value must be a string")
		}
		groupField = s
	}

	return op, field, true, groupSource, groupField, nil
}
