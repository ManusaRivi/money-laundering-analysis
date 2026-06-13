package aggregator

import (
	"fmt"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
)

type aggFunction string

const (
	opMax   aggFunction = "max"
	opMin   aggFunction = "min"
	opSum   aggFunction = "sum"
	opCount aggFunction = "count"
	opAvg   aggFunction = "avg"
)

type avgState struct {
	sum    float64
	count  int
	sample protocol.Transaction
}

// combine merges the incoming transaction into the stored aggregate for its group.
// If no prior entry exists, the incoming tx becomes the seed.
func (a *Aggregator) combine(current, incoming protocol.Transaction, hasCurrent bool) protocol.Transaction {
	if !hasCurrent {
		switch a.aggFunction {
		case opCount:
			seed := incoming
			seed.AmountPaid = 1
			return seed
		case opSum:
			seed := incoming
			return seed
		default:
			return incoming
		}
	}

	switch a.aggFunction {
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
		current.AmountPaid += a.fieldValue(incoming)
		return current
	case opCount:
		current.AmountPaid++
		return current
	default:
		return current
	}
}

func (a *Aggregator) fieldValue(tx protocol.Transaction) float64 {
	switch a.field {
	case "Amount":
		return tx.AmountPaid
	default:
		return 0
	}
}

func (a *Aggregator) extractGroupKey(tx protocol.Transaction) (string, error) {
	var acct *domain.Account
	switch a.groupSource {
	case "origin":
		acct = &domain.Account{
			BankID: tx.FromBank,
			ID:     tx.FromAccount,
		}
	case "dest":
		acct = &domain.Account{
			BankID: tx.ToBank,
			ID:     tx.ToAccount,
		}
	case "format":
		if tx.PaymentFormat == "" {
			return "", fmt.Errorf("Transaction missing format")
		}
		return tx.PaymentFormat, nil
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

func parseParams(params map[string]any) (aggFunction, string, bool, string, string, error) {
	rawType, ok := params["type"].(string)
	if !ok {
		return "", "", false, "", "", fmt.Errorf("aggregator params: missing or invalid 'type'")
	}
	op := aggFunction(rawType)
	switch op {
	case opMax, opMin, opSum, opCount, opAvg:
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
