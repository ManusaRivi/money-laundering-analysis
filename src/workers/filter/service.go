package filter

import (
	"log/slog"
	"slices"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"
)

func filterTransactionByAmount(tx external.Transaction, operator string, value float64) bool {
	switch operator {
	case ">":
		return tx.AmountPaid > value
	case "<":
		return tx.AmountPaid < value
	case "==":
		return tx.AmountPaid == value
	default:
		slog.Error("Invalid operator for amount filter", "operator", operator)
		return false
	}
}

// filterTransactionByFormat returns true when the transaction's format matches
// the configured set of accepted values. Operator "==" means "format is in the
// set"; "!=" means "format is not in the set".
func filterTransactionByFormat(tx external.Transaction, operator string, values []string) bool {
	if tx.PaymentFormat == "" {
		slog.Error("Transaction has no format", "transaction", tx)
		return false
	}
	matched := slices.Contains(values, tx.PaymentFormat)
	switch operator {
	case "==":
		return matched
	case "!=":
		return !matched
	default:
		slog.Error("Invalid operator for format filter", "operator", operator)
		return false
	}
}

func filterTransaction(tx external.Transaction, filterType string, operator string, floatValue float64, stringValues []string) bool {
	switch filterType {
	case "amount":
		return filterTransactionByAmount(tx, operator, floatValue)
	case "format":
		return filterTransactionByFormat(tx, operator, stringValues)
	default:
		slog.Error("Invalid filter type", "filterType", filterType)
		return false
	}
}
