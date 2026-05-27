package filter

import (
	"log/slog"
	"slices"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
)

func filterTransactionByAmount(tx domain.Transaction, operator string, value float64) bool {
	money := tx.Paid
	if money == nil {
		slog.Error("Transaction has no amount", "transaction", tx)
		return false
	}
	switch operator {
	case ">":
		return money.Amount > value
	case "<":
		return money.Amount < value
	case "==":
		return money.Amount == value
	default:
		slog.Error("Invalid operator for amount filter", "operator", operator)
		return false
	}
}

// filterTransactionByFormat returns true when the transaction's format matches
// the configured set of accepted values. Operator "==" means "format is in the
// set"; "!=" means "format is not in the set".
func filterTransactionByFormat(tx domain.Transaction, operator string, values []string) bool {
	if tx.Format == "" {
		slog.Error("Transaction has no format", "transaction", tx)
		return false
	}
	matched := slices.Contains(values, tx.Format)
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

func filterTransaction(tx domain.Transaction, filterType string, operator string, floatValue float64, stringValues []string) bool {
	switch filterType {
	case "amount":
		return filterTransactionByAmount(tx, operator, floatValue)
	case "format":
		return filterTransactionByFormat(tx, operator, stringValues)
	// TODO: handle other types of filters (date range...?)
	default:
		slog.Error("Invalid filter type", "filterType", filterType)
		return false
	}
}
