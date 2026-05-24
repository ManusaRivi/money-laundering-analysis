package filter

import (
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
)

func filterTransactionByAmount(tx domain.Transaction, operator string, value float64) bool {
	amount := tx.Paid
	switch operator {
	case ">":
		return amount.Amount > value
	case "<":
		return amount.Amount < value
	case "==":
		return amount.Amount == value
	default:
		slog.Error("Invalid operator for amount filter", "operator", operator)
		return false
	}
}

func filterTransactionByFormat(tx domain.Transaction, operator string, value string) bool {
	switch operator {
	case "==":
		return tx.Format == value
	default:
		slog.Error("Invalid operator for format filter", "operator", operator)
		return false
	}
}

func filterTransaction(tx domain.Transaction, filterType string, operator string, floatValue float64, stringValue string) bool {
	switch filterType {
	case "amount":
		return filterTransactionByAmount(tx, operator, floatValue)
	case "format":
		return filterTransactionByFormat(tx, operator, stringValue)
	// TODO: handle other types of filters (date range...?)
	default:
		slog.Error("Invalid filter type", "filterType", filterType)
		return false
	}
}
