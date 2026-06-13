package router

import (
	"fmt"
	"hash/fnv"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
)

func (r *Router) shardByField(tx protocol.Transaction) string {
	value := r.extractFieldValue(tx)
	h := fnv.New32a()
	h.Write([]byte(value))
	index := int(h.Sum32()) % r.nextWorkerAmount
	if index < 0 {
		index += r.nextWorkerAmount
	}
	return fmt.Sprintf("%s_%d", r.cfg.NextWorkerPrefix, index)
}

func (r *Router) extractFieldValue(tx protocol.Transaction) string {
	switch r.sectionToRouteBy {
	case "origin":
		return accountField(&domain.Account{
			BankID: tx.FromBank,
			ID:     tx.FromAccount,
		}, r.fieldToRouteBy)
	case "dest":
		return accountField(&domain.Account{
			BankID: tx.ToBank,
			ID:     tx.ToAccount,
		}, r.fieldToRouteBy)
	case "paid":
		return moneyField(&domain.Money{
			Amount:   tx.AmountPaid,
			Currency: tx.PaymentCurrency,
		}, r.fieldToRouteBy)
	case "format":
		return tx.PaymentFormat
	default:
		return ""
	}
}

func accountField(a *domain.Account, field string) string {
	if a == nil {
		return ""
	}
	switch field {
	case "BankID":
		return a.BankID
	case "ID":
		return a.ID
	default:
		return ""
	}
}

func moneyField(m *domain.Money, field string) string {
	if m == nil {
		return ""
	}
	switch field {
	case "Currency":
		return m.Currency
	default:
		return ""
	}
}
