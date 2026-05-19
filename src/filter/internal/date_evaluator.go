package internal

import (
    "fmt"
    "time"
    "money-laundering-analysis/src/common/transaction"
    "money-laundering-analysis/src/filter/config"
)

const dateFormat = "2006/01/02 15:04"

type DateRangeEvaluator struct {
	Field string
    From time.Time
    To   time.Time
}

func newDateRangeEvaluator(cfg config.FilterConfig) (*DateRangeEvaluator, error) {
    from, err := time.Parse(dateFormat, cfg.FromDate)
    if err != nil {
        return nil, fmt.Errorf("error parseando from_date: %w", err)
    }

    to, err := time.Parse(dateFormat, cfg.ToDate)
    if err != nil {
        return nil, fmt.Errorf("error parseando to_date: %w", err)
    }

    return &DateRangeEvaluator{
        Field: cfg.Field,
        From: from,
        To:   to,
    }, nil
}

func (f *DateRangeEvaluator) Evaluate(tx transaction.Transaction) bool {
    dateStr, ok := tx.GetStringProperty(f.Field)
    if !ok || dateStr == "" {
        // TODO: Log error o manejar caso donde el campo no existe o no es un string
        return false
    }

    txDate, err := time.Parse(dateFormat, dateStr)
    if err != nil {
        // TODO: Log error o manejar caso donde el campo no es un string con formato de fecha
        return false
    }

    return (txDate.After(f.From) || txDate.Equal(f.From)) &&
        (txDate.Before(f.To) || txDate.Equal(f.To))
}