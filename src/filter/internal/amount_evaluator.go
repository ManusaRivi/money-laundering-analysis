package internal

import (
    "money-laundering-analysis/src/common/transaction"
    "money-laundering-analysis/src/filter/config"
)

type AmountEvaluator struct {
    Field     string
    Threshold float64
    Operator  string
}

func newAmountEvaluator(cfg config.FilterConfig) (*AmountEvaluator, error) {
    // TODO: Validar que cfg.Operator sea uno de los operadores permitidos (>, <, ==, >=, <=)
    return &AmountEvaluator{
        Field:     cfg.Field,
        Threshold: cfg.ValueFloat,
        Operator:  cfg.Operator,
    }, nil
}

func (f *AmountEvaluator) Evaluate(tx transaction.Transaction) bool {

    amount, ok := tx.GetFloatProperty(f.Field)
    if !ok {
        // TODO: Log error o manejar caso donde el campo no existe o no es un float
        return false
    }

    switch f.Operator {
    case ">":
        return amount > f.Threshold
    case "<":
        return amount < f.Threshold
    case "==":
        return amount == f.Threshold
    case ">=":
        return amount >= f.Threshold
    case "<=":
        return amount <= f.Threshold
    default:
        return false
    }
}