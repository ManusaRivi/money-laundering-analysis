// src/filter/internal/filter.go
package internal

import (
    "encoding/json"
    "fmt"
    "log"
    "os"
    "os/signal"
    "syscall"

    "money-laundering-analysis/src/common/transaction"
    "money-laundering-analysis/src/filter/config"
)

type Evaluator interface {
    Evaluate(tx transaction.Transaction) bool
}

func newEvaluator(cfg config.FilterConfig) (Evaluator, error) {
    switch cfg.Type {
    case "amount":
        return newAmountEvaluator(cfg)
    case "date_range":
        return newDateRangeEvaluator(cfg)
    default:
        return nil, fmt.Errorf("tipo de evaluación desconocido: %s", cfg.Type)
    }
}

type Filter struct {
    cfg       config.Config
    evaluator Evaluator
}

func NewFilter(cfg config.Config) (*Filter, error) {
    eval, err := newEvaluator(cfg.Filter)
    if err != nil {
        return nil, err
    }

    return &Filter{
        cfg:       cfg,
        evaluator: eval,
    }, nil
}

func (f *Filter) Run() {
    // Correr filtro
}

func (f *Filter) processMessage(msg []byte) {
    var tx transaction.Transaction
    // Deserializar mensaje en tx

    if f.evaluator.Evaluate(tx) {
        // Paso el filtro, enviar a la output queue
    }
}

func (f *Filter) Stop() {
    // Cerrar rabbit y liberar recursos
}