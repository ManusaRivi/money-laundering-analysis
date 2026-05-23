package filter

import (
	"fmt"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
)

type SyncAmountFilter struct {
	Broker broker.Broker
	Type  string `json:"type"`  // Tipo de filtro: "amount", "date_range", etc.
	Field string `json:"field"` // Campo a filtrar: "Amount", "Timestamp"

	// Campos para filtros simples (amount, string)
	Operator   string  `json:"operator"`
	ValueFloat float64 `json:"value_float"`
	// ValueString string  `json:"value_string"`

	// Campos para filtro por rango de fechas
	// FromDate    string  `json:"from_date"`
	// ToDate      string  `json:"to_date"`
}

func NewSyncAmountFilter(params map[string]any, broker broker.Broker) (*SyncAmountFilter, error) {
	typeVal, ok := params["type"].(string); if !ok {
		return nil, fmt.Errorf("Invalid type parameter for SyncAmountFilter")
	}
	field, ok := params["field"].(string); if !ok {
		return nil, fmt.Errorf("Invalid field parameter for SyncAmountFilter")
	}
	operator, ok := params["operator"].(string); if !ok {
		return nil, fmt.Errorf("Invalid operator parameter for SyncAmountFilter")
	}
	valueFloat, ok := params["value_float"].(float64); if !ok {
		return nil, fmt.Errorf("Invalid value_float parameter for SyncAmountFilter")
	}
	return &SyncAmountFilter{
		Broker:     broker,
		Type:       typeVal,
		Field:      field,
		Operator:   operator,
		ValueFloat: valueFloat,
	}, nil
}

func (f *SyncAmountFilter) Run() error {
	return nil
}

func (f *SyncAmountFilter) Stop() {}