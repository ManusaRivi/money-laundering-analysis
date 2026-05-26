package filter

import (
	"fmt"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
)

type SyncFilter struct {
	Broker broker.Broker
	Type   string `json:"type"`  // Tipo de filtro: "amount", "date_range", etc.
	Field  string `json:"field"` // Campo a filtrar: "Amount", "Timestamp"

	// Campos para filtros simples (amount, string)
	Operator    string  `json:"operator"`
	ValueFloat  float64 `json:"value_float"`
	ValueString string  `json:"value_string"`

	// Campos para filtro por rango de fechas
	// FromDate    string  `json:"from_date"`
	// ToDate      string  `json:"to_date"`
}

func NewSyncFilter(cfg config.WorkerConfig, broker broker.Broker) (*SyncFilter, error) {
	params := cfg.Params
	typeVal, ok := params["type"].(string)
	if !ok {
		return nil, fmt.Errorf("Invalid type parameter for SyncAmountFilter")
	}
	field, ok := params["field"].(string)
	if !ok {
		return nil, fmt.Errorf("Invalid field parameter for SyncAmountFilter")
	}
	operator, ok := params["operator"].(string)
	if !ok {
		return nil, fmt.Errorf("Invalid operator parameter for SyncAmountFilter")
	}
	valueFloat, ok := params["value_float"].(float64)
	if !ok {
		return nil, fmt.Errorf("Invalid value_float parameter for SyncAmountFilter")
	}
	valueString, ok := params["value_string"].(string)
	if !ok {
		return nil, fmt.Errorf("Invalid value_string parameter for SyncAmountFilter")
	}
	return &SyncFilter{
		Broker:      broker,
		Type:        typeVal,
		Field:       field,
		Operator:    operator,
		ValueFloat:  valueFloat,
		ValueString: valueString,
	}, nil
}

func (f *SyncFilter) Run() error {
	defer func() {
		f.Broker.StopConsuming()
		f.Broker.Close()
	}()
	f.Broker.StartConsuming(func(msg broker.Message, ack func(), nack func()) {
		err := f.handleMessage(msg)
		if err != nil {
			slog.Error("Error handling message", "error", err)
			nack()
			return
		}
		ack()
	})
	return nil
}

func (f *SyncFilter) Stop() {}

// Private methods

func (f *SyncFilter) handleMessage(msg broker.Message) error {
	pkt, err := inner.UnmarshalPacket(msg)

	if err != nil {
		slog.Error("Error unmarshalling packet", "error", err)
		return err
	}

	switch pkt.Type {
	case inner.TypeTransaction:
		var data domain.Transaction
		if err := pkt.UnmarshalData(&data); err != nil {
			return err
		}
		if filterTransaction(data, f.Type, f.Operator, f.ValueFloat, f.ValueString) {
			queryResult := domain.Query1Result{
				FromBank:    data.Origin.BankID,
				FromAccount: data.Origin.ID,
				ToBank:      data.Dest.BankID,
				ToAccount:   data.Dest.ID,
				AmountPaid:  data.Paid.Amount,
			}
			responseMsg, err := inner.MarshalQuery1ResultPacket(pkt.ClientID, queryResult)
			if err != nil {
				return err
			}
			if err := f.Broker.Send(*responseMsg); err != nil {
				return err
			}
		}
	case inner.TypeEOF:
		slog.Debug("Handling EOF Packet...")
		// Propagar EOF usando el EOFBroker
		eofMsg, err := inner.MarshalQuery1EOFPacket(pkt.ClientID)
		if err != nil {
			return err
		}
		if err := f.Broker.Send(*eofMsg); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unexpected inbound packet type: %v", pkt.Type)
	}
	return nil
}
