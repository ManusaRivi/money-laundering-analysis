package filter

import (
	"fmt"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/eof"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
	"github.com/google/uuid"
)

type SyncFilter struct {
	cfg	config.WorkerConfig
	Broker broker.Broker
	Type   string `json:"type"`  // Tipo de filtro: "amount", "date_range", etc.
	Field  string `json:"field"` // Campo a filtrar: "Amount", "Timestamp"

	// Campos para filtros simples (amount, string)
	Operator    string  `json:"operator"`
	ValueFloat  float64 `json:"value_float"`
	ValueString string  `json:"value_string"`

	syncEOFController *eof.SyncEOFController

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
		cfg: 	   	cfg,
		Broker:      broker,
		Type:        typeVal,
		Field:       field,
		Operator:    operator,
		ValueFloat:  valueFloat,
		ValueString: valueString,
		syncEOFController: nil,
	}, nil
}

func (f *SyncFilter) Run() error {
	defer func() {
		f.Broker.StopConsuming()
	}()

	var err error
	f.syncEOFController, err = eof.NewSyncEOFController(
		f.cfg.SyncEOFConfig,
		f.onflush,
		f.onLeaderFlush,
		f.onRetryExceeded,
	)

	if err != nil {
		slog.Error("Error creating SyncEOFController", "error", err)
		return err
	}

	go f.syncEOFController.Start()
		
	return f.Broker.StartConsuming(func(msg broker.Message, ack func(), nack func()) {
		err := f.handleMessage(msg)
		if err != nil {
			slog.Error("Error handling message", "error", err)
			nack()
			return
		}
		ack()
	})
}

func (f *SyncFilter) onflush(clientID uuid.UUID) error {
	// El filtro sincronizado esta constantemente haciendo flush, no tiene nada que hacer cuando recibe el callback de flush.
	return nil
}

func (f *SyncFilter) onRetryExceeded(clientID uuid.UUID) error {
	// TODO: Loguear que el cliente supero el maximo de reintentos y tomar la decision que se considere (ej: emitir un EOF forzado, loguear un error, etc)
	return nil
}

func (f *SyncFilter) onLeaderFlush(clientID uuid.UUID, finalSent int) error {
	eofMsg, err := inner.MarshalQuery1EOFPacket(clientID)
	if err != nil {
		slog.Error("Error marshalling EOF packet", "error", err)
		return err
	}
	if err := f.Broker.Send(*eofMsg); err != nil {
		slog.Error("Error sending EOF packet to broker", "error", err)
		return err
	}
	// limpieza adicional si es necesaria
	return nil
}

func (f *SyncFilter) Stop() {}

// Private methods

func (f *SyncFilter) handleTransactionMessage(pkt inner.Packet) error {
	var data domain.Transaction
	if err := pkt.UnmarshalData(&data); err != nil {
		return err
	}
	f.syncEOFController.MessageReceived(pkt.ClientID)
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
		f.syncEOFController.MessageSent(pkt.ClientID)
	}
	return nil
}

func (f *SyncFilter) handleEOFMessage(pkt inner.Packet) error {
	// El filtro sincronizado no necesita hacer nada especial con los mensajes EOF, simplemente los propaga usando el EOFBroker.
	slog.Debug("Received EOF packet, forwarding to next worker...")

	var eofCounts domain.EOFCounts
	if err := pkt.UnmarshalData(&eofCounts); err != nil {
		slog.Error("Error unmarshalling EOF counts", "error", err)
		return err
	}
	total_transactions := eofCounts.Counts[broker.KeyNil]
	f.syncEOFController.SyncEof(pkt.ClientID, total_transactions)
	return nil
}

func (f *SyncFilter) handleMessage(msg broker.Message) error {
	pkt, err := inner.UnmarshalPacket(msg)

	if err != nil {
		slog.Error("Error unmarshalling packet", "error", err)
		return err
	}

	switch pkt.Type {
	case inner.TypeTransaction:
		f.handleTransactionMessage(*pkt)
	case inner.TypeEOF:
		f.handleEOFMessage(*pkt)
	default:
		return fmt.Errorf("unexpected inbound packet type: %v", pkt.Type)
	}
	return nil
}
