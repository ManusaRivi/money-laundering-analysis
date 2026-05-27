package filter

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/eof"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
	"github.com/google/uuid"
)




type DateRange struct {
	cfg	config.WorkerConfig
	Broker broker.Broker
	Type   string // Tipo de filtro: "amount", "date_between", etc.
	// Field  string `json:"field"` // Campo a filtrar: "Amount", "Timestamp"

	// Campos para filtros simples (amount, string)
	// Operator    string  `json:"operator"`
	// ValueFloat  float64 `json:"value_float"`
	// ValueString string  `json:"value_string"`

	syncEOFController *eof.SyncEOFController
	syncEOFkeys []broker.KeyType
	usdTransactionsSent map[uuid.UUID]int

	// Campos para filtro por rango de fechas
	fromTime    time.Time
	toTime      time.Time
}

func NewDateRange(cfg config.WorkerConfig, brokerToUse broker.Broker) (*DateRange, error) {
	params := cfg.Params
	typeVal, ok := params["type"].(string)
	if !ok {
		return nil, fmt.Errorf("Invalid type parameter for DateRangeFilter")
	}
	// field, ok := params["field"].(string)
	// if !ok {
	// 	return nil, fmt.Errorf("Invalid field parameter for DateRangeFilter")
	// }
	fromDate, ok := params["from"].(string)
	if !ok {
		return nil, fmt.Errorf("Invalid from parameter for DateRangeFilter")
	}
	toDate, ok := params["to"].(string)
	if !ok {
		return nil, fmt.Errorf("Invalid to parameter for DateRangeFilter")
	}
	if fromDate == "" || toDate == "" {
		return nil, fmt.Errorf("both 'from' and 'to' are required for DateRangeFilter")
	}

	fromTime, err := time.Parse(time.RFC3339, fromDate)
	if err != nil {
		return nil, fmt.Errorf("invalid from date %q: %w", fromDate, err)
	}
	
	toTime, err := time.Parse(time.RFC3339, toDate)
	if err != nil {
		return nil, fmt.Errorf("invalid to date %q: %w", toDate, err)
	}

	syncEOFkeys := broker.StringsToKeyType(cfg.SyncEOFConfig.InputKeys)

	return &DateRange{
		cfg: 	   	cfg,
		Broker:      brokerToUse,
		Type:        typeVal,
		fromTime:    fromTime,
		toTime:      toTime,
		syncEOFController: nil,
		syncEOFkeys: syncEOFkeys,
	}, nil
}

func (f *DateRange) Run() error {
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

func (f *DateRange) onflush(clientID uuid.UUID) error {
	// El filtro sincronizado esta constantemente haciendo flush, no tiene nada que hacer cuando recibe el callback de flush.
	return nil
}

func (f *DateRange) onRetryExceeded(clientID uuid.UUID) error {
	// TODO: Loguear que el cliente supero el maximo de reintentos y tomar la decision que se considere (ej: emitir un EOF forzado, loguear un error, etc)
	return nil
}

func (f *DateRange) onLeaderFlush(clientID uuid.UUID, finalSent int) error {
	counts := map[broker.KeyType]int{
		broker.KeyAllTransaction: finalSent,
		broker.KeyDollarTransaction: f.usdTransactionsSent[clientID],
	}
	eofCounts := domain.EOFCounts{
		Counts: counts,
	}
	eofMsg, err := inner.MarshalEOFPacket(clientID, eofCounts)
	if err != nil {
		slog.Error("Error marshalling EOF packet", "error", err)
		return err
	}
	if err := f.Broker.Send(*eofMsg); err != nil {
		slog.Error("Error sending EOF packet to broker", "error", err)
		return err
	}
	
	delete(f.usdTransactionsSent, clientID)

	return nil
}

func (f *DateRange) Stop() {
	f.Broker.StopConsuming()
	f.Broker.Close()
}

// Private methods

func (f *DateRange) filterTransactionByDate(tx domain.Transaction) bool {
	if tx.Timestamp == "" {
		slog.Error("Transaction has no timestamp", "transaction", tx)
		return false
	}
	txTime, err := time.Parse(time.RFC3339, tx.Timestamp)
	if err != nil {
		slog.Error("Transaction has invalid timestamp", "timestamp", tx.Timestamp, "error", err)
		return false
	}
	if txTime.Before(f.fromTime) || txTime.After(f.toTime) {
		return false
	}
	return true
}

func (f *DateRange) handleTransactionMessage(pkt inner.Packet) error {
	var tx domain.Transaction
	if err := pkt.UnmarshalData(&tx); err != nil {
		return err
	}
	f.syncEOFController.MessageReceived(pkt.ClientID)
	
	if f.filterTransactionByDate(tx) {
		keyToUse := broker.KeyNonDollarTransaction
		if tx.IsUSDTransaction() {
			keyToUse = broker.KeyDollarTransaction
		}
		msdToSend, err := inner.MarshalTransactionPacket(pkt.ClientID, keyToUse, tx)
		if err != nil {
			return err
		}
		if err := f.Broker.Send(*msdToSend); err != nil {
			return err
		}
		f.syncEOFController.MessageSent(pkt.ClientID)

		if tx.IsUSDTransaction() {
			f.usdTransactionsSent[pkt.ClientID]++
		}
	}
	return nil
}

func (f *DateRange) handleEOFMessage(pkt inner.Packet) error {
	// El filtro sincronizado no necesita hacer nada especial con los mensajes EOF, simplemente los propaga usando el EOFBroker.
	slog.Debug("Received EOF packet, forwarding to next worker...")

	var eofCounts domain.EOFCounts
	if err := pkt.UnmarshalData(&eofCounts); err != nil {
		slog.Error("Error unmarshalling EOF counts", "error", err)
		return err
	}

	total_transactions := 0
	for _, key := range f.syncEOFkeys {
		total_transactions += eofCounts.Counts[key]
	}
	f.syncEOFController.SyncEof(pkt.ClientID, total_transactions)
	return nil
}

func (f *DateRange) handleMessage(msg broker.Message) error {
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
