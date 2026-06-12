package filter

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/eof"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external/codec"
	"github.com/google/uuid"
)

const timeTxFormat = "2006/01/02 15:04"
const Dollar = "US Dollar"

type DateRange struct {
	cfg      config.WorkerConfig
	Broker   broker.Broker
	codec    codec.Codec
	Type     string
	fromTime time.Time
	toTime   time.Time

	syncEOFController *eof.SyncEOFController
	syncEOFKey        broker.KeyType
}

func NewDateRange(cfg config.WorkerConfig, b broker.Broker) (*DateRange, error) {
	params := cfg.Params
	typeVal, ok := params["type"].(string)
	if !ok {
		return nil, fmt.Errorf("Invalid type parameter for DateRangeFilter")
	}

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

	fromTime, err := time.Parse(timeTxFormat, fromDate)
	if err != nil {
		return nil, fmt.Errorf("invalid from date %q: %w", fromDate, err)
	}

	toTime, err := time.Parse(timeTxFormat, toDate)
	if err != nil {
		return nil, fmt.Errorf("invalid to date %q: %w", toDate, err)
	}

	syncEOFKey := eof.SyncKeyFromInputKeys(cfg.SyncEOFConfig.InputKeys)

	return &DateRange{
		cfg:               cfg,
		Broker:            b,
		codec:             codec.New(),
		Type:              typeVal,
		fromTime:          fromTime,
		toTime:            toTime,
		syncEOFController: nil,
		syncEOFKey:        syncEOFKey,
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
	slog.Warn("Client exceeded max retries for EOF synchronization", "clientID", clientID)
	return nil
}

func (f *DateRange) onLeaderFlush(clientID uuid.UUID, finalSent map[broker.KeyType]int) error {
	slog.Debug("Forwarding EOF to next worker...")
	eofPayload, err := f.codec.EncodeEOFCounts(finalSent)
	if err != nil {
		slog.Error("Error encoding EOF counts", "error", err)
		return err
	}
	envelope, err := f.codec.EncodeInternalEnvelope(external.InternalEnvelope{
		MsgType:  external.MsgTransactionsEOF,
		ClientId: clientID,
		Payload:  eofPayload,
	})
	if err != nil {
		slog.Error("Error encoding internal envelope for EOF packet", "error", err)
		return err
	}
	brokerMsg := broker.Message{
		RoutingKey:  broker.KeyControlEOF,
		ContentType: broker.ContentTypeBinary,
		Body:        envelope,
	}
	err = f.Broker.Send(brokerMsg)
	if err != nil {
		slog.Error("Error sending EOF packet to broker", "error", err)
		return err
	}
	return nil
}

func (f *DateRange) Stop() {
	f.Broker.StopConsuming()
	f.Broker.Close()
}

// Private methods

func (f *DateRange) filterTransactionByDate(tx external.Transaction) bool {
	if tx.Timestamp == "" {
		slog.Error("Transaction has no timestamp", "transaction", tx)
		return false
	}

	txTime, err := time.Parse(timeTxFormat, tx.Timestamp)
	if err != nil {
		slog.Error("Transaction has invalid timestamp", "timestamp", tx.Timestamp, "error", err)
		return false
	}
	if txTime.Before(f.fromTime) || txTime.After(f.toTime) {
		return false
	}
	return true
}

func (f *DateRange) sendMessageToBroker(msgType external.MsgType, clientId uuid.UUID, payload []byte, routingKey broker.KeyType, payloadLen int) bool {
	envelope, err := f.codec.EncodeInternalEnvelope(external.InternalEnvelope{
		MsgType:  msgType,
		ClientId: clientId,
		Payload:  payload,
	})
	if err != nil {
		slog.Error("Error encoding internal envelope", "error", err)
		return false
	}
	brokerMsg := broker.Message{
		RoutingKey:  routingKey,
		ContentType: broker.ContentTypeBinary,
		Body:        envelope,
	}

	err = f.Broker.Send(brokerMsg)
	if err != nil {
		slog.Error("Error sending message to broker", "error", err)
		return false
	}
	f.syncEOFController.MessageSentWithKey(clientId, routingKey, payloadLen)
	return true
}

func (f *DateRange) sendTransactionBatch(transactions []external.Transaction, clientId uuid.UUID, routingKey broker.KeyType) bool {
	if len(transactions) == 0 {
		return true
	}
	slog.Debug("Sending transactions batch to broker", "batchSize", len(transactions), "routingKey", routingKey)
	txPayload, err := f.codec.EncodeTransactionBatch(transactions)
	if err != nil {
		slog.Error("Error marshalling transaction packet", "error", err)
		return false
	}
	return f.sendMessageToBroker(external.MsgTransactionsBatch, clientId, txPayload, routingKey, len(transactions))
}

func (f *DateRange) handleTransactionsBatchMessage(envelope external.InternalEnvelope) error {
	clientId := envelope.ClientId
	transactions, err := f.codec.DecodeTransactionBatch(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding transaction batch", "error", err)
		return err
	}
	f.syncEOFController.MessageReceived(clientId, len(transactions))
	slog.Debug("Received transaction batch, applying date range filter", "clientId", clientId)
	dollarTx := make([]external.Transaction, 0)
	nonDollarTx := make([]external.Transaction, 0)
	for _, tx := range transactions {
		if f.filterTransactionByDate(tx) {
			if tx.PaymentCurrency == Dollar {
				dollarTx = append(dollarTx, tx)
			} else {
				nonDollarTx = append(nonDollarTx, tx)
			}
		}
	}
	if !f.sendTransactionBatch(dollarTx, clientId, broker.KeyDollarTransaction) {
		return fmt.Errorf("error sending dollar transaction batch to broker")
	}
	if !f.sendTransactionBatch(nonDollarTx, clientId, broker.KeyNonDollarTransaction) {
		return fmt.Errorf("error sending non-dollar transaction batch to broker")
	}
	return nil
}

func (f *DateRange) handleEOFMessage(envelope external.InternalEnvelope) error {
	// El filtro sincronizado no necesita hacer nada especial con los mensajes EOF, simplemente los propaga usando el EOFBroker.
	slog.Debug("Received EOF packet, beginning syncing...", "clientID", envelope.ClientId)
	eofCounts, err := f.codec.DecodeEOFCounts(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding EOF counts", "error", err)
		return err
	}

	f.syncEOFController.SyncEof(envelope.ClientId, eofCounts, f.syncEOFKey)
	return nil
}

func (f *DateRange) handleMessage(msg broker.Message) error {
	envelope, err := f.codec.DecodeInternalEnvelope(msg.Body)
	if err != nil {
		slog.Error("Error decoding envelope", "error", err)
		return err
	}

	switch envelope.MsgType {
	case external.MsgTransactionsBatch:
		if err := f.handleTransactionsBatchMessage(envelope); err != nil {
			return err
		}
	case external.MsgTransactionsEOF:
		if err := f.handleEOFMessage(envelope); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unexpected inbound packet type: %v", envelope.MsgType)
	}
	return nil
}
