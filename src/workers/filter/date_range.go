package filter

import (
	"fmt"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/checkpoint"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/eof"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/messaging"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
	"github.com/google/uuid"
)

const Dollar = "US Dollar"

type DateRange struct {
	cfg      config.WorkerConfig
	Broker   broker.Broker
	pub      *messaging.Publisher
	Type     string
	fromDate string
	toDate   string

	syncEOFController *eof.SyncEOFController
	syncEOFKey        broker.KeyType
	coord             *checkpoint.Coordinator
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

	syncEOFKey := eof.SyncKeyFromInputKeys(cfg.SyncEOFConfig.InputKeys)

	return &DateRange{
		cfg:               cfg,
		Broker:            b,
		pub:               messaging.New(codec.New(), b),
		Type:              typeVal,
		fromDate:          fromDate,
		toDate:            toDate,
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

	checkpointManager, err := checkpoint.NewManager(f.cfg.CheckpointDir)
	if err != nil {
		slog.Error("Error creating checkpoint manager", "error", err)
		return err
	}
	f.coord = checkpoint.NewCoordinator(checkpointManager, f.pub, f.syncEOFController, nil, f.cfg.CheckpointInterval)
	if err := f.coord.Recover(); err != nil {
		return err
	}

	go f.syncEOFController.Start()

	return f.Broker.StartConsuming(func(msg broker.Message, ack func(), nack func()) {
		clientID, msgType, err := f.handleMessage(msg)
		if err != nil {
			slog.Error("Error handling message", "error", err)
			nack()
			return
		}
		f.coord.Track(clientID, ack)
		if msgType == protocol.MsgTransactionsEOF {
			f.coord.Flush()
		}
	})
}

func (f *DateRange) onflush(clientID uuid.UUID) error {
	return f.coord.Flush()
}

func (f *DateRange) onRetryExceeded(clientID uuid.UUID) error {
	// TODO: Loguear que el cliente supero el maximo de reintentos y tomar la decision que se considere (ej: emitir un EOF forzado, loguear un error, etc)
	slog.Warn("Client exceeded max retries for EOF synchronization", "clientID", clientID)
	return nil
}

func (f *DateRange) onLeaderFlush(clientID uuid.UUID, finalSent map[broker.KeyType]int) error {
	slog.Debug("Forwarding EOF to next worker...")
	eofPayload, err := f.pub.EncodeEOFCounts(finalSent)
	if err != nil {
		slog.Error("Error encoding EOF counts", "error", err)
		return err
	}
	eofID := protocol.StageMsgID(clientID, f.cfg.WorkerPrefix, "eof", 0)
	if err := f.pub.PublishInternalWithID(clientID, protocol.MsgTransactionsEOF, broker.KeyControlEOF, eofPayload, eofID); err != nil {
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

func (f *DateRange) filterTransactionByDate(tx protocol.Transaction) bool {
	if tx.Timestamp == "" {
		slog.Error("Transaction has no timestamp", "transaction", tx)
		return false
	}

	if tx.Timestamp < f.fromDate || tx.Timestamp > f.toDate {
		return false
	}
	return true
}

func (f *DateRange) sendMessageToBroker(msgType protocol.MsgType, clientId uuid.UUID, payload []byte, routingKey broker.KeyType, payloadLen int, id protocol.MsgID) bool {
	if err := f.pub.PublishInternalWithID(clientId, msgType, routingKey, payload, id); err != nil {
		slog.Error("Error sending message to broker", "error", err)
		return false
	}
	f.syncEOFController.MessageSentWithKey(clientId, routingKey, payloadLen)
	return true
}

func (f *DateRange) sendTransactionBatch(transactions []protocol.Transaction, clientId uuid.UUID, routingKey broker.KeyType, parentID protocol.MsgID) bool {
	if len(transactions) == 0 {
		return true
	}
	slog.Debug("Sending transactions batch to broker", "batchSize", len(transactions), "routingKey", routingKey)
	txPayload, err := f.pub.EncodeTransactionBatch(transactions)
	if err != nil {
		slog.Error("Error marshalling transaction packet", "error", err)
		return false
	}
	id := protocol.DeriveMsgID(parentID, string(routingKey), 0)
	return f.sendMessageToBroker(protocol.MsgTransactionsBatch, clientId, txPayload, routingKey, len(transactions), id)
}

func (f *DateRange) handleTransactionsBatchMessage(envelope protocol.InternalEnvelope) error {
	clientId := envelope.ClientId
	transactions, err := f.pub.DecodeTransactionBatch(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding transaction batch", "error", err)
		return err
	}
	slog.Debug("Received transaction batch, applying date range filter", "clientId", clientId)
	dollarTx := make([]protocol.Transaction, 0)
	nonDollarTx := make([]protocol.Transaction, 0)
	for _, tx := range transactions {
		if f.filterTransactionByDate(tx) {
			if tx.PaymentCurrency == Dollar {
				dollarTx = append(dollarTx, tx)
			} else {
				nonDollarTx = append(nonDollarTx, tx)
			}
		}
	}
	if !f.sendTransactionBatch(dollarTx, clientId, broker.KeyDollarTransaction, envelope.MsgID) {
		return fmt.Errorf("error sending dollar transaction batch to broker")
	}
	if !f.sendTransactionBatch(nonDollarTx, clientId, broker.KeyNonDollarTransaction, envelope.MsgID) {
		return fmt.Errorf("error sending non-dollar transaction batch to broker")
	}
	f.syncEOFController.MessageReceived(clientId, len(transactions))
	return nil
}

func (f *DateRange) handleEOFMessage(envelope protocol.InternalEnvelope) error {
	// El filtro sincronizado no necesita hacer nada especial con los mensajes EOF, simplemente los propaga usando el EOFBroker.
	slog.Debug("Received EOF packet, beginning syncing...", "clientID", envelope.ClientId)
	eofCounts, err := f.pub.DecodeEOFCounts(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding EOF counts", "error", err)
		return err
	}

	f.syncEOFController.SyncEof(envelope.ClientId, eofCounts, f.syncEOFKey)
	return nil
}

func (f *DateRange) handleMessage(msg broker.Message) (uuid.UUID, protocol.MsgType, error) {
	return f.pub.Dispatch(msg, map[protocol.MsgType]messaging.Handler{
		protocol.MsgTransactionsBatch: f.handleTransactionsBatchMessage,
		protocol.MsgTransactionsEOF:   f.handleEOFMessage,
	})
}
