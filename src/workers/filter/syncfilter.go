package filter

import (
	"fmt"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/batch"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/checkpoint"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/eof"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/messaging"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
	"github.com/google/uuid"
)

const Query1 = 1

type SyncFilter struct {
	cfg    config.WorkerConfig
	Broker broker.Broker
	Type   string `json:"type"`  // Tipo de filtro: "amount", "date_range", etc.
	Field  string `json:"field"` // Campo a filtrar: "Amount", "Timestamp"

	// Campos para filtros simples (amount, string)
	Operator     string   `json:"operator"`
	ValueFloat   float64  `json:"value_float"`
	ValueStrings []string `json:"value_string"`

	syncEOFController *eof.SyncEOFController
	syncEOFKey        broker.KeyType
	coord             *checkpoint.Coordinator

	// Query 1: los resultados se acumulan y publican como lotes binarios del
	// protocolo external, que el gateway reenvía al cliente sin decodificar.
	pub      *messaging.Publisher
	q1Buffer *batch.Buffer[protocol.Query1Result]
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
		return nil, fmt.Errorf("Invalid value_float parameter for SyncFilter")
	}
	valueStrings, err := parseValueStrings(params["value_string"])
	if err != nil {
		return nil, err
	}
	if typeVal == "format" && len(valueStrings) == 0 {
		return nil, fmt.Errorf("format filter requires at least one value_string entry")
	}
	syncEOFKey := eof.SyncKeyFromInputKeys(cfg.SyncEOFConfig.InputKeys)

	return &SyncFilter{
		cfg:               cfg,
		Broker:            broker,
		Type:              typeVal,
		Field:             field,
		Operator:          operator,
		ValueFloat:        valueFloat,
		ValueStrings:      valueStrings,
		syncEOFController: nil,
		syncEOFKey:        syncEOFKey,
		pub:               messaging.New(codec.New(), broker),
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

	coord, err := checkpoint.NewCoordinator(f.cfg.CheckpointDir, f.pub, f.syncEOFController, nil, f.cfg.CheckpointInterval)
	if err != nil {
		slog.Error("Error creating checkpoint coordinator", "error", err)
		return err
	}
	f.coord = coord
	if err := f.coord.Recover(); err != nil {
		return err
	}
	go f.syncEOFController.Start()

	return f.Broker.StartConsuming(func(msg broker.Message, ack func(), nack func()) {
		clientID, msgType, err := f.handleMessage(msg, ack)
		if err != nil {
			slog.Error("Error handling message", "error", err)
			nack()
			return
		}
		if msgType != protocol.MsgTransactionsEOF {
			f.coord.Track(clientID, ack)
		}
	})
}

func (f *SyncFilter) Stop() {
	if f.syncEOFController != nil {
		f.syncEOFController.Stop()
	}
}

// Private methods

func (f *SyncFilter) onflush(clientID uuid.UUID) error {
	if err := f.coord.Flush(); err != nil {
		return err
	}
	f.pub.Forget(clientID)
	return f.coord.Delete(clientID)
}

func (f *SyncFilter) onRetryExceeded(clientID uuid.UUID) error {
	// TODO: Loguear que el cliente supero el maximo de reintentos y tomar la decision que se considere (ej: emitir un EOF forzado, loguear un error, etc)
	return nil
}

func (f *SyncFilter) onLeaderFlush(clientID uuid.UUID, finalSent map[broker.KeyType]int) error {
	slog.Debug("Sending EOF to next worker...")
	msgType, key, payload, err := f.resolveEOFMessage(finalSent)
	if err != nil {
		slog.Error("Error marshalling EOF packet", "error", err)
		return err
	}
	eofID := protocol.StageMsgID(clientID, f.cfg.WorkerPrefix, "eof", 0)
	if err := f.pub.PublishInternalWithID(clientID, msgType, key, payload, eofID); err != nil {
		slog.Error("Error sending EOF packet to broker", "error", err)
		return err
	}
	// limpieza adicional si es necesaria
	return nil
}

// parseValueStrings accepts either nil, a scalar string, or a YAML list of
// strings and normalises to []string. An empty scalar yields an empty slice,
// which lets amount-style filters omit the field without erroring.
func parseValueStrings(raw any) ([]string, error) {
	switch v := raw.(type) {
	case nil:
		return nil, nil
	case string:
		if v == "" {
			return nil, nil
		}
		return []string{v}, nil
	case []any:
		out := make([]string, 0, len(v))
		for _, e := range v {
			s, ok := e.(string)
			if !ok {
				return nil, fmt.Errorf("value_string entries must be strings, got %T", e)
			}
			out = append(out, s)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("Invalid value_string parameter for SyncFilter: %T", raw)
	}
}

func (f *SyncFilter) resolveEOFMessage(finalSent map[broker.KeyType]int) (protocol.MsgType, broker.KeyType, []byte, error) {
	if f.cfg.Query == Query1 {
		return protocol.MsgQuery1ResultEOF, broker.KeyNil, nil, nil
	}
	eofPayload, err := f.pub.EncodeEOFCounts(finalSent)
	if err != nil {
		return 0, broker.KeyNil, nil, fmt.Errorf("encoding EOF counts for EOF envelope: %w", err)
	}
	return protocol.MsgTransactionsEOF, broker.KeyControlEOF, eofPayload, nil
}

func (f *SyncFilter) encodeAndSendBatch(clientID uuid.UUID, msgType protocol.MsgType, payload []byte, batchLength int, id protocol.MsgID) error {
	slog.Debug("Sending batch to broker:", "batchSize", batchLength, "clientId", clientID, "msgType", msgType)
	if err := f.pub.PublishInternalWithID(clientID, msgType, broker.KeyNil, payload, id); err != nil {
		return err
	}
	f.syncEOFController.MessageSentWithKey(clientID, broker.KeyNil, id, batchLength)
	return nil
}

func (f *SyncFilter) forwardTransactionBatchMessage(transactions []protocol.Transaction, clientID uuid.UUID, parentID protocol.MsgID) error {
	filteredTx := make([]protocol.Transaction, 0, len(transactions))
	f.syncEOFController.MessageReceived(clientID, parentID, len(transactions))
	for _, tx := range transactions {
		if filterTransaction(tx, f.Type, f.Operator, f.ValueFloat, f.ValueStrings) {
			filteredTx = append(filteredTx, tx)
		}
	}
	if len(filteredTx) == 0 {
		slog.Debug("No transactions passed the filter", "clientId", clientID)
		return nil
	}
	filteredTxBytes, err := f.pub.EncodeTransactionBatch(filteredTx)
	if err != nil {
		return fmt.Errorf("encoding filtered transaction batch: %w", err)
	}
	txID := protocol.DeriveMsgID(parentID, string(broker.KeyNil), 0)
	return f.encodeAndSendBatch(clientID, protocol.MsgTransactionsBatch, filteredTxBytes, len(filteredTx), txID)
}

func (f *SyncFilter) forwardQuery1ResultBatchMessage(transactions []protocol.Transaction, clientID uuid.UUID, parentID protocol.MsgID) error {
	results := make([]protocol.Query1Result, 0)
	f.syncEOFController.MessageReceived(clientID, parentID, len(transactions))
	for _, tx := range transactions {
		if filterTransaction(tx, f.Type, f.Operator, f.ValueFloat, f.ValueStrings) {
			results = append(results, protocol.Query1Result{
				FromBank:    tx.FromBank,
				FromAccount: tx.FromAccount,
				ToBank:      tx.ToBank,
				ToAccount:   tx.ToAccount,
				AmountPaid:  tx.AmountPaid,
			})
		}
	}
	if len(results) == 0 {
		slog.Debug("No transactions passed the filter for Query 1", "clientId", clientID)
		return nil
	}
	resultsBytes, err := f.pub.EncodeQuery1ResultBatch(results)
	if err != nil {
		return fmt.Errorf("encoding query1 result batch: %w", err)
	}
	resultID := protocol.DeriveMsgID(parentID, string(broker.KeyNil), 0)
	return f.encodeAndSendBatch(clientID, protocol.MsgQuery1Result, resultsBytes, len(results), resultID)
}

func (f *SyncFilter) handleEOFMessage(envelope protocol.InternalEnvelope, ack func()) error {
	// El filtro sincronizado no necesita hacer nada especial con los mensajes EOF, simplemente los propaga usando el EOFBroker.
	slog.Debug("Received EOF packet, beginning syncing...")
	counts, err := f.pub.DecodeEOFCounts(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding EOF counts", "error", err)
		return err
	}
	f.coord.Flush()
	f.syncEOFController.SyncEof(envelope.ClientId, counts, f.syncEOFKey, ack)
	return nil
}

func (f *SyncFilter) handleTransactionsBatchMessage(envelope protocol.InternalEnvelope) error {
	txBatch, err := f.pub.DecodeTransactionBatch(envelope.Payload)
	if err != nil {
		return fmt.Errorf("decoding transaction batch: %w", err)
	}
	slog.Debug("Received transactions batch", "batchSize", len(txBatch), "clientId", envelope.ClientId)
	if f.cfg.Query == Query1 {
		if err := f.forwardQuery1ResultBatchMessage(txBatch, envelope.ClientId, envelope.MsgID); err != nil {
			return fmt.Errorf("forwarding query1 result batch: %w", err)
		}
	} else {
		if err := f.forwardTransactionBatchMessage(txBatch, envelope.ClientId, envelope.MsgID); err != nil {
			return fmt.Errorf("forwarding transaction batch: %w", err)
		}
	}
	return nil
}

func (f *SyncFilter) handleMessage(msg broker.Message, ack func()) (uuid.UUID, protocol.MsgType, error) {
	if msg.ContentType != broker.ContentTypeBinary {
		return uuid.Nil, protocol.MsgType(0), fmt.Errorf("unexpected content type: %v", msg.ContentType)
	}

	return f.pub.Dispatch(msg, map[protocol.MsgType]messaging.Handler{
		protocol.MsgTransactionsBatch: f.handleTransactionsBatchMessage,
		protocol.MsgTransactionsEOF:   func(envelope protocol.InternalEnvelope) error { return f.handleEOFMessage(envelope, ack) },
	})
}
