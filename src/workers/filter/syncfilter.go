package filter

import (
	"fmt"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/batch"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/eof"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external/codec"
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

	// Query 1: los resultados se acumulan y publican como lotes binarios del
	// protocolo external, que el gateway reenvía al cliente sin decodificar.
	codec    codec.Codec
	q1Buffer *batch.Buffer[external.Query1Result]
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
		codec:             codec.New(),
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

func (f *SyncFilter) Stop() {}

// Private methods

func (f *SyncFilter) onflush(clientID uuid.UUID) error {
	return nil
}

func (f *SyncFilter) onRetryExceeded(clientID uuid.UUID) error {
	// TODO: Loguear que el cliente supero el maximo de reintentos y tomar la decision que se considere (ej: emitir un EOF forzado, loguear un error, etc)
	return nil
}

func (f *SyncFilter) onLeaderFlush(clientID uuid.UUID, finalSent map[broker.KeyType]int) error {
	slog.Debug("Sending EOF to next worker...")
	eofMsg, err := f.resolveEOFMessage(clientID, finalSent)
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

func (f *SyncFilter) resolveEOFMessage(clientID uuid.UUID, finalSent map[broker.KeyType]int) (*broker.Message, error) {
	if f.cfg.Query == Query1 {
		envelope, err := f.codec.EncodeInternalEnvelope(external.InternalEnvelope{
			MsgType:  external.MsgQuery1ResultEOF,
			ClientId: clientID,
			Payload:  nil,
		})
		if err != nil {
			return nil, fmt.Errorf("encoding query 1 EOF envelope: %w", err)
		}
		return &broker.Message{
			RoutingKey:  broker.KeyNil,
			Body:        envelope,
			ContentType: broker.ContentTypeBinary,
		}, nil
	} else {
		eofPayload, err := f.codec.EncodeEOFCounts(finalSent)
		if err != nil {
			return nil, fmt.Errorf("encoding EOF counts for EOF envelope: %w", err)
		}
		envelope, err := f.codec.EncodeInternalEnvelope(external.InternalEnvelope{
			MsgType:  external.MsgTransactionsEOF,
			ClientId: clientID,
			Payload:  eofPayload,
		})
		if err != nil {
			return nil, fmt.Errorf("encoding transaction EOF envelope: %w", err)
		}
		return &broker.Message{
			RoutingKey:  broker.KeyControlEOF,
			Body:        envelope,
			ContentType: broker.ContentTypeBinary,
		}, nil
	}
}

func (f *SyncFilter) encodeAndSendBatch(clientID uuid.UUID, msgType external.MsgType, payload []byte, batchLength int) error {
	slog.Debug("Sending batch to broker:", "batchSize", batchLength, "clientId", clientID, "msgType", msgType)
	envelope, err := f.codec.EncodeInternalEnvelope(external.InternalEnvelope{
		MsgType:  msgType,
		ClientId: clientID,
		Payload:  payload,
	})
	if err != nil {
		return fmt.Errorf("encoding internal envelope: %w", err)
	}
	err = f.Broker.Send(broker.Message{
		RoutingKey:  broker.KeyNil,
		Body:        envelope,
		ContentType: broker.ContentTypeBinary,
	})
	if err != nil {
		return fmt.Errorf("sending message to broker: %w", err)
	}
	f.syncEOFController.MessageSentWithKey(clientID, broker.KeyNil, batchLength)
	return nil
}

func (f *SyncFilter) forwardTransactionBatchMessage(transactions []external.Transaction, clientID uuid.UUID) error {
	filteredTx := make([]external.Transaction, 0, len(transactions))
	f.syncEOFController.MessageReceived(clientID, len(transactions))
	for _, tx := range transactions {
		if filterTransaction(tx, f.Type, f.Operator, f.ValueFloat, f.ValueStrings) {
			filteredTx = append(filteredTx, tx)
		}
	}
	if len(filteredTx) == 0 {
		slog.Debug("No transactions passed the filter", "clientId", clientID)
		return nil
	}
	filteredTxBytes, err := f.codec.EncodeTransactionBatch(filteredTx)
	if err != nil {
		return fmt.Errorf("encoding filtered transaction batch: %w", err)
	}
	return f.encodeAndSendBatch(clientID, external.MsgTransactionsBatch, filteredTxBytes, len(filteredTx))
}

func (f *SyncFilter) forwardQuery1ResultBatchMessage(transactions []external.Transaction, clientID uuid.UUID) error {
	results := make([]external.Query1Result, 0)
	f.syncEOFController.MessageReceived(clientID, len(transactions))
	for _, tx := range transactions {
		if filterTransaction(tx, f.Type, f.Operator, f.ValueFloat, f.ValueStrings) {
			results = append(results, external.Query1Result{
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
	resultsBytes, err := f.codec.EncodeQuery1ResultBatch(results)
	if err != nil {
		return fmt.Errorf("encoding query1 result batch: %w", err)
	}
	return f.encodeAndSendBatch(clientID, external.MsgQuery1Result, resultsBytes, len(results))
}

func (f *SyncFilter) handleEOFMessage(envelope external.InternalEnvelope) error {
	// El filtro sincronizado no necesita hacer nada especial con los mensajes EOF, simplemente los propaga usando el EOFBroker.
	slog.Debug("Received EOF packet, beginning syncing...")
	counts, err := f.codec.DecodeEOFCounts(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding EOF counts", "error", err)
		return err
	}
	f.syncEOFController.SyncEof(envelope.ClientId, counts, f.syncEOFKey)
	return nil
}

func (f *SyncFilter) handleMessage(msg broker.Message) error {
	if msg.ContentType != broker.ContentTypeBinary {
		return fmt.Errorf("unexpected content type: %v", msg.ContentType)
	}

	envelope, err := f.codec.DecodeInternalEnvelope(msg.Body)
	if err != nil {
		return fmt.Errorf("decoding internal envelope: %w", err)
	}

	switch envelope.MsgType {
	case external.MsgTransactionsBatch:
		txBatch, err := f.codec.DecodeTransactionBatch(envelope.Payload)
		if err != nil {
			return fmt.Errorf("decoding transaction batch: %w", err)
		}
		slog.Debug("Received transactions batch", "batchSize", len(txBatch), "clientId", envelope.ClientId)
		if f.cfg.Query == Query1 {
			err := f.forwardQuery1ResultBatchMessage(txBatch, envelope.ClientId)
			if err != nil {
				return fmt.Errorf("forwarding query1 result batch: %w", err)
			}
		} else {
			err := f.forwardTransactionBatchMessage(txBatch, envelope.ClientId)
			if err != nil {
				return fmt.Errorf("forwarding transaction batch: %w", err)
			}
		}
	case external.MsgTransactionsEOF:
		f.handleEOFMessage(envelope)
	default:
		return fmt.Errorf("unexpected inbound packet type: %v", envelope.MsgType)
	}
	return nil
}
