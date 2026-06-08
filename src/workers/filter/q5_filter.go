package filter

import (
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external/codec"
)

type Q5Filter struct {
	cfg    config.WorkerConfig
	Broker broker.Broker
	codec  codec.Codec

	Type         string
	Field        string
	Operator     string
	ValueFloat   float64
	ValueStrings []string

	previousWorkerAmount int

	workerEofReceived     map[uuid.UUID]int
	receivedCount         map[uuid.UUID]int
	sentCount             map[uuid.UUID]int
	expectedPreviousCount map[uuid.UUID]int
}

func NewQ5Filter(cfg config.WorkerConfig, b broker.Broker) (*Q5Filter, error) {
	params := cfg.Params
	typeVal, ok := params["type"].(string)
	if !ok {
		return nil, fmt.Errorf("Invalid type parameter for Q5Filter")
	}
	field, ok := params["field"].(string)
	if !ok {
		return nil, fmt.Errorf("Invalid field parameter for Q5Filter")
	}
	operator, ok := params["operator"].(string)
	if !ok {
		return nil, fmt.Errorf("Invalid operator parameter for Q5Filter")
	}
	valueFloat, ok := params["value_float"].(float64)
	if !ok {
		return nil, fmt.Errorf("Invalid value_float parameter for Q5Filter")
	}
	valueStrings, err := parseValueStrings(params["value_string"])
	if err != nil {
		return nil, err
	}

	if cfg.PrevWorkerAmount <= 0 {
		return nil, fmt.Errorf("Q5Filter requires PREV_WORKER_AMOUNT to be set (got %d)", cfg.PrevWorkerAmount)
	}

	return &Q5Filter{
		cfg:                   cfg,
		Broker:                b,
		codec:                 codec.New(),
		Type:                  typeVal,
		Field:                 field,
		Operator:              operator,
		ValueFloat:            valueFloat,
		ValueStrings:          valueStrings,
		previousWorkerAmount:  cfg.PrevWorkerAmount,
		workerEofReceived:     make(map[uuid.UUID]int),
		receivedCount:         make(map[uuid.UUID]int),
		sentCount:             make(map[uuid.UUID]int),
		expectedPreviousCount: make(map[uuid.UUID]int),
	}, nil
}

func (f *Q5Filter) Run() error {
	defer f.Broker.StopConsuming()

	return f.Broker.StartConsuming(func(msg broker.Message, ack func(), nack func()) {
		if err := f.handleMessage(msg); err != nil {
			slog.Error("Error handling message", "error", err)
			nack()
			return
		}
		ack()
	})
}

func (f *Q5Filter) Stop() {}

// Private methods

func (f *Q5Filter) encodeAndSendBatch(clientID uuid.UUID, msgType external.MsgType, payload []byte, batchLength int) error {
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
	f.sentCount[clientID] += batchLength
	return nil
}

func (f *Q5Filter) handleTransactionMessage(envelope external.InternalEnvelope) error {
	transactions, err := f.codec.DecodeTransactionBatch(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding transactions batch", "error", err)
		return err
	}
	slog.Debug("Received transaction batch", "clientId", envelope.ClientId, "batchSize", len(transactions))
	f.receivedCount[envelope.ClientId] += len(transactions)
	filteredTx := make([]external.Transaction, 0, len(transactions))
	for _, tx := range transactions {
		if filterTransaction(tx, f.Type, f.Operator, f.ValueFloat, f.ValueStrings) {
			filteredTx = append(filteredTx, tx)
		}
	}
	if len(filteredTx) == 0 {
		slog.Debug("No transactions passed the filter", "clientId", envelope.ClientId)
		return nil
	}
	filteredTxBytes, err := f.codec.EncodeTransactionBatch(filteredTx)
	if err != nil {
		return fmt.Errorf("encoding filtered transaction batch: %w", err)
	}
	return f.encodeAndSendBatch(envelope.ClientId, external.MsgTransactionsBatch, filteredTxBytes, len(filteredTx))
}

func (f *Q5Filter) handleEOFMessage(envelope external.InternalEnvelope) error {
	clientID := envelope.ClientId
	counts, err := f.codec.DecodeEOFCounts(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding EOF counts", "error", err)
		return err
	}
	slog.Debug("Received EOF message", "clientId", clientID, "counts", counts)
	f.workerEofReceived[clientID]++
	f.expectedPreviousCount[clientID] += counts[broker.KeyNil]

	received := f.workerEofReceived[clientID]
	closed := received == f.previousWorkerAmount

	slog.Debug("Q5Filter EOF received",
		"clientID", clientID,
		"received", f.receivedCount[clientID],
		"expected", f.expectedPreviousCount[clientID],
	)

	if closed {
		if f.receivedCount[clientID] != f.expectedPreviousCount[clientID] {
			slog.Warn("EOF count mismatch for client",
				"clientID", clientID,
				"receivedCount", f.receivedCount[clientID],
				"expectedPreviousCount", f.expectedPreviousCount[clientID],
			)
			// TODO: decide what to do when there's a mismatch between received count and expected count. For now, we log a warning and proceed to send the EOF downstream.
		}
		slog.Debug("Sending EOF packet downstream for client", "clientID", clientID)

		eofPayload, err := f.codec.EncodeEOFCounts(map[broker.KeyType]int{broker.KeyNil: f.sentCount[clientID]})
		if err != nil {
			return fmt.Errorf("encoding EOF counts for EOF envelope: %w", err)
		}
		envelope, err := f.codec.EncodeInternalEnvelope(external.InternalEnvelope{
			MsgType:  external.MsgTransactionsEOF,
			ClientId: clientID,
			Payload:  eofPayload,
		})
		if err != nil {
			return fmt.Errorf("encoding transaction EOF envelope: %w", err)
		}
		err = f.Broker.Send(broker.Message{
			RoutingKey:  broker.KeyControlEOF,
			Body:        envelope,
			ContentType: broker.ContentTypeBinary,
		})
		if err != nil {
			return fmt.Errorf("sending EOF message to broker: %w", err)
		}
		delete(f.workerEofReceived, clientID)
		delete(f.receivedCount, clientID)
		delete(f.sentCount, clientID)
		delete(f.expectedPreviousCount, clientID)
	}
	return nil
}

func (f *Q5Filter) handleMessage(msg broker.Message) error {
	envelope, err := f.codec.DecodeInternalEnvelope(msg.Body)
	if err != nil {
		return fmt.Errorf("error decoding internal envelope: %w", err)
	}

	switch envelope.MsgType {
	case external.MsgTransactionsBatch:
		return f.handleTransactionMessage(envelope)
	case external.MsgTransactionsEOF:
		return f.handleEOFMessage(envelope)
	default:
		return fmt.Errorf("unexpected inbound packet type: %v", envelope.MsgType)
	}
}
