package filter

import (
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/checkpoint"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/messaging"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
)

type Q5Filter struct {
	cfg    config.WorkerConfig
	Broker broker.Broker
	pub    *messaging.Publisher

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

	coord *checkpoint.Coordinator
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
		pub:                   messaging.New(codec.New(), b),
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

	checkpointManager, err := checkpoint.NewManager(f.cfg.CheckpointDir)
	if err != nil {
		slog.Error("Error creating checkpoint manager", "error", err)
		return err
	}
	f.coord = checkpoint.NewCoordinator(checkpointManager, f.pub, nil, f.cfg.CheckpointInterval)
	if err := f.coord.Recover(); err != nil {
		return err
	}

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

func (f *Q5Filter) Stop() {}

// Private methods

func (f *Q5Filter) encodeAndSendBatch(clientID uuid.UUID, msgType protocol.MsgType, payload []byte, batchLength int, id protocol.MsgID) error {
	slog.Debug("Sending batch to broker:", "batchSize", batchLength, "clientId", clientID, "msgType", msgType)
	if err := f.pub.PublishInternalWithID(clientID, msgType, broker.KeyNil, payload, id); err != nil {
		return err
	}
	f.sentCount[clientID] += batchLength
	return nil
}

func (f *Q5Filter) handleTransactionMessage(envelope protocol.InternalEnvelope) error {
	transactions, err := f.pub.DecodeTransactionBatch(envelope.Payload)
	if err != nil {
		slog.Error("Error decoding transactions batch", "error", err)
		return err
	}
	slog.Debug("Received transaction batch", "clientId", envelope.ClientId, "batchSize", len(transactions))
	f.receivedCount[envelope.ClientId] += len(transactions)
	filteredTx := make([]protocol.Transaction, 0, len(transactions))
	for _, tx := range transactions {
		if filterTransaction(tx, f.Type, f.Operator, f.ValueFloat, f.ValueStrings) {
			filteredTx = append(filteredTx, tx)
		}
	}
	if len(filteredTx) == 0 {
		slog.Debug("No transactions passed the filter", "clientId", envelope.ClientId)
		return nil
	}
	filteredTxBytes, err := f.pub.EncodeTransactionBatch(filteredTx)
	if err != nil {
		return fmt.Errorf("encoding filtered transaction batch: %w", err)
	}
	txID := protocol.DeriveMsgID(envelope.MsgID, string(broker.KeyNil), 0)
	return f.encodeAndSendBatch(envelope.ClientId, protocol.MsgTransactionsBatch, filteredTxBytes, len(filteredTx), txID)
}

func (f *Q5Filter) handleEOFMessage(envelope protocol.InternalEnvelope) error {
	clientID := envelope.ClientId
	counts, err := f.pub.DecodeEOFCounts(envelope.Payload)
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

		eofPayload, err := f.pub.EncodeEOFCounts(map[broker.KeyType]int{broker.KeyNil: f.sentCount[clientID]})
		if err != nil {
			return fmt.Errorf("encoding EOF counts for EOF envelope: %w", err)
		}
		eofID := protocol.StageMsgID(clientID, fmt.Sprintf("%s#%d", f.cfg.WorkerPrefix, f.cfg.WorkerID), "eof", 0)
		if err := f.pub.PublishInternalWithID(clientID, protocol.MsgTransactionsEOF, broker.KeyControlEOF, eofPayload, eofID); err != nil {
			return fmt.Errorf("sending EOF message to broker: %w", err)
		}
		delete(f.workerEofReceived, clientID)
		delete(f.receivedCount, clientID)
		delete(f.sentCount, clientID)
		delete(f.expectedPreviousCount, clientID)
	}
	return f.coord.Flush()
}

func (f *Q5Filter) handleMessage(msg broker.Message) (uuid.UUID, protocol.MsgType, error) {
	clientID, msgType, err := f.pub.Dispatch(msg, map[protocol.MsgType]messaging.Handler{
		protocol.MsgTransactionsBatch: f.handleTransactionMessage,
		protocol.MsgTransactionsEOF:   f.handleEOFMessage,
	})
	return clientID, msgType, err
}
