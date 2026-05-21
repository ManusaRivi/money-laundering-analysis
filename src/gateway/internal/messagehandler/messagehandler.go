package messagehandler

import (
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"
)

type MessageHandler struct {
}

// TODO: Definir interfaz

func NewMessageHandler() MessageHandler {
	return MessageHandler{}
}

func (messageHandler *MessageHandler) HandleTransactionsBatch(transactions []external.Transaction) {
	for _, transaction := range transactions {
		slog.Debug("Handling transaction", "transaction - account paid", transaction.AmountPaid)
	}
}

func (messageHandler *MessageHandler) SerializeDataMessage() (*broker.Message, error) {
	// TODO: Implement me!
	return nil, nil
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*broker.Message, error) {
	// TODO: Implement me!
	return nil, nil
}

func (messageHandler *MessageHandler) DeserializeResultMessage() {
	// TODO: Implement me!
}
