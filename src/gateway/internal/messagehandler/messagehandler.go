package messagehandler

import (
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/middleware"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
)

type MessageHandler struct {
}

// TODO: Definir interfaz

func NewMessageHandler() MessageHandler {
	return MessageHandler{}
}

func (messageHandler *MessageHandler) HandleTransactionsBatch(transactions []protocol.Transaction) {
	for _, transaction := range transactions {
		slog.Debug("Handling transaction", "transaction - account paid", transaction.AmountPaid)
	}
}

func (messageHandler *MessageHandler) SerializeDataMessage() (*middleware.Message, error) {
	// TODO: Implement me!
	return nil, nil
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	// TODO: Implement me!
	return nil, nil
}

func (messageHandler *MessageHandler) DeserializeResultMessage() {
	// TODO: Implement me!
}
