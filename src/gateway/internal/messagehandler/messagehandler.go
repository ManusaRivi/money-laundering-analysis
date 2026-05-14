package messagehandler

import "github.com/ManusaRivi/money-laundering-analysis/src/common/middleware"

type MessageHandler struct {
}

// TODO: Definir interfaz

func NewMessageHandler() MessageHandler {
	return MessageHandler{}
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
