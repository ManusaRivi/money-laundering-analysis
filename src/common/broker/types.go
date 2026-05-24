package broker

const (
	TypeQueueToQueue       = "q-q"
	TypeQueueToExchange    = "q-e"
	TypeExchangeToQueue    = "e-q"
	TypeExchangeToExchange = "e-e"
)

func IsInputExchangeType(brokerType string) bool {
	return brokerType == TypeExchangeToQueue || brokerType == TypeExchangeToExchange
}

func IsOutputExchangeType(brokerType string) bool {
	return brokerType == TypeQueueToExchange || brokerType == TypeExchangeToExchange
}
