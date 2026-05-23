package broker

import (
	"fmt"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
)

type QueueBroker struct{}

func CreateQueueBroker(queueName string, connectionSettings ConnSettings) (Broker, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	return createQueueToQueueBroker(queueName, "", url, config.BrokerConfig{})
}
