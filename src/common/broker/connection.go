package broker

import (
	"fmt"
	"net/url"

	amqp "github.com/rabbitmq/amqp091-go"
)

func connectRabbit(rawURL string) (*amqp.Connection, *amqp.Channel, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid rabbitmq url: %w", err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, nil, fmt.Errorf("invalid rabbitmq url: %s", rawURL)
	}

	conn, err := amqp.Dial(rawURL)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to rabbitmq: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("failed to open channel: %w", err)
	}

	return conn, channel, nil
}
