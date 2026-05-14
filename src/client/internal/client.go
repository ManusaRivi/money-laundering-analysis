package client

import (
	"log/slog"
	"net"
	"time"

	"github.com/ManusaRivi/money-laundering-analysis/src/client/config"
)

type Client struct {
	config *config.ClientConfig
	conn   net.Conn
}

func NewClient(config *config.ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.Server.Host,
		config.Server.Port,
		config.Server.ConnectionAttempts,
		config.Server.ConnectionAttemptDelayMs,
	)

	if err != nil {
		return nil, err
	}
	return &Client{
		config: config,
		conn:   conn,
	}, nil
}

func connectToServer(host, port string, connectionAttempts int, connectionAttemptDelayMs int) (net.Conn, error) {
	var err error
	var conn net.Conn

	for range connectionAttempts {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			slog.Warn("Retrying connection...")
			time.Sleep(time.Duration(connectionAttemptDelayMs) * time.Millisecond)
			continue
		}
		break
	}

	return conn, err
}

// Add methods for the Client as necessary
func (c *Client) Start() error {
	// Implement the functionality here
	return nil
}

func (c *Client) Stop() {
	// Implement the functionality here
}
