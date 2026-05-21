package client

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ManusaRivi/money-laundering-analysis/src/client/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/client/internal/data"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/network"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
)

const BatchAmount = 3

type Client struct {
	config             *config.ClientConfig
	sender             *Sender
	running            atomic.Bool
	conn               network.Connection
	AccountsStream     accountStream
	TransactionsStream transactionStream
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

	codec := codec.New()

	connection := network.NewConnection(conn)

	sender := NewSender(&connection, codec)

	accountsReader, err := data.NewBatchReader(
		config.AccountsDatasetPath,
		BatchAmount,
		data.ParseAccount,
	)
	if err != nil {
		return nil, err
	}

	accountsStream := NewAccountStream(accountsReader, codec)

	transactionsReader, err := data.NewBatchReader(
		config.TransactionsDatasetPath,
		BatchAmount,
		data.ParseTransaction,
	)
	if err != nil {
		return nil, err
	}

	transactionsStream := NewTransactionStream(transactionsReader, codec)

	return &Client{
		config:             config,
		sender:             sender,
		conn:               connection,
		AccountsStream:     *accountsStream,
		TransactionsStream: *transactionsStream,
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

func (client *Client) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	client.running.Store(false)
	client.conn.Close()
}

// Add methods for the Client as necessary
func (c *Client) Start() error {
	defer c.conn.Close()
	go c.handleSignals()

	c.sender.Stream(&c.AccountsStream)
	c.sender.Stream(&c.TransactionsStream)

	return nil
}

func (c *Client) Stop() {
	// Implement the functionality here
}
