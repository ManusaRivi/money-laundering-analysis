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
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external/codec"
)

type Client struct {
	config             *config.ClientConfig
	sender             *Sender
	receiver           *Receiver
	running            atomic.Bool
	conn               network.Connection
	AccountsStream     data.AccountStream
	TransactionsStream data.TransactionStream
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
	receiver := NewReceiver(&connection, codec, config.OutputPath)

	accountsReader, err := data.NewBatchReader(
		config.AccountsDatasetPath,
		config.AccountBatchSize,
		data.ParseAccount,
	)
	if err != nil {
		return nil, err
	}

	accountsStream := data.NewAccountStream(accountsReader, codec)

	transactionsReader, err := data.NewBatchReader(
		config.TransactionsDatasetPath,
		config.TransactionBatchSize,
		data.ParseTransaction,
	)
	if err != nil {
		return nil, err
	}

	transactionsStream := data.NewTransactionStream(transactionsReader, codec)

	return &Client{
		config:             config,
		sender:             sender,
		receiver:           receiver,
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

// Client streams accounts dataset first, the streams transactions dataset.
// Meanwhile, the receiver goroutine listens for results from the server
// and writes them to the corresponding output file, depending on the msgType.
func (c *Client) Start() error {
	defer func() {
		c.conn.Close()
		c.AccountsStream.Close()
		c.TransactionsStream.Close()
	}()

	start := time.Now()

	go c.handleSignals()

	go c.receiver.Listen()

	if err := c.sender.StreamDataset(&c.TransactionsStream); err != nil {
		return err
	}

	if err := c.sender.StreamDataset(&c.AccountsStream); err != nil {
		return err
	}

	slog.Info("Finished streaming datasets, waiting for results...")

	<-c.receiver.Done()

	end := time.Now()
	slog.Info("Client finished", "duration", end.Sub(start))

	return nil
}
