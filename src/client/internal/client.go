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
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external/codec"
)

const BatchAmount = 3

type Client struct {
	config             *config.ClientConfig
	conn               network.Connection
	running            atomic.Bool
	TransactionsReader *data.BatchReader[external.Transaction]
	BinaryCodec        *codec.BinaryCodec
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

	transactionsReader, err := data.NewBatchReader(
		config.DatasetPath,
		BatchAmount,
		data.ParseTransaction,
	)
	if err != nil {
		return nil, err
	}

	return &Client{
		config:             config,
		conn:               *network.NewConnection(conn),
		TransactionsReader: transactionsReader,
		BinaryCodec:        codec.New(),
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

	for range BatchAmount {
		batch, err := c.TransactionsReader.Next()
		if err != nil {
			if err == os.ErrClosed {
				slog.Info("Batch reader closed, stopping client")
				return nil
			}
		}

		if len(batch) == 0 {
			slog.Info("No more transactions to read, stopping client")
			break
		}

		slog.Info("Read batch of transactions", "batch_size", len(batch))

		payload, err := c.BinaryCodec.EncodeTransactionBatch(batch)
		if err != nil {
			slog.Error("Error encoding batch", "err", err)
			return err
		}

		envelope, err := c.BinaryCodec.EncodeEnvelope(external.Envelope{
			MsgType: external.MsgTransactionsBatch,
			Payload: payload,
		})
		if err != nil {
			slog.Error("Error encoding envelope", "err", err)
			return err
		}

		if err := c.conn.Send(envelope); err != nil {
			slog.Error("Error sending batch", "err", err)
			return err
		}
	}

	return nil
}

func (c *Client) Stop() {
	// Implement the functionality here
}
