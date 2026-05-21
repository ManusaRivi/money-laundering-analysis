package gateway

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/network"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external/codec"
	"github.com/ManusaRivi/money-laundering-analysis/src/gateway/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/gateway/internal/clientregistry"
	"github.com/ManusaRivi/money-laundering-analysis/src/gateway/internal/messagehandler"
)

type Gateway struct {
	config         *config.GatewayConfig
	registry       clientregistry.ClientRegistry
	inputQueue     broker.Broker
	outputExchange broker.Broker
	listener       net.Listener
	running        atomic.Bool
	binaryCodec    *codec.BinaryCodec
}

func NewGateway(config *config.GatewayConfig) (*Gateway, error) {
	connSettings := broker.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := broker.CreateQueueBroker(config.OutputQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchange, err := broker.CreateExchangeBroker(config.InputQueueName, []string{}, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	listener, err := net.Listen("tcp", config.ServerHost+":"+config.ServerPort)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}

	gateway := &Gateway{outputExchange: outputExchange, inputQueue: inputQueue, listener: listener, binaryCodec: codec.New()}
	gateway.running.Store(true)
	return gateway, nil
}

func (gateway *Gateway) Run() error {
	defer gateway.listener.Close()

	// No exchange is created yet
	/* go gateway.outputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		gateway.handleClientResponse(msg, ack, nack)
	}) */
	go gateway.handleSignals()

	slog.Info("Accepting connections...")

	for {
		conn, err := gateway.listener.Accept()
		if err != nil {
			if !gateway.running.Load() {
				break
			}
			return err
		}

		slog.Info("Client connected...")

		handler := messagehandler.NewMessageHandler()
		client := clientregistry.ClientState{Conn: network.NewConnection(conn), Handler: &handler}
		gateway.registry.Add(client)

		go gateway.handleClientRequest(client)
	}

	gateway.outputExchange.StopConsuming()
	gateway.registry.WithLock(func(clients []clientregistry.ClientState) {
		for _, client := range clients {
			client.Conn.Close()
		}
	})
	return nil
}

func (gateway *Gateway) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	gateway.running.Store(false)
	gateway.listener.Close()
}

func (gateway *Gateway) handleClientRequest(client clientregistry.ClientState) {
loop:
	for {
		msg, err := client.Conn.Receive(codec.HeaderSize)
		if err != nil {
			slog.Error("Error receiving message", "err", err)
			break
		}

		msgType, payloadSize := codec.DecodeHeader(msg)

		payload, err := client.Conn.Receive(int(payloadSize))
		if err != nil {
			slog.Error("Error receiving message payload", "err", err)
			break
		}

		switch msgType {
		case external.MsgTransactionsBatch:
			transactions, err := gateway.binaryCodec.DecodeTransactionBatch(payload)
			if err != nil {
				slog.Error("Error decoding transaction batch", "err", err)
				break loop
			}
			client.Handler.HandleTransactionsBatch(transactions)
		}
	}
}

func (gateway *Gateway) handleClientResponse(msg broker.Message, ack func(), nack func()) {
	// TODO: Implement me!
}

func (gateway *Gateway) handleEndOfRecordsMessage(client clientregistry.ClientState) error {
	// TODO: Implement me!
	return nil
}
