package gateway

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/middleware"
	"github.com/ManusaRivi/money-laundering-analysis/src/gateway/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/gateway/internal/clientregistry"
	"github.com/ManusaRivi/money-laundering-analysis/src/gateway/internal/messagehandler"
)

type Gateway struct {
	config         *config.GatewayConfig
	registry       clientregistry.ClientRegistry
	inputQueue     middleware.Middleware
	outputExchange middleware.Middleware
	listener       net.Listener
	running        atomic.Bool
}

func NewGateway(config *config.GatewayConfig) (*Gateway, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.InputQueueName, []string{}, connSettings)
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

	gateway := &Gateway{outputExchange: outputExchange, inputQueue: inputQueue, listener: listener}
	gateway.running.Store(true)
	return gateway, nil
}

func (gateway *Gateway) Run() error {
	defer gateway.listener.Close()

	go gateway.outputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		gateway.handleClientResponse(msg, ack, nack)
	})
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
		client := clientregistry.ClientState{Conn: conn, Handler: &handler}
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
	// TODO: Implement me!
}

func (gateway *Gateway) handleClientResponse(msg middleware.Message, ack func(), nack func()) {
	// TODO: Implement me!
}

func (gateway *Gateway) handleEndOfRecordsMessage(client clientregistry.ClientState) error {
	// TODO: Implement me!
	return nil
}
