package gateway

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/middleware"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
	"github.com/ManusaRivi/money-laundering-analysis/src/gateway/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/gateway/internal/clientmanagement/clientconnection"
	"github.com/ManusaRivi/money-laundering-analysis/src/gateway/internal/clientmanagement/clientregistry"
)

type Gateway struct {
	config         *config.GatewayConfig
	registry       clientregistry.ClientRegistry
	inputQueue     middleware.Middleware
	outputExchange middleware.Middleware
	listener       net.Listener
	running        atomic.Bool
	binaryCodec    *codec.BinaryCodec
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

		client := clientconnection.NewClientConnection(conn, gateway.binaryCodec)
		gateway.registry.Add(client)

		go func() {
			defer gateway.registry.Remove(client)
			client.Run()
		}()
	}

	gateway.outputExchange.StopConsuming()
	gateway.registry.WithLock(func(clients []*clientconnection.ClientConnection) {
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
