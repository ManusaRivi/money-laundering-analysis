package gateway

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external/codec"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
	"github.com/ManusaRivi/money-laundering-analysis/src/gateway/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/gateway/internal/clientmanagement/clientconnection"
	"github.com/ManusaRivi/money-laundering-analysis/src/gateway/internal/clientmanagement/clientregistry"
	"github.com/google/uuid"
)

type Gateway struct {
	config         *config.GatewayConfig
	registry       *clientregistry.ClientRegistry
	inputQueue     broker.Broker
	outputExchange broker.Broker
	listener       net.Listener
	running        atomic.Bool
	codec          codec.Codec
}

func NewGateway(config *config.GatewayConfig) (*Gateway, error) {
	connSettings := broker.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := broker.CreateQueueBroker(config.OutputQueueName, connSettings)
	if err != nil {
		slog.Error("Error creating input queue broker", "err", err)
		return nil, err
	}

	outputExchange, err := broker.CreateExchangeBroker(config.InputQueueName, []string{}, connSettings)
	if err != nil {
		inputQueue.Close()
		slog.Error("Error creating output exchange broker", "err", err)
		return nil, err
	}

	listener, err := net.Listen("tcp", config.ServerHost+":"+config.ServerPort)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		slog.Error("Error creating listener", "err", err)
		return nil, err
	}

	registry := clientregistry.NewClientRegistry()

	gateway := &Gateway{registry: &registry, outputExchange: outputExchange, inputQueue: inputQueue, listener: listener, codec: codec.New()}
	gateway.running.Store(true)
	return gateway, nil
}

func (gateway *Gateway) Run() error {
	defer gateway.listener.Close()

	/* go gateway.inputQueue.StartConsuming(func(msg broker.Message, ack, nack func()) {
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

		clientId := uuid.New()

		client := clientconnection.NewClientConnection(clientId, conn, gateway.codec)
		gateway.registry.Add(client)

		go func() {
			defer gateway.registry.Remove(client)
			client.Run()
		}()
	}

	gateway.outputExchange.StopConsuming()
	gateway.registry.WithLock(func(clients map[uuid.UUID]*clientconnection.ClientConnection) {
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

func (gateway *Gateway) handleClientResponse(msg broker.Message, ack, nack func()) {
	gateway.registry.WithLock(func(clients map[uuid.UUID]*clientconnection.ClientConnection) {
		packet, err := inner.UnmarshalPacket(msg)
		if err != nil {
			slog.Error("Error unmarshalling message", "err", err)
			nack()
			return
		}

		client, exists := clients[packet.ClientID]
		if !exists {
			slog.Error("No client found for response message", "clientId", packet.ClientID)
			nack()
			return
		}

		if err := client.HandleResponseMessage(packet); err != nil {
			slog.Error("Error handling client response message", "clientId", packet.ClientID, "err", err)
			nack()
			return
		}

		ack()
	})
}
