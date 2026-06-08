package gateway

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external/codec"
	"github.com/ManusaRivi/money-laundering-analysis/src/gateway/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/gateway/internal/clientmanagement/clientconnection"
	"github.com/ManusaRivi/money-laundering-analysis/src/gateway/internal/clientmanagement/clientregistry"
	"github.com/google/uuid"
)

const Dollar = "US Dollar"

type Client struct {
	ID           uuid.UUID
	tx_count     int
	tx_usd_count int
}

type Gateway struct {
	config         *config.GatewayConfig
	registry       *clientregistry.ClientRegistry
	broker         broker.Broker
	accountsBroker broker.Broker
	listener       net.Listener
	running        atomic.Bool
	codec          codec.Codec
	clients        map[uuid.UUID]*Client
}

func NewGateway(config *config.GatewayConfig) (*Gateway, error) {
	mainBroker, err := broker.NewBroker(config.BrokerConfig)
	if err != nil {
		return nil, err
	}

	var accountsBroker broker.Broker
	if config.AccountsBrokerConfig != nil && config.AccountsBrokerConfig.Type != "" {
		accountsBroker, err = broker.NewBroker(*config.AccountsBrokerConfig)
		if err != nil {
			return nil, err
		}
	}

	listener, err := net.Listen("tcp", config.ServerHost+":"+config.ServerPort)
	if err != nil {
		slog.Error("Error creating listener", "err", err)
		return nil, err
	}

	registry := clientregistry.NewClientRegistry()

	gateway := &Gateway{registry: &registry, broker: mainBroker, accountsBroker: accountsBroker, listener: listener, codec: codec.New(), clients: make(map[uuid.UUID]*Client)}
	gateway.running.Store(true)
	return gateway, nil
}

func (gateway *Gateway) Run() error {
	defer func() {
		gateway.listener.Close()
		gateway.broker.StopConsuming()
	}()
	go gateway.broker.StartConsuming(func(msg broker.Message, ack, nack func()) {
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

		clientId := uuid.New()

		client := clientconnection.NewClientConnection(clientId, conn, gateway.codec)
		gateway.registry.Add(client)

		// Initialize client stats
		gateway.clients[clientId] = &Client{
			ID:           clientId,
			tx_count:     0,
			tx_usd_count: 0,
		}

		go func() {
			defer gateway.registry.Remove(client)
			defer client.Conn.Close()

			if gateway.HandleClientRequest(client) {
				<-client.Done()
			}
		}()
	}

	gateway.broker.StopConsuming()
	gateway.registry.WithLock(func(clients map[uuid.UUID]*clientconnection.ClientConnection) {
		for _, client := range clients {
			client.Conn.Close()
		}
	})
	return nil
}

func (gateway *Gateway) HandleClientRequest(c *clientconnection.ClientConnection) bool {
	for {
		header, err := c.Conn.Receive(codec.ExternalHeaderSize)
		if err != nil {
			slog.Error("Error receiving header", "err", err)
			return false
		}

		msgType, payloadSize := codec.DecodeExternalHeader(header)

		payload, err := c.Conn.Receive(int(payloadSize))
		if err != nil {
			slog.Error("Error receiving payload", "err", err)
			return false
		}

		switch msgType {
		case external.MsgAccountsBatch:
			if !gateway.handleAccountsBatch(c, payload) {
				return false
			}
		case external.MsgAccountsEOF:
			if !gateway.handleAccountsEOF(c) {
				return false
			}
		case external.MsgTransactionsBatch:
			if !gateway.handleTransactionsBatch(c, payload) {
				return false
			}
		case external.MsgTransactionsEOF:
			return gateway.handleTransactionsEOF(c)
		default:
			slog.Warn("Unknown message type received", "msgType", msgType)
			return false
		}
	}
}

// Private methods

func (gateway *Gateway) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	gateway.running.Store(false)
	gateway.listener.Close()
}

func (gateway *Gateway) sendMessageToBroker(msgType external.MsgType, clientId uuid.UUID, payload []byte, routingKey broker.KeyType) bool {
	envelope, err := gateway.codec.EncodeInternalEnvelope(external.InternalEnvelope{
		MsgType:  msgType,
		ClientId: clientId,
		Payload:  payload,
	})
	if err != nil {
		slog.Error("Error encoding internal envelope", "error", err)
		return false
	}
	brokerMsg := broker.Message{
		RoutingKey:  routingKey,
		ContentType: broker.ContentTypeBinary,
		Body:        envelope,
	}
	if msgType == external.MsgAccountsBatch || msgType == external.MsgAccountsEOF {
		err = gateway.accountsBroker.Send(brokerMsg)
	} else {
		err = gateway.broker.Send(brokerMsg)
	}
	if err != nil {
		slog.Error("Error sending message to broker", "error", err)
		return false
	}
	return true
}

func (gateway *Gateway) handleAccountsBatch(c *clientconnection.ClientConnection, payload []byte) bool {
	slog.Debug("Received accounts batch")
	return gateway.sendMessageToBroker(external.MsgAccountsBatch, c.ClientId, payload, broker.KeyNil)
}

func (gateway *Gateway) handleAccountsEOF(c *clientconnection.ClientConnection) bool {
	slog.Debug("Received accounts EOF")
	return gateway.sendMessageToBroker(external.MsgAccountsEOF, c.ClientId, nil, broker.KeyNil)
}

func (gateway *Gateway) sendTransactionBatch(c *clientconnection.ClientConnection, transactions []external.Transaction, routingKey broker.KeyType) bool {
	if len(transactions) == 0 {
		return true
	}
	slog.Debug("Sending transactions batch to broker", "batchSize", len(transactions), "routingKey", routingKey)
	txPayload, err := gateway.codec.EncodeTransactionBatch(transactions)
	if err != nil {
		slog.Error("Error marshalling transaction packet", "error", err)
		return false
	}
	return gateway.sendMessageToBroker(external.MsgTransactionsBatch, c.ClientId, txPayload, routingKey)
}

func (gateway *Gateway) handleTransactionsBatch(c *clientconnection.ClientConnection, payload []byte) bool {
	slog.Debug("Received transactions batch")
	transactions, err := gateway.codec.DecodeTransactionBatch(payload)
	if err != nil {
		slog.Error("decoding transaction batch", "error", err)
		return false
	}
	dollarTx := make([]external.Transaction, 0)
	nonDollarTx := make([]external.Transaction, 0)
	for _, transaction := range transactions {
		if transaction.PaymentCurrency == Dollar {
			dollarTx = append(dollarTx, transaction)
		} else {
			nonDollarTx = append(nonDollarTx, transaction)
		}
	}
	if !gateway.sendTransactionBatch(c, dollarTx, broker.KeyDollarTransaction) {
		return false
	}
	if !gateway.sendTransactionBatch(c, nonDollarTx, broker.KeyNonDollarTransaction) {
		return false
	}
	gateway.clients[c.ClientId].tx_count += len(transactions)
	gateway.clients[c.ClientId].tx_usd_count += len(dollarTx)
	// ACK to Client would be sent here?
	return true
}

func (gateway *Gateway) handleTransactionsEOF(c *clientconnection.ClientConnection) bool {
	slog.Debug("Received transactions EOF")
	tx_count := gateway.clients[c.ClientId].tx_count
	tx_usd_count := gateway.clients[c.ClientId].tx_usd_count

	eofPayload, err := gateway.codec.EncodeEOFCounts(map[broker.KeyType]int{
		broker.KeyNil:                  tx_count,
		broker.KeyDollarTransaction:    tx_usd_count,
		broker.KeyNonDollarTransaction: tx_count - tx_usd_count,
		broker.KeyAllTransaction:       tx_count,
	},
	)
	if err != nil {
		slog.Error("Error marshalling EOF packet", "error", err)
		return false
	}
	return gateway.sendMessageToBroker(external.MsgTransactionsEOF, c.ClientId, eofPayload, broker.KeyControlEOF)
}

/*
Decodes the envelope without decoding the actual payload.
Checks the client ID, finds that particular connection, and calls
the forwarding method. Reencodes an external envelope and sends it to the client.
*/
func (gateway *Gateway) forwardResponse(msg broker.Message, ack, nack func()) {
	envelope, err := gateway.codec.DecodeInternalEnvelope(msg.Body)
	if err != nil {
		// Mensaje malformado: requeuearlo solo repetiría el error para siempre.
		slog.Error("Dropping malformed binary result message", "err", err)
		ack()
		return
	}

	/* 	if !isValidResultType(envelope.MsgType) {
		// if MsgType ==
	} */

	var client *clientconnection.ClientConnection
	gateway.registry.WithLock(func(clients map[uuid.UUID]*clientconnection.ClientConnection) {
		client = clients[envelope.ClientId]
	})
	if client == nil {
		// Cliente desconectado: descartar en lugar de requeuear infinitamente.
		slog.Warn("Dropping result for unknown client", "clientId", envelope.ClientId, "msgType", envelope.MsgType)
		ack()
		return
	}

	if err := client.ForwardEnvelope(external.ExternalEnvelope{
		MsgType: envelope.MsgType,
		Payload: envelope.Payload,
	}); err != nil {
		slog.Error("Error forwarding binary result to client", "clientId", envelope.ClientId, "err", err)
		nack()
		return
	}
	ack()
}

func (gateway *Gateway) handleClientResponse(msg broker.Message, ack, nack func()) {
	if msg.ContentType == broker.ContentTypeBinary {
		gateway.forwardResponse(msg, ack, nack)
		return
	}
}
