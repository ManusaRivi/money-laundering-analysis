package gateway

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
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
	// Per-stream monotonic sequences used to mint deterministic source MsgIDs.
	// Transactions and accounts keep independent spaces. Mutated only from this
	// client's connection goroutine.
	txSeq  uint64
	accSeq uint64

	accountsEOFSent     bool
	transactionsEOFSent bool
}

type Gateway struct {
	config         *config.GatewayConfig
	registry       *clientregistry.ClientRegistry
	broker         broker.Broker
	accountsBroker broker.Broker
	listener       net.Listener
	running        atomic.Bool
	codec          codec.Codec
	clientsMu      sync.Mutex
	clients        map[uuid.UUID]*Client
	// seenResults dedups inbound results by (client, MsgID). The gateway is the
	// single-replica sink, so it sees every copy of a given id and its dedup is
	// sound — this is the dedup point for paths whose last stage is a round-robin
	// replica set (e.g. the Q1 filters), where a worker restart redelivers and
	// re-emits the same deterministic-id result. Only touched from the broker
	// consume goroutine (forwardResponse), so it needs no lock.
	seenResults map[uuid.UUID]map[protocol.MsgID]struct{}
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

	gateway := &Gateway{registry: &registry, broker: mainBroker, accountsBroker: accountsBroker, listener: listener, codec: codec.New(), clients: make(map[uuid.UUID]*Client), seenResults: make(map[uuid.UUID]map[protocol.MsgID]struct{})}
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
		gateway.clientsMu.Lock()
		gateway.clients[clientId] = &Client{
			ID:           clientId,
			tx_count:     0,
			tx_usd_count: 0,
		}
		gateway.clientsMu.Unlock()

		go func() {
			defer gateway.forgetClient(clientId)
			defer gateway.registry.Remove(client)
			defer client.Conn.Close()

			if gateway.HandleClientRequest(client) {
				<-client.Done()
			} else {
				gateway.handleClientDisconnect(client)
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
		case protocol.MsgAccountsBatch:
			if !gateway.handleAccountsBatch(c, payload) {
				return false
			}
		case protocol.MsgAccountsEOF:
			if !gateway.handleAccountsEOF(c) {
				return false
			}
		case protocol.MsgTransactionsBatch:
			if !gateway.handleTransactionsBatch(c, payload) {
				return false
			}
		case protocol.MsgTransactionsEOF:
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

func (gateway *Gateway) getClient(clientId uuid.UUID) *Client {
	gateway.clientsMu.Lock()
	defer gateway.clientsMu.Unlock()
	return gateway.clients[clientId]
}

func (gateway *Gateway) forgetClient(clientId uuid.UUID) {
	gateway.clientsMu.Lock()
	delete(gateway.clients, clientId)
	gateway.clientsMu.Unlock()
}

// nextSourceMsgID generates the id of the next outbound message of clientId on the stream implied by msgType,
// advancing that stream's per-client sequence. Transactions and accounts keep independent sequences so
// their id spaces can't collide.
func (gateway *Gateway) nextSourceMsgID(clientId uuid.UUID, msgType protocol.MsgType) protocol.MsgID {
	stream := protocol.StreamTransactions
	if msgType == protocol.MsgAccountsBatch || msgType == protocol.MsgAccountsEOF {
		stream = protocol.StreamAccounts
	}
	var seq uint64
	if client := gateway.getClient(clientId); client != nil {
		if stream == protocol.StreamAccounts {
			seq = client.accSeq
			client.accSeq++
		} else {
			seq = client.txSeq
			client.txSeq++
		}
	}
	return protocol.SourceMsgID(clientId, stream, seq)
}

func (gateway *Gateway) sendMessageToBroker(msgType protocol.MsgType, clientId uuid.UUID, payload []byte, routingKey broker.KeyType) bool {
	envelope, err := gateway.codec.EncodeInternalEnvelope(protocol.InternalEnvelope{
		MsgType:  msgType,
		ClientId: clientId,
		MsgID:    gateway.nextSourceMsgID(clientId, msgType),
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
	if msgType == protocol.MsgAccountsBatch || msgType == protocol.MsgAccountsEOF {
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
	return gateway.sendMessageToBroker(protocol.MsgAccountsBatch, c.ClientId, payload, broker.KeyNil)
}

func (gateway *Gateway) handleAccountsEOF(c *clientconnection.ClientConnection) bool {
	slog.Debug("Received accounts EOF")
	if !gateway.sendMessageToBroker(protocol.MsgAccountsEOF, c.ClientId, nil, broker.KeyNil) {
		return false
	}
	if client := gateway.getClient(c.ClientId); client != nil {
		client.accountsEOFSent = true
	}
	return true
}

func (gateway *Gateway) sendTransactionBatch(c *clientconnection.ClientConnection, transactions []protocol.Transaction, routingKey broker.KeyType) bool {
	if len(transactions) == 0 {
		return true
	}
	slog.Debug("Sending transactions batch to broker", "batchSize", len(transactions), "routingKey", routingKey)
	txPayload, err := gateway.codec.EncodeTransactionBatch(transactions)
	if err != nil {
		slog.Error("Error marshalling transaction packet", "error", err)
		return false
	}
	return gateway.sendMessageToBroker(protocol.MsgTransactionsBatch, c.ClientId, txPayload, routingKey)
}

func (gateway *Gateway) handleTransactionsBatch(c *clientconnection.ClientConnection, payload []byte) bool {
	slog.Debug("Received transactions batch")
	transactions, err := gateway.codec.DecodeTransactionBatch(payload)
	if err != nil {
		slog.Error("decoding transaction batch", "error", err)
		return false
	}
	dollarTx := make([]protocol.Transaction, 0)
	nonDollarTx := make([]protocol.Transaction, 0)
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
	if client := gateway.getClient(c.ClientId); client != nil {
		client.tx_count += len(transactions)
		client.tx_usd_count += len(dollarTx)
	}
	// ACK to Client would be sent here?
	return true
}

func (gateway *Gateway) handleTransactionsEOF(c *clientconnection.ClientConnection) bool {
	slog.Debug("Received transactions EOF")
	client := gateway.getClient(c.ClientId)
	if client == nil {
		return false
	}
	tx_count := client.tx_count
	tx_usd_count := client.tx_usd_count

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
	if !gateway.sendMessageToBroker(protocol.MsgTransactionsEOF, c.ClientId, eofPayload, broker.KeyControlEOF) {
		return false
	}
	client.transactionsEOFSent = true
	return true
}

func (gateway *Gateway) handleClientDisconnect(c *clientconnection.ClientConnection) {
	client := gateway.getClient(c.ClientId)
	if client == nil {
		return
	}
	slog.Info("Client disconnected before finishing, synthesizing EOFs to release downstream state", "clientId", c.ClientId)
	if !client.accountsEOFSent {
		gateway.handleAccountsEOF(c)
	}
	if !client.transactionsEOFSent {
		gateway.handleTransactionsEOF(c)
	}
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

	// A worker restart can redeliver an un-acked input on a round-robin queue,
	// so the same deterministic-id result is re-emitted (e.g. Q1 filters). Drop
	// the duplicate here. Mirror Dispatch: only ids that opt into dedup (non-zero
	// MsgID) are tracked; zero-id results (e.g. the join's Q2 output) are
	// forwarded as-is and rely on their own upstream dedup point.
	deduped := envelope.MsgID != (protocol.MsgID{})
	if deduped {
		if seen := gateway.seenResults[envelope.ClientId]; seen != nil {
			if _, ok := seen[envelope.MsgID]; ok {
				slog.Debug("Dropping already-forwarded result", "clientId", envelope.ClientId, "msgType", envelope.MsgType)
				ack()
				return
			}
		}
	}

	var client *clientconnection.ClientConnection
	gateway.registry.WithLock(func(clients map[uuid.UUID]*clientconnection.ClientConnection) {
		client = clients[envelope.ClientId]
	})
	if client == nil {
		// Cliente desconectado: descartar en lugar de requeuear infinitamente.
		slog.Warn("Dropping result for unknown client", "clientId", envelope.ClientId, "msgType", envelope.MsgType)
		delete(gateway.seenResults, envelope.ClientId)
		ack()
		return
	}

	if err := client.ForwardEnvelope(protocol.ExternalEnvelope{
		MsgType: envelope.MsgType,
		Payload: envelope.Payload,
	}); err != nil {
		slog.Error("Error forwarding binary result to client", "clientId", envelope.ClientId, "err", err)
		nack()
		return
	}
	if deduped {
		seen := gateway.seenResults[envelope.ClientId]
		if seen == nil {
			seen = make(map[protocol.MsgID]struct{})
			gateway.seenResults[envelope.ClientId] = seen
		}
		seen[envelope.MsgID] = struct{}{}
	}
	if client.AllEOFSent() {
		delete(gateway.seenResults, envelope.ClientId)
	}
	ack()
}

func (gateway *Gateway) handleClientResponse(msg broker.Message, ack, nack func()) {
	if msg.ContentType == broker.ContentTypeBinary {
		gateway.forwardResponse(msg, ack, nack)
		return
	}
}
