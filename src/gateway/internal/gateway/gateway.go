package gateway

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external/codec"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
	"github.com/ManusaRivi/money-laundering-analysis/src/gateway/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/gateway/internal/clientmanagement/clientconnection"
	"github.com/ManusaRivi/money-laundering-analysis/src/gateway/internal/clientmanagement/clientregistry"
	"github.com/google/uuid"
)

type Gateway struct {
	config   *config.GatewayConfig
	registry *clientregistry.ClientRegistry
	broker   broker.Broker
	listener net.Listener
	running  atomic.Bool
	codec    codec.Codec
}

func NewGateway(config *config.GatewayConfig) (*Gateway, error) {
	broker, err := broker.NewBroker(config.BrokerConfig)
	if err != nil {
		return nil, err
	}

	listener, err := net.Listen("tcp", config.ServerHost+":"+config.ServerPort)
	if err != nil {
		slog.Error("Error creating listener", "err", err)
		return nil, err
	}

	registry := clientregistry.NewClientRegistry()

	gateway := &Gateway{registry: &registry, broker: broker, listener: listener, codec: codec.New()}
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

func (gateway *Gateway) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	gateway.running.Store(false)
	gateway.listener.Close()
}

func (gateway *Gateway) HandleClientRequest(c *clientconnection.ClientConnection) bool {
	for {
		header, err := c.Conn.Receive(codec.HeaderSize)
		if err != nil {
			slog.Error("Error receiving header", "err", err)
			return false
		}

		msgType, payloadSize := codec.DecodeHeader(header)

		payload, err := c.Conn.Receive(int(payloadSize))
		if err != nil {
			slog.Error("Error receiving payload", "err", err)
			return false
		}

		switch msgType {
		case external.MsgAccountsBatch:
			accounts, err := gateway.codec.DecodeAccountBatch(payload)
			if err != nil {
				slog.Error("decoding account batch", "error", err)
				return false
			}
			// Since our internal protocol handles single account messages, we're sending
			// one message for each account in the inbound batch.
			for _, account := range accounts {
				bankInfo := domain.BankInfo{
					ID:            account.BankID,
					Name:          account.BankName,
					AccountNumber: account.AccountNumber,
					EntityID:      account.EntityID,
					EntityName:    account.EntityName,
				}
				_, err := inner.MarshalBankInfoPacket(c.ClientId, bankInfo)
				// TODO: Send to Join worker directly.
				if err != nil {
					slog.Error("Error marshalling bank info packet", "error", err)
					return false
				}
				// gateway.broker.Send(msg)
				// ACK to Client would be sent here.
			}
		case external.MsgAccountsEOF:
			// This case will be potentially deprecated.
			slog.Debug("Received accounts EOF")
			c.Handler.HandleAccountsEOF()
		case external.MsgTransactionsBatch:
			transactions, err := gateway.codec.DecodeTransactionBatch(payload)
			if err != nil {
				slog.Error("decoding transaction batch", "error", err)
				return false
			}
			for _, transaction := range transactions {
				tx := domain.Transaction{
					Timestamp: transaction.Timestamp,
					Origin: domain.Account{
						BankID: transaction.FromBank,
						ID:     transaction.FromAccount,
					},
					Dest: domain.Account{
						BankID: transaction.ToBank,
						ID:     transaction.ToAccount,
					},
					Paid: domain.Money{
						Amount:   transaction.AmountPaid,
						Currency: transaction.PaymentCurrency,
					},
					Received: domain.Money{
						Amount:   transaction.AmountReceived,
						Currency: transaction.ReceivingCurrency,
					},
					Format: transaction.PaymentFormat,
				}
				msg, err := inner.MarshalTransactionPacket(c.ClientId, tx)
				if err != nil {
					slog.Error("Error marshalling transaction packet", "error", err)
					return false
				}
				if err := gateway.broker.Send(msg); err != nil {
					slog.Error("Error sending transaction packet to broker", "error", err)
					// NACK to Client would be sent here.
					return false
				}
				// ACK to Client would be sent here.
			}
		case external.MsgTransactionsEOF:
			slog.Debug("Received transactions EOF")
			msg, err := inner.MarshalEOFPacket(c.ClientId)
			if err != nil {
				slog.Error("Error marshalling EOF packet", "error", err)
				return false
			}
			if err := gateway.broker.Send(msg); err != nil {
				slog.Error("Error sending EOF packet to broker", "error", err)
				// NACK to Client would be sent here.
				return false
			}
			// ACK to Client would be sent here.
			return true
		default:
			slog.Warn("Unknown message type received", "msgType", msgType)
			return false
		}
	}
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
