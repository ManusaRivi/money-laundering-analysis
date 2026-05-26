package eof

import (
	"log/slog"
	"sync"
	"time"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/google/uuid"
)

type client struct {
	clientID uuid.UUID
	msgRcvCount int // cantidad de mensajes que recibio este nodo
	msgSntCount int // cantidad de mensajes que envio este nodo al siguiente stage
	expectedTotal int // total_messages que espera recibir el cluster para flushear
	retryCount int // cantidad de reintentos de amount request

	rcvResponses map[int]int // senderID -> amount rcv reportado
	sntResponses map[int]int // senderID -> amount snt reportado
	flushResponses map[int]bool // senderID -> flush ack
	flushExpectedSent int
}

type SyncEOFController struct {
	broker *EOFBroker

	nodeID     int
	totalNodes int // La cantidad total de workers del mismo tipo

	mu            sync.Mutex
	clients	   map[uuid.UUID]*client

	// Callback a ejecutar cuando todos los workers terminan.
	// Se llama pasando el clientID
	onFlush func(clientID uuid.UUID) error

	// Callback para que el lider emita el EOF a la siguiente etapa.
	onLeaderFlush func(clientID uuid.UUID, finalSent int) error

	// Callback cuando el cliente supera el maximo de reintentos.
	onRetryExceeded func(clientID uuid.UUID) error

	retryBaseDelay time.Duration
	retryStepDelay time.Duration
	maxRetries     int
}

func NewClient(clientID uuid.UUID) *client {
	return &client{
		clientID: clientID,
		rcvResponses: make(map[int]int),
		sntResponses: make(map[int]int),
		flushResponses: make(map[int]bool),
	}
}

// NewSyncEOFController inicializa un nuevo SyncEOFController
func NewSyncEOFController(
	cfg config.SyncEOFControllerConfig,
	onFlush func(clientID uuid.UUID) error,
	onLeaderFlush func(clientID uuid.UUID, finalSent int) error,
	onRetryExceeded func(clientID uuid.UUID) error,
	) (*SyncEOFController, error) {	
	eofBroker, err := NewEOFBroker(cfg.RabbitURL, cfg.BroadcastExchange, cfg.WorkerID, cfg.EOFPrefix)
	if err != nil {
		return nil, err
	}

	retryBaseDelay := time.Duration(cfg.RetryBaseDelay * float64(time.Microsecond))
	retryStepDelay := time.Duration(cfg.RetryStepDelay * float64(time.Microsecond))

	controller := &SyncEOFController{
		broker:        eofBroker,
		nodeID:        cfg.WorkerID,
		totalNodes:    cfg.WorkerAmount,
		clients:      make(map[uuid.UUID]*client),
		onFlush:       onFlush,
		onLeaderFlush: onLeaderFlush,
		onRetryExceeded: onRetryExceeded,
		retryBaseDelay: retryBaseDelay,
		retryStepDelay: retryStepDelay,
		maxRetries:     cfg.MaxRetries,
	}

	slog.Debug("[SyncEOFController] Initialized",
		"worker_id", cfg.WorkerID,
		"total_nodes", cfg.WorkerAmount,
		"retry_base_delay", retryBaseDelay,
		"retry_step_delay", retryStepDelay,
		"max_retries", cfg.MaxRetries,
	)

	return controller, nil
}

// Start comienza a escuchar en el broker los mensajes de control de otros workers
func (c *SyncEOFController) Start() error {
	slog.Debug("[SyncEOFController] Start consuming", "worker_id", c.nodeID)
	return c.broker.StartConsuming(c.handleControlMessage)
}

// MessageReceived incrementa el contador de mensajes recibidos para un cliente dado.
// Se llama cada vez que este nodo recibe un mensaje de ese cliente.
func (c *SyncEOFController) MessageReceived(clientID uuid.UUID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.clients[clientID]; !exists {
		c.clients[clientID] = NewClient(clientID)
		slog.Debug("[SyncEOFController] Added client state", "client_id", clientID)
	}

	c.clients[clientID].msgRcvCount++
	slog.Debug("[SyncEOFController] Message received",
		"client_id", clientID,
		"received_count", c.clients[clientID].msgRcvCount,
	)
}

func (c *SyncEOFController) MessageSent(clientID uuid.UUID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.clients[clientID].msgSntCount++
	slog.Debug("[SyncEOFController] Message sent",
		"client_id", clientID,
		"sent_count", c.clients[clientID].msgSntCount,
	)
}

// SyncEof inicia el proceso de sincronizacion. Se llama cuando un nodo asume el rol de lider
func (c *SyncEOFController) SyncEof(clientID uuid.UUID, expectedTotalMsg int) {
	c.mu.Lock()
	client := c.clients[clientID]
	client.expectedTotal = expectedTotalMsg
	client.retryCount = 0
	c.mu.Unlock()

	slog.Debug("[SyncEOFController] SyncEof started",
		"client_id", clientID,
		"expected_total", expectedTotalMsg,
	)
	c.broadcastAmountRequest(clientID)
}

func (c *SyncEOFController) broadcastAmountRequest(clientID uuid.UUID) {
	slog.Debug("[SyncEOFController] Broadcast amount request", "client_id", clientID)
	msg := ControlMessage{
		Type:        MsgTypeAmountRequest,
		ClientID:    clientID,
		RequesterID: c.nodeID,
	}
	c.sendControlMessage(msg)
}

func (c *SyncEOFController) broadcastFlush(clientID uuid.UUID, totalSnt int) {
	slog.Debug("[SyncEOFController] Broadcast flush",
		"client_id", clientID,
		"total_sent", totalSnt,
	)
	msg := ControlMessage{
		Type:      MsgTypeFlush,
		ClientID:  clientID,
		RequesterID: c.nodeID,
		SentCount: totalSnt,
	}
	c.sendControlMessage(msg)
}

func (c *SyncEOFController) handleControlMessage(msg broker.Message, ack func(), nack func()) {
	ctrlMsg, err := UnmarshalControlMessage(msg)
	if err != nil {
		slog.Error("[SyncEOFController] Error al deserializar mensaje de control", "err", err)
		nack()
		return
	}

	slog.Debug("[SyncEOFController] Control message received",
		"type", ctrlMsg.Type,
		"client_id", ctrlMsg.ClientID,
		"requester_id", ctrlMsg.RequesterID,
		"sender_id", ctrlMsg.SenderID,
	)

	switch ctrlMsg.Type {
	case MsgTypeAmountRequest:
		c.processAmountRequest(*ctrlMsg)
	case MsgTypeAmountResponse:
		c.processAmountResponse(*ctrlMsg)
	case MsgTypeFlush:
		c.processFlush(*ctrlMsg)
	case MsgTypeFlushAck:
		c.processFlushAck(*ctrlMsg)
	case MsgTypeRetryExceeded:
		c.processRetryExceeded(*ctrlMsg)
	default:
		slog.Warn("[SyncEOFController] Tipo de mensaje de control desconocido", "type", ctrlMsg.Type)
	}
	
	ack()
}

func (c *SyncEOFController) processAmountRequest(msg ControlMessage) {
	c.mu.Lock()
	client := c.clients[msg.ClientID]
	rcvAmount := client.msgRcvCount
	sntAmount := client.msgSntCount
	c.mu.Unlock()

	slog.Debug("[SyncEOFController] Process amount request",
		"client_id", msg.ClientID,
		"received_count", rcvAmount,
		"sent_count", sntAmount,
		"requester_id", msg.RequesterID,
	)

	resp := ControlMessage{
		Type:          MsgTypeAmountResponse,
		ClientID:      msg.ClientID,
		RequesterID:   msg.RequesterID,
		SenderID:      c.nodeID,
		ReceivedCount: rcvAmount,
		SentCount:     sntAmount,
	}
	c.sendControlMessage(resp)
}

func (c *SyncEOFController) processAmountResponse(msg ControlMessage) {
	c.mu.Lock()
	client := c.clients[msg.ClientID]
	client.rcvResponses[msg.SenderID] = msg.ReceivedCount
	client.sntResponses[msg.SenderID] = msg.SentCount
	
	responsesCount := len(client.rcvResponses)
	c.mu.Unlock()

	slog.Debug("[SyncEOFController] Process amount response",
		"client_id", msg.ClientID,
		"sender_id", msg.SenderID,
		"received_count", msg.ReceivedCount,
		"sent_count", msg.SentCount,
		"responses_count", responsesCount,
	)

	if responsesCount == c.totalNodes {
		c.checkTotalAndFlush(msg.ClientID)
	}
}

func (c *SyncEOFController) retryAmountRequest(clientID uuid.UUID) {
	c.mu.Lock()
	client := c.clients[clientID]
	client.retryCount++
	attempt := client.retryCount
	maxRetries := c.maxRetries
	client.rcvResponses = make(map[int]int)
	client.sntResponses = make(map[int]int)
	baseDelay := c.retryBaseDelay
	stepDelay := c.retryStepDelay
	c.mu.Unlock()

	if maxRetries > 0 && attempt > maxRetries {
		slog.Warn("[SyncEOFController] Max retries exceeded",
			"client_id", clientID,
			"attempt", attempt,
			"max_retries", maxRetries,
		)
		c.broadcastRetryExceeded(clientID)
		c.runRetryExceededCallback(clientID)
		return
	}

	if attempt > 1 {
		baseDelay = baseDelay + (stepDelay * time.Duration(attempt-1))
	}

	slog.Debug("[SyncEOFController] Retry amount request",
		"client_id", clientID,
		"attempt", attempt,
		"delay", baseDelay,
	)

	time.Sleep(baseDelay)
	c.broadcastAmountRequest(clientID)
}

func (c *SyncEOFController) checkTotalAndFlush(clientID uuid.UUID) {
	c.mu.Lock()
	totalRcvReported := 0
	totalSntReported := 0
	for _, count := range c.clients[clientID].rcvResponses {
		totalRcvReported += count
	}
	for _, count := range c.clients[clientID].sntResponses {
		totalSntReported += count
	}
	expectedRcv := c.clients[clientID].expectedTotal
	c.mu.Unlock()

	if totalRcvReported == expectedRcv {
		slog.Info("[SyncEOFController] EOF sincronizado",
			"client_id", clientID,
			"expected_total", expectedRcv,
			"reported_total", totalRcvReported,
			"total_sent", totalSntReported,
		)
		c.clients[clientID].flushResponses = make(map[int]bool)
		c.clients[clientID].flushExpectedSent = totalSntReported
		c.broadcastFlush(clientID, totalSntReported)
	} else {
		slog.Warn("[SyncEOFController] EOF no matchea, reintentando",
			"client_id", clientID,
			"expected_total", expectedRcv,
			"reported_total", totalRcvReported,
		)
		c.retryAmountRequest(clientID)
	}
}

func (c *SyncEOFController) processFlush(msg ControlMessage) {
	slog.Info("[SyncEOFController] Recibido FLUSH",
		"client_id", msg.ClientID,
		"sent_count", msg.SentCount,
		"requester_id", msg.RequesterID,
	)
	if c.onFlush != nil {
		if err := c.onFlush(msg.ClientID); err != nil {
			slog.Error("[SyncEOFController] onFlush failed", "client_id", msg.ClientID, "err", err)
		}
	}
	c.sendFlushAck(msg.ClientID, msg.RequesterID)
	if msg.RequesterID != c.nodeID {
		c.cleanupClientState(msg.ClientID)
	}
}

func (c *SyncEOFController) processFlushAck(msg ControlMessage) {
	if msg.RequesterID != c.nodeID {
		return
	}

	c.mu.Lock()
	if c.clients[msg.ClientID].flushResponses == nil {
		c.clients[msg.ClientID].flushResponses = make(map[int]bool)
	}
	c.clients[msg.ClientID].flushResponses[msg.SenderID] = true
	responsesCount := len(c.clients[msg.ClientID].flushResponses)
	finalSent := c.clients[msg.ClientID].flushExpectedSent
	c.mu.Unlock()

	slog.Debug("[SyncEOFController] FLUSH ack recibido",
		"client_id", msg.ClientID,
		"sender_id", msg.SenderID,
		"responses_count", responsesCount,
		"expected_nodes", c.totalNodes,
	)

	if responsesCount == c.totalNodes {
		if c.onLeaderFlush != nil {
			if err := c.onLeaderFlush(msg.ClientID, finalSent); err != nil {
				slog.Error("[SyncEOFController] onLeaderFlush failed",
					"client_id", msg.ClientID,
					"final_sent", finalSent,
					"err", err,
				)
			}
		}
		c.cleanupClientState(msg.ClientID)
	}
}

func (c *SyncEOFController) processRetryExceeded(msg ControlMessage) {
	slog.Warn("[SyncEOFController] RETRY_EXCEEDED",
		"client_id", msg.ClientID,
		"requester_id", msg.RequesterID,
	)
	c.runRetryExceededCallback(msg.ClientID)
}

func (c *SyncEOFController) broadcastRetryExceeded(clientID uuid.UUID) {
	slog.Warn("[SyncEOFController] Broadcast retry exceeded", "client_id", clientID)
	msg := ControlMessage{
		Type:     MsgTypeRetryExceeded,
		ClientID: clientID,
	}
	c.sendControlMessage(msg)
}

func (c *SyncEOFController) runRetryExceededCallback(clientID uuid.UUID) {
	if c.onRetryExceeded != nil {
		if err := c.onRetryExceeded(clientID); err != nil {
			slog.Error("[SyncEOFController] onRetryExceeded failed", "client_id", clientID, "err", err)
		}
	}
	c.cleanupClientState(clientID)
}

func (c *SyncEOFController) cleanupClientState(clientID uuid.UUID) {
	c.mu.Lock()
	delete(c.clients, clientID)
	c.mu.Unlock()
	slog.Debug("[SyncEOFController] Client state cleaned", "client_id", clientID)
}

func (c *SyncEOFController) sendFlushAck(clientID uuid.UUID, requesterID int) {
	slog.Debug("[SyncEOFController] Send FLUSH ack",
		"client_id", clientID,
		"requester_id", requesterID,
	)
	msg := ControlMessage{
		Type:        MsgTypeFlushAck,
		ClientID:    clientID,
		RequesterID: requesterID,
		SenderID:    c.nodeID,
	}
	c.sendControlMessage(msg)
}

func (c *SyncEOFController) sendControlMessage(msg ControlMessage) {
	brokerMsg, err := MarshalControlMessage(msg)
	if err != nil {
		slog.Error("[SyncEOFController] Fail to marshal control message", "err", err)
		return
	}
	
	if err := c.broker.Send(*brokerMsg); err != nil {
		slog.Error("[SyncEOFController] Failed to send control message", "err", err)
		return
	}

	slog.Debug("[SyncEOFController] Control message sent",
		"type", msg.Type,
		"client_id", msg.ClientID,
		"requester_id", msg.RequesterID,
		"sender_id", msg.SenderID,
	)
}
