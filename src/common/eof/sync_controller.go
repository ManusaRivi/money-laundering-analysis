package eof

import (
	"log"
	"sync"
	"time"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
)

type SyncEOFController struct {
	broker *EOFBroker

	nodeID     string
	totalNodes int // La cantidad total de workers del mismo tipo

	mu            sync.Mutex
	msgRcvCount   map[string]int // clientID -> cantidad de mensajes que recibio este nodo
	msgSntCount   map[string]int // clientID -> cantidad de mensajes que envio este nodo al siguiente stage
	expectedTotal map[string]int // clientID -> total_messages que espera recibir el cluster para flushear
	retryCounts   map[string]int // clientID -> cantidad de reintentos de amount request

	rcvResponses map[string]map[string]int // clientID -> senderID -> amount rcv reportado
	sntResponses map[string]map[string]int // clientID -> senderID -> amount snt reportado
	flushResponses map[string]map[string]bool // clientID -> senderID -> flush ack
	flushExpectedSent map[string]int // clientID -> total sent for EOF

	// Callback a ejecutar cuando todos los workers terminan.
	// Se llama pasando el clientID
	onFlush func(clientID string)

	// Callback para que el lider emita el EOF a la siguiente etapa.
	onLeaderFlush func(clientID string, finalSent int)

	// Callback cuando el cliente supera el maximo de reintentos.
	onRetryExceeded func(clientID string)

	retryBaseDelay time.Duration
	retryStepDelay time.Duration
	maxRetries     int
}

// NewSyncEOFController inicializa un nuevo SyncEOFController
func NewSyncEOFController(
	rabbitURL string,
	broadcastExchange string,
	nodeID string,
	EOFPrefix string,
	totalNodes int,
	retryBaseDelay time.Duration,
	retryStepDelay time.Duration,
	maxRetries int,
	onFlush func(clientID string),
	onLeaderFlush func(clientID string, finalSent int),
	onRetryExceeded func(clientID string),
) (*SyncEOFController, error) {
	eofBroker, err := NewEOFBroker(rabbitURL, broadcastExchange, nodeID, EOFPrefix)
	if err != nil {
		return nil, err
	}

	return &SyncEOFController{
		broker:        eofBroker,
		nodeID:        nodeID,
		totalNodes:    totalNodes,
		msgRcvCount:   make(map[string]int),
		msgSntCount:   make(map[string]int),
		expectedTotal: make(map[string]int),
		retryCounts:   make(map[string]int),
		rcvResponses:  make(map[string]map[string]int),
		sntResponses:  make(map[string]map[string]int),
		flushResponses: make(map[string]map[string]bool),
		flushExpectedSent: make(map[string]int),
		onFlush:       onFlush,
		onLeaderFlush: onLeaderFlush,
		onRetryExceeded: onRetryExceeded,
		retryBaseDelay: retryBaseDelay,
		retryStepDelay: retryStepDelay,
		maxRetries:     maxRetries,
	}, nil
}

// Start comienza a escuchar en el broker los mensajes de control de otros workers
func (c *SyncEOFController) Start() error {
	return c.broker.StartConsuming(c.handleControlMessage)
}

// AddMsg incrementa la cantidad de mensajes procesados por este nodo para un clientID.
// Si sended es true se suma a la cantidad de enviados a la siguiente etapa.
func (c *SyncEOFController) AddMsg(clientID string, sended bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.msgRcvCount[clientID]++

	if sended {
		c.msgSntCount[clientID]++
	}
}

// SyncEof inicia el proceso de sincronizacion. Se llama cuando un nodo asume el rol de lider
func (c *SyncEOFController) SyncEof(clientID string, expectedTotalMsg int) {
	c.mu.Lock()
	c.expectedTotal[clientID] = expectedTotalMsg
	c.rcvResponses[clientID] = make(map[string]int)
	c.sntResponses[clientID] = make(map[string]int)
	c.retryCounts[clientID] = 0
	c.mu.Unlock()

	c.broadcastAmountRequest(clientID)
}

func (c *SyncEOFController) broadcastAmountRequest(clientID string) {
	msg := ControlMessage{
		Type:        MsgTypeAmountRequest,
		ClientID:    clientID,
		RequesterID: c.nodeID,
	}
	c.sendControlMessage(msg)
}

func (c *SyncEOFController) broadcastFlush(clientID string, totalSnt int) {
	msg := ControlMessage{
		Type:      MsgTypeFlush,
		ClientID:  clientID,
		RequesterID: c.nodeID,
	}
	c.sendControlMessage(msg)
}

func (c *SyncEOFController) handleControlMessage(msg broker.Message, ack func(), nack func()) {
	ctrlMsg, err := UnmarshalControlMessage(msg)
	if err != nil {
		log.Printf("[SyncEOFController] Error al deserializar mensaje de control: %v\n", err)
		nack()
		return
	}

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
		log.Printf("[SyncEOFController] Tipo de mensaje de control desconocido: %s\n", ctrlMsg.Type)
	}
	
	ack()
}

func (c *SyncEOFController) processAmountRequest(msg ControlMessage) {
	c.mu.Lock()
	rcvAmount := c.msgRcvCount[msg.ClientID]
	sntAmount := c.msgSntCount[msg.ClientID]
	c.mu.Unlock()

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
	c.rcvResponses[msg.ClientID][msg.SenderID] = msg.ReceivedCount
	c.sntResponses[msg.ClientID][msg.SenderID] = msg.SentCount
	
	responsesCount := len(c.rcvResponses[msg.ClientID])
	c.mu.Unlock()

	if responsesCount == c.totalNodes {
		c.checkTotalAndFlush(msg.ClientID)
	}
}

func (c *SyncEOFController) retryAmountRequest(clientID string) {
	c.mu.Lock()
	c.retryCounts[clientID]++
	attempt := c.retryCounts[clientID]
	maxRetries := c.maxRetries
	c.rcvResponses[clientID] = make(map[string]int)
	c.sntResponses[clientID] = make(map[string]int)
	baseDelay := c.retryBaseDelay
	stepDelay := c.retryStepDelay
	c.mu.Unlock()

	if maxRetries > 0 && attempt > maxRetries {
		c.broadcastRetryExceeded(clientID)
		c.runRetryExceededCallback(clientID)
		return
	}

	if attempt > 1 {
		baseDelay = baseDelay + (stepDelay * time.Duration(attempt-1))
	}

	time.Sleep(baseDelay)
	c.broadcastAmountRequest(clientID)
}

func (c *SyncEOFController) checkTotalAndFlush(clientID string) {
	c.mu.Lock()
	totalRcvReported := 0
	totalSntReported := 0
	for _, count := range c.rcvResponses[clientID] {
		totalRcvReported += count
	}
	for _, count := range c.sntResponses[clientID] {
		totalSntReported += count
	}
	expectedRcv := c.expectedTotal[clientID]
	c.mu.Unlock()

	if totalRcvReported == expectedRcv {
		log.Printf("[SyncEOFController] EOF Sincronizado para %s! Match total: %d. Total sent to next stage: %d. Emitiendo FLUSH.\n", clientID, expectedRcv, totalSntReported)
		c.flushResponses[clientID] = make(map[string]bool)
		c.flushExpectedSent[clientID] = totalSntReported
		c.broadcastFlush(clientID, totalSntReported)
	} else {
		log.Printf("[SyncEOFController] EOF para %s no matchea. Esperado: %d, Reportado: %d. Retrying...\n", clientID, expectedRcv, totalRcvReported)
		c.retryAmountRequest(clientID)
	}
}

func (c *SyncEOFController) processFlush(msg ControlMessage) {
	log.Printf("[SyncEOFController] Recibido FLUSH para %s. Next stage amount: %d\n", msg.ClientID, msg.SentCount)
	if c.onFlush != nil {
		c.onFlush(msg.ClientID)
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
	if c.flushResponses[msg.ClientID] == nil {
		c.flushResponses[msg.ClientID] = make(map[string]bool)
	}
	c.flushResponses[msg.ClientID][msg.SenderID] = true
	responsesCount := len(c.flushResponses[msg.ClientID])
	finalSent := c.flushExpectedSent[msg.ClientID]
	c.mu.Unlock()

	if responsesCount == c.totalNodes {
		if c.onLeaderFlush != nil {
			c.onLeaderFlush(msg.ClientID, finalSent)
		}
		c.cleanupClientState(msg.ClientID)
	}
}

func (c *SyncEOFController) processRetryExceeded(msg ControlMessage) {
	log.Printf("[SyncEOFController] RETRY_EXCEEDED para %s\n", msg.ClientID)
	c.runRetryExceededCallback(msg.ClientID)
}

func (c *SyncEOFController) broadcastRetryExceeded(clientID string) {
	msg := ControlMessage{
		Type:     MsgTypeRetryExceeded,
		ClientID: clientID,
	}
	c.sendControlMessage(msg)
}

func (c *SyncEOFController) runRetryExceededCallback(clientID string) {
	if c.onRetryExceeded != nil {
		c.onRetryExceeded(clientID)
	}
	c.cleanupClientState(clientID)
}

func (c *SyncEOFController) cleanupClientState(clientID string) {
	c.mu.Lock()
	delete(c.msgRcvCount, clientID)
	delete(c.msgSntCount, clientID)
	delete(c.expectedTotal, clientID)
	delete(c.retryCounts, clientID)
	delete(c.rcvResponses, clientID)
	delete(c.sntResponses, clientID)
	delete(c.flushResponses, clientID)
	delete(c.flushExpectedSent, clientID)
	c.mu.Unlock()
}

func (c *SyncEOFController) sendFlushAck(clientID string, requesterID string) {
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
		log.Printf("[SyncEOFController] Fail to marshal control message: %v\n", err)
		return
	}
	
	if err := c.broker.Send(*brokerMsg); err != nil {
		log.Printf("[SyncEOFController] Fail to send control message: %v\n", err)
	}
}
