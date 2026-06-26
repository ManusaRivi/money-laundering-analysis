package eof

import (
	"encoding/binary"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/google/uuid"
)

var msgIDLen = len(protocol.MsgID{})

const defaultRetryBaseDelay = 500 * time.Millisecond

type nodeInfo struct {
	rcvByID     map[protocol.MsgID]int                    // batchID -> txCount recibidos reportados
	sentByKeyID map[broker.KeyType]map[protocol.MsgID]int // por key: batchID -> txCount enviados reportados
}

type client struct {
	clientID       uuid.UUID
	msgRcvByID     map[protocol.MsgID]int                    // batchID -> txCount que recibio este nodo
	msgSentByKeyID map[broker.KeyType]map[protocol.MsgID]int // por key: batchID -> txCount enviados
	expectedTotal  int                                       // total_messages que espera recibir el cluster para flushear
	retryCount     int                                       // cantidad de reintentos de amount request

	nodesInfo map[int]nodeInfo // senderID -> nodeInfo
}

type SyncEOFController struct {
	broker *EOFBroker

	nodeID     int
	totalNodes int // La cantidad total de workers del mismo tipo

	mu      sync.Mutex
	clients map[uuid.UUID]*client

	// Callback a ejecutar cuando todos los workers terminan.
	// Se llama pasando el clientID
	onFlush func(clientID uuid.UUID) error

	// Callback para que el lider emita el EOF a la siguiente etapa.
	onLeaderFlush func(clientID uuid.UUID, finalCountSentByKey map[broker.KeyType]int) error

	// Callback cuando el cliente supera el maximo de reintentos.
	onRetryExceeded func(clientID uuid.UUID) error

	retryBaseDelay time.Duration
	retryStepDelay time.Duration
	maxRetries     int
}

func NewClient(clientID uuid.UUID) *client {
	return &client{
		clientID:       clientID,
		msgRcvByID:     make(map[protocol.MsgID]int),
		msgSentByKeyID: make(map[broker.KeyType]map[protocol.MsgID]int),
		nodesInfo:      make(map[int]nodeInfo),
	}
}

// NewSyncEOFController inicializa un nuevo SyncEOFController
func NewSyncEOFController(
	cfg config.SyncEOFControllerConfig,
	onFlush func(clientID uuid.UUID) error,
	onLeaderFlush func(clientID uuid.UUID, finalCountSentByKey map[broker.KeyType]int) error,
	onRetryExceeded func(clientID uuid.UUID) error,
) (*SyncEOFController, error) {
	eofBroker, err := NewEOFBroker(cfg.RabbitURL, cfg.BroadcastExchange, cfg.WorkerID, cfg.EOFPrefix)
	if err != nil {
		return nil, err
	}

	retryBaseDelay := time.Duration(cfg.RetryBaseDelay * float64(time.Second))
	retryStepDelay := time.Duration(cfg.RetryStepDelay * float64(time.Second))
	if retryBaseDelay <= 0 {
		retryBaseDelay = defaultRetryBaseDelay
	}

	controller := &SyncEOFController{
		broker:          eofBroker,
		nodeID:          cfg.WorkerID,
		totalNodes:      cfg.WorkerAmount,
		clients:         make(map[uuid.UUID]*client),
		onFlush:         onFlush,
		onLeaderFlush:   onLeaderFlush,
		onRetryExceeded: onRetryExceeded,
		retryBaseDelay:  retryBaseDelay,
		retryStepDelay:  retryStepDelay,
		maxRetries:      cfg.MaxRetries,
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

func (c *SyncEOFController) Stop() {
	slog.Debug("[SyncEOFController] Stopping", "worker_id", c.nodeID)
	c.broker.StopConsuming()
	c.broker.Close()
}

// MessageReceived incrementa el contador de mensajes recibidos para un cliente dado.
// Se llama cada vez que este nodo recibe un mensaje de ese cliente.
func (c *SyncEOFController) MessageReceived(clientID uuid.UUID, msgID protocol.MsgID, processedCount int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.clients[clientID]; !exists {
		c.clients[clientID] = NewClient(clientID)
		slog.Debug("[SyncEOFController] Added client state", "client_id", clientID)
	}

	c.clients[clientID].msgRcvByID[msgID] = processedCount
}

func (c *SyncEOFController) MessageSent(clientID uuid.UUID, msgID protocol.MsgID, sentCount int) {
	c.MessageSentWithKey(clientID, broker.KeyNil, msgID, sentCount)
}

func (c *SyncEOFController) MessageSentWithKey(clientID uuid.UUID, keyType broker.KeyType, msgID protocol.MsgID, sentCount int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.clients[clientID]; !exists {
		c.clients[clientID] = NewClient(clientID)
		slog.Debug("[SyncEOFController] Added client state", "client_id", clientID)
	}
	client := c.clients[clientID]
	if keyType == broker.KeyNil {
		setSentID(client.msgSentByKeyID, broker.KeyNil, msgID, sentCount)
	} else {
		setSentID(client.msgSentByKeyID, keyType, msgID, sentCount)
		setSentID(client.msgSentByKeyID, broker.KeyNil, msgID, sentCount)
	}
}

// SyncEof inicia el proceso de sincronizacion con key Nil. Se llama cuando un nodo asume el rol de lider.
func (c *SyncEOFController) SyncEof(clientID uuid.UUID, counts map[broker.KeyType]int, keyType broker.KeyType) {
	expectedTotal := 0
	if counts != nil {
		expectedTotal = counts[keyType]
	}

	c.mu.Lock()
	if _, exists := c.clients[clientID]; !exists {
		c.clients[clientID] = NewClient(clientID)
		slog.Debug("[SyncEOFController] Added client state", "client_id", clientID)
	}
	client := c.clients[clientID]
	client.expectedTotal = expectedTotal
	client.retryCount = 0
	c.mu.Unlock()

	slog.Debug("[SyncEOFController] SyncEof started",
		"client_id", clientID,
		"expected_total", expectedTotal,
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

func (c *SyncEOFController) broadcastFlush(clientID uuid.UUID) {
	slog.Debug("[SyncEOFController] Broadcast flush", "client_id", clientID)
	msg := ControlMessage{
		Type:        MsgTypeFlush,
		ClientID:    clientID,
		RequesterID: c.nodeID,
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
	case MsgTypeRetryExceeded:
		c.processRetryExceeded(*ctrlMsg)
	default:
		slog.Warn("[SyncEOFController] Tipo de mensaje de control desconocido", "type", ctrlMsg.Type)
	}

	ack()
}

func (c *SyncEOFController) processAmountRequest(msg ControlMessage) {
	c.mu.Lock()
	if _, exists := c.clients[msg.ClientID]; !exists {
		c.clients[msg.ClientID] = NewClient(msg.ClientID)
		slog.Debug("[SyncEOFController] Added client state", "client_id", msg.ClientID)
	}
	client := c.clients[msg.ClientID]
	rcvIDs := encodeIDCounts(client.msgRcvByID)
	sentIDsByKey := encodeSentIDCounts(client.msgSentByKeyID)
	c.mu.Unlock()

	slog.Debug("[SyncEOFController] Process amount request",
		"client_id", msg.ClientID,
		"received_batches", len(rcvIDs)/(msgIDLen+4),
		"requester_id", msg.RequesterID,
	)

	resp := ControlMessage{
		Type:         MsgTypeAmountResponse,
		ClientID:     msg.ClientID,
		RequesterID:  msg.RequesterID,
		SenderID:     c.nodeID,
		ReceivedIDs:  rcvIDs,
		SentIDsByKey: sentIDsByKey,
	}
	c.sendControlMessage(resp)
}

func (c *SyncEOFController) processAmountResponse(msg ControlMessage) {
	c.mu.Lock()
	client := c.clients[msg.ClientID]
	info := client.nodesInfo[msg.SenderID]
	info.rcvByID = decodeIDCounts(msg.ReceivedIDs)
	info.sentByKeyID = decodeSentIDCounts(msg.SentIDsByKey)
	client.nodesInfo[msg.SenderID] = info
	responsesCount := len(client.nodesInfo)
	c.mu.Unlock()

	slog.Debug("[SyncEOFController] Process amount response",
		"client_id", msg.ClientID,
		"sender_id", msg.SenderID,
		"received_batches", len(info.rcvByID),
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
	client.nodesInfo = make(map[int]nodeInfo)
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
	client := c.clients[clientID]
	totalRcvReported := mergeReceived(client.nodesInfo)
	combinedSentByKey := mergeSentByKey(client.nodesInfo)
	expectedRcv := client.expectedTotal

	// Si el set union de IDs está vacío, todos los esclavos ya borraron sus datos:
	// el líder anterior completó el trabajo antes de caerse. No hay nada que hacer.
	unionEmpty := isReceivedUnionEmpty(client.nodesInfo)
	c.mu.Unlock()

	if unionEmpty {
		slog.Info("[SyncEOFController] Union de IDs vacia, EOF ya fue procesado por lider anterior",
			"client_id", clientID,
		)
		c.cleanupClientState(clientID)
		return
	}

	if totalRcvReported == expectedRcv {
		slog.Info("[SyncEOFController] EOF sincronizado",
			"client_id", clientID,
			"expected_total", expectedRcv,
			"reported_total", totalRcvReported,
			"total_sent", combinedSentByKey,
		)
		if c.onLeaderFlush != nil {
			if err := c.onLeaderFlush(clientID, combinedSentByKey); err != nil {
				slog.Error("[SyncEOFController] onLeaderFlush failed",
					"client_id", clientID,
					"final_count_sent", combinedSentByKey,
					"err", err,
				)
			}
		}
		c.broadcastFlush(clientID)
		c.cleanupClientState(clientID)
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
		"requester_id", msg.RequesterID,
	)
	if c.onFlush != nil {
		if err := c.onFlush(msg.ClientID); err != nil {
			slog.Error("[SyncEOFController] onFlush failed", "client_id", msg.ClientID, "err", err)
		}
	}
	c.cleanupClientState(msg.ClientID)
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

type clientCheckpoint struct {
	RcvByID     []byte                    `json:"rcv_by_id"`
	SentByKeyID map[broker.KeyType][]byte `json:"sent_by_key_id"`
}

func (c *SyncEOFController) SnapshotClient(clientID uuid.UUID) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cl, exists := c.clients[clientID]
	if !exists {
		return nil, nil
	}
	return json.Marshal(clientCheckpoint{
		RcvByID:     encodeIDCounts(cl.msgRcvByID),
		SentByKeyID: encodeSentIDCounts(cl.msgSentByKeyID),
	})
}

func (c *SyncEOFController) RestoreClient(clientID uuid.UUID, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	var cp clientCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cl, exists := c.clients[clientID]
	if !exists {
		cl = NewClient(clientID)
		c.clients[clientID] = cl
	}
	cl.msgRcvByID = decodeIDCounts(cp.RcvByID)
	cl.msgSentByKeyID = decodeSentIDCounts(cp.SentByKeyID)
	return nil
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

func mergeReceived(nodes map[int]nodeInfo) int {
	union := make(map[protocol.MsgID]int)
	for _, info := range nodes {
		for id, count := range info.rcvByID {
			union[id] = count
		}
	}
	total := 0
	for _, count := range union {
		total += count
	}
	return total
}

// isReceivedUnionEmpty devuelve true si ningún nodo reportó haber recibido mensajes del cliente.
// Esto indica que todos ya borraron su estado (el líder anterior completó el flush).
func isReceivedUnionEmpty(nodes map[int]nodeInfo) bool {
	for _, info := range nodes {
		if len(info.rcvByID) > 0 {
			return false
		}
	}
	return true
}

func mergeSentByKey(nodes map[int]nodeInfo) map[broker.KeyType]int {
	union := make(map[broker.KeyType]map[protocol.MsgID]int)
	for _, info := range nodes {
		for key, set := range info.sentByKeyID {
			merged := union[key]
			if merged == nil {
				merged = make(map[protocol.MsgID]int)
				union[key] = merged
			}
			for id, count := range set {
				merged[id] = count
			}
		}
	}
	out := make(map[broker.KeyType]int, len(union))
	for key, set := range union {
		sum := 0
		for _, count := range set {
			sum += count
		}
		out[key] = sum
	}
	return out
}

func setSentID(sets map[broker.KeyType]map[protocol.MsgID]int, key broker.KeyType, id protocol.MsgID, count int) {
	set := sets[key]
	if set == nil {
		set = make(map[protocol.MsgID]int)
		sets[key] = set
	}
	set[id] = count
}

func encodeIDCounts(set map[protocol.MsgID]int) []byte {
	if len(set) == 0 {
		return nil
	}
	rec := msgIDLen + 4
	buf := make([]byte, 0, len(set)*rec)
	for id, count := range set {
		buf = append(buf, id[:]...)
		var c [4]byte
		binary.BigEndian.PutUint32(c[:], uint32(count))
		buf = append(buf, c[:]...)
	}
	return buf
}

func decodeIDCounts(data []byte) map[protocol.MsgID]int {
	rec := msgIDLen + 4
	set := make(map[protocol.MsgID]int, len(data)/rec)
	for i := 0; i+rec <= len(data); i += rec {
		var id protocol.MsgID
		copy(id[:], data[i:i+msgIDLen])
		set[id] = int(binary.BigEndian.Uint32(data[i+msgIDLen : i+rec]))
	}
	return set
}

func encodeSentIDCounts(sets map[broker.KeyType]map[protocol.MsgID]int) map[broker.KeyType][]byte {
	if len(sets) == 0 {
		return nil
	}
	out := make(map[broker.KeyType][]byte, len(sets))
	for key, set := range sets {
		out[key] = encodeIDCounts(set)
	}
	return out
}

func decodeSentIDCounts(in map[broker.KeyType][]byte) map[broker.KeyType]map[protocol.MsgID]int {
	out := make(map[broker.KeyType]map[protocol.MsgID]int, len(in))
	for key, data := range in {
		out[key] = decodeIDCounts(data)
	}
	return out
}

func SyncKeyFromInputKeys(inputKeys []string) broker.KeyType {
	if len(inputKeys) == 1 {
		key := broker.KeyType(inputKeys[0])
		if key != broker.KeyNil && key != broker.KeyAllTransaction {
			return key
		}
	}
	return broker.KeyNil
}
