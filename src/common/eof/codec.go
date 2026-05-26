package eof

import (
	"encoding/json"
	"errors"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/google/uuid"
)

var (
	ErrInvalidControlMessage = errors.New("invalid control message")
)

const (
	MsgTypeAmountRequest  = "AMOUNT_REQUEST"
	MsgTypeAmountResponse = "AMOUNT_RESPONSE"
	MsgTypeFlush          = "FLUSH"
	MsgTypeFlushAck       = "FLUSH_ACK"
	MsgTypeRetryExceeded  = "RETRY_EXCEEDED"
)

// ControlMessage representa la estructura del mensaje enviado para sincronizar EOF
type ControlMessage struct {
	Type          string `json:"type"`
	ClientID      uuid.UUID `json:"client_id"`
	RequesterID   int `json:"requester_id"`
	SenderID      int `json:"sender_id"`
	ReceivedCount int    `json:"received_count"`
	SentCount     int    `json:"sent_count"`
}

func MarshalControlMessage(msg ControlMessage) (*broker.Message, error) {
	data, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return &broker.Message{Body: data}, nil
}

func UnmarshalControlMessage(msg broker.Message) (*ControlMessage, error) {
	var ctrlMsg ControlMessage
	err := json.Unmarshal([]byte(msg.Body), &ctrlMsg)
	if err != nil {
		return nil, err
	}
	if ctrlMsg.Type == "" || ctrlMsg.ClientID == uuid.Nil {
		return nil, ErrInvalidControlMessage
	}
	return &ctrlMsg, nil
}
