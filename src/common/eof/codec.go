package eof

import (
	"encoding/binary"
	"errors"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/google/uuid"
)

var (
	ErrInvalidControlMessage = errors.New("invalid control message")
	ErrMalformedControlMsg   = errors.New("malformed control message")
)

const (
	MsgTypeAmountRequest  = "AMOUNT_REQUEST"
	MsgTypeAmountResponse = "AMOUNT_RESPONSE"
	MsgTypeFlush          = "FLUSH"
	MsgTypeFlushAck       = "FLUSH_ACK"
	MsgTypeRetryExceeded  = "RETRY_EXCEEDED"
)

// ControlMessage representa la estructura del mensaje enviado para sincronizar EOF.
// Usa encoding binario ad-hoc (no JSON) porque los tipos map[[16]byte]struct{}
// no son serializables con encoding/json.
type ControlMessage struct {
	Type               string
	ClientID           uuid.UUID
	RequesterID        int
	SenderID           int
	ReceivedIds        map[protocol.MsgID]struct{}
	SentCountByKeyIds  map[broker.KeyType]map[protocol.MsgID]struct{}
}

func MarshalControlMessage(msg ControlMessage) (*broker.Message, error) {
	buf := make([]byte, 0, 256)

	// Type
	buf = binary.AppendUvarint(buf, uint64(len(msg.Type)))
	buf = append(buf, msg.Type...)

	// ClientID
	buf = append(buf, msg.ClientID[:]...)

	// RequesterID
	buf = binary.AppendUvarint(buf, uint64(msg.RequesterID))

	// SenderID
	buf = binary.AppendUvarint(buf, uint64(msg.SenderID))

	// ReceivedIds
	buf = binary.AppendUvarint(buf, uint64(len(msg.ReceivedIds)))
	for id := range msg.ReceivedIds {
		buf = append(buf, id[:]...)
	}

	// SentCountByKeyIds
	buf = binary.AppendUvarint(buf, uint64(len(msg.SentCountByKeyIds)))
	for key, ids := range msg.SentCountByKeyIds {
		buf = binary.AppendUvarint(buf, uint64(len(key)))
		buf = append(buf, key...)
		buf = binary.AppendUvarint(buf, uint64(len(ids)))
		for id := range ids {
			buf = append(buf, id[:]...)
		}
	}

	return &broker.Message{Body: buf}, nil
}

func UnmarshalControlMessage(msg broker.Message) (*ControlMessage, error) {
	data := msg.Body

	var ctrlMsg ControlMessage

	offset := 0
	readNext := data[offset:]

	// Type
	typLen, n := binary.Uvarint(readNext)
	if n <= 0 {
		return nil, ErrMalformedControlMsg
	}
	offset += n
	if offset+int(typLen) > len(data) {
		return nil, ErrMalformedControlMsg
	}
	ctrlMsg.Type = string(data[offset : offset+int(typLen)])
	offset += int(typLen)

	// ClientID
	if offset+len(ctrlMsg.ClientID) > len(data) {
		return nil, ErrMalformedControlMsg
	}
	copy(ctrlMsg.ClientID[:], data[offset:])
	offset += len(ctrlMsg.ClientID)

	if ctrlMsg.Type == "" || ctrlMsg.ClientID == uuid.Nil {
		return nil, ErrInvalidControlMessage
	}

	// RequesterID
	readNext = data[offset:]
	reqID, n := binary.Uvarint(readNext)
	if n <= 0 {
		return nil, ErrMalformedControlMsg
	}
	offset += n
	ctrlMsg.RequesterID = int(reqID)

	// SenderID
	readNext = data[offset:]
	sndID, n := binary.Uvarint(readNext)
	if n <= 0 {
		return nil, ErrMalformedControlMsg
	}
	offset += n
	ctrlMsg.SenderID = int(sndID)

	// ReceivedIds
	readNext = data[offset:]
	rcvCount, n := binary.Uvarint(readNext)
	if n <= 0 {
		return nil, ErrMalformedControlMsg
	}
	offset += n
	ctrlMsg.ReceivedIds = make(map[protocol.MsgID]struct{}, rcvCount)
	const msgIDLen = len(protocol.MsgID{})
	for i := uint64(0); i < rcvCount; i++ {
		if offset+msgIDLen > len(data) {
			return nil, ErrMalformedControlMsg
		}
		var id protocol.MsgID
		copy(id[:], data[offset:])
		offset += msgIDLen
		ctrlMsg.ReceivedIds[id] = struct{}{}
	}

	// SentCountByKeyIds
	readNext = data[offset:]
	numKeys, n := binary.Uvarint(readNext)
	if n <= 0 {
		return nil, ErrMalformedControlMsg
	}
	offset += n
	ctrlMsg.SentCountByKeyIds = make(map[broker.KeyType]map[protocol.MsgID]struct{}, numKeys)
	for i := uint64(0); i < numKeys; i++ {
		readNext = data[offset:]
		keyLen, n := binary.Uvarint(readNext)
		if n <= 0 {
			return nil, ErrMalformedControlMsg
		}
		offset += n
		if offset+int(keyLen) > len(data) {
			return nil, ErrMalformedControlMsg
		}
		key := broker.KeyType(data[offset : offset+int(keyLen)])
		offset += int(keyLen)

		readNext = data[offset:]
		numIDs, n := binary.Uvarint(readNext)
		if n <= 0 {
			return nil, ErrMalformedControlMsg
		}
		offset += n

		ids := make(map[protocol.MsgID]struct{}, numIDs)
		for j := uint64(0); j < numIDs; j++ {
			if offset+msgIDLen > len(data) {
				return nil, ErrMalformedControlMsg
			}
			var id protocol.MsgID
			copy(id[:], data[offset:])
			offset += msgIDLen
			ids[id] = struct{}{}
		}
		ctrlMsg.SentCountByKeyIds[key] = ids
	}

	return &ctrlMsg, nil
}
