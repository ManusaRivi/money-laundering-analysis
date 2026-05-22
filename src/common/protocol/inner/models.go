package inner

import "encoding/json"

type MessageType int

const (
	MsgTypeTransaction MessageType = iota
	// MsgTypeTransactionBatch
	MsgTypeBankInfo
	MsgTypeEOF
)

type InnerMessage struct {
	ClientID string          `json:"client_id"`
	Type     MessageType     `json:"type"`
	Data     json.RawMessage `json:"data"`
}
