package inner

import "encoding/json"

type PacketType int

const (
	TypeTransaction PacketType = iota
	// TypeTransactionBatch
	TypeBankInfo
	TypeEOF
)

type Packet struct {
	ClientID string     `json:"client_id"`
	Type     PacketType `json:"type"`
	// Data can be a Transaction, BankInfo, or any other type depending on the PacketType
	// If the PacketType is TypeEOF, Data can be nil, empty or whatever we want...
	Data json.RawMessage `json:"data"`
}

func (t *PacketType) String() string {
	switch *t {
	case TypeTransaction:
		return "transaction"
	case TypeBankInfo:
		return "bank_info"
	case TypeEOF:
		return "eof"
	default:
		return "unknown"
	}
}
