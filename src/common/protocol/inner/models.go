package inner

import (
	"encoding/json"

	"github.com/google/uuid"
)

type PacketType int

const (
	TypeTransaction PacketType = iota
	// TypeTransactionBatch
	TypeBankInfo
	TypeEOF
	TypeAccountsEOF

	TypeTxQ4

	TypeQuery1Result
	TypeQuery1EOF

	TypeQuery2Result
	TypeQuery2EOF

	TypeQuery3Result
	TypeQuery3EOF

	TypeQuery4Result
	TypeQuery4EOF

	TypeQuery5Result
	TypeQuery5EOF
)

type Packet struct {
	ClientID uuid.UUID  `json:"client_id"`
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
	case TypeTxQ4:
		return "tx_q4"
	case TypeQuery1Result:
		return "query1_result"
	case TypeQuery2Result:
		return "query2_result"
	case TypeQuery3Result:
		return "query3_result"
	case TypeQuery4Result:
		return "query4_result"
	case TypeQuery5Result:
		return "query5_result"
	case TypeQuery2EOF:
		return "query2_eof"
	case TypeQuery3EOF:
		return "query3_eof"
	case TypeQuery4EOF:
		return "query4_eof"
	case TypeQuery5EOF:
		return "query5_eof"
	case TypeEOF:
		return "eof"
	case TypeAccountsEOF:
		return "accounts_eof"
	default:
		return "unknown"
	}
}
