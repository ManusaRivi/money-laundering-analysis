package protocol

import "github.com/google/uuid"

type MsgType uint8

const (
	MsgTypeInvalid MsgType = iota
	MsgAccountsBatch
	MsgAccountsEOF
	MsgTransactionsBatch
	MsgTransactionsEOF

	MsgTxQ4
	MsgTxAccounts //for Q4, preguntar a MANU

	MsgQuery1Result
	MsgQuery2Result
	MsgQuery3Result
	MsgQuery4Result
	MsgQuery5Result

	MsgQuery1ResultEOF
	MsgQuery2ResultEOF
	MsgQuery3ResultEOF
	MsgQuery4ResultEOF
	MsgQuery5ResultEOF

	// Q4 degree exchange between ScatterAndGather replicas.
	MsgQ4HeavyBatch
	MsgQ4HeavyDone
)

// Roles carried in a Q4 heavy-accounts batch: whether the accounts are heavy
// sources (eligible A1) or heavy sinks (eligible A2).
const (
	Q4HeavyRoleSource uint8 = 0
	Q4HeavyRoleSink   uint8 = 1
)

func (m MsgType) String() string {
	switch m {
	case MsgAccountsBatch:
		return "MsgAccountsBatch"
	case MsgAccountsEOF:
		return "MsgAccountsEOF"
	case MsgTransactionsBatch:
		return "MsgTransactionsBatch"
	case MsgTransactionsEOF:
		return "MsgTransactionsEOF"
	case MsgTxQ4:
		return "MsgTxQ4"
	case MsgTxAccounts:
		return "MsgTxAccounts"
	case MsgQuery1Result:
		return "MsgQuery1Result"
	case MsgQuery2Result:
		return "MsgQuery2Result"
	case MsgQuery3Result:
		return "MsgQuery3Result"
	case MsgQuery4Result:
		return "MsgQuery4Result"
	case MsgQuery5Result:
		return "MsgQuery5Result"
	case MsgQuery1ResultEOF:
		return "MsgQuery1ResultEOF"
	case MsgQuery2ResultEOF:
		return "MsgQuery2ResultEOF"
	case MsgQuery3ResultEOF:
		return "MsgQuery3ResultEOF"
	case MsgQuery4ResultEOF:
		return "MsgQuery4ResultEOF"
	case MsgQuery5ResultEOF:
		return "MsgQuery5ResultEOF"
	case MsgQ4HeavyBatch:
		return "MsgQ4HeavyBatch"
	case MsgQ4HeavyDone:
		return "MsgQ4HeavyDone"
	default:
		return "MsgTypeInvalid"
	}
}

type ExternalEnvelope struct {
	MsgType MsgType
	Payload []byte
}

type InternalEnvelope struct {
	MsgType  MsgType
	ClientId uuid.UUID
	Payload  []byte
}

type Transaction struct {
	Timestamp         string
	FromBank          string
	FromAccount       string
	ToBank            string
	ToAccount         string
	AmountReceived    float64
	ReceivingCurrency string
	AmountPaid        float64
	PaymentCurrency   string
	PaymentFormat     string
	IsLaundering      bool
}

type AccountData struct {
	BankName      string
	BankID        string
	AccountNumber string
	EntityID      string
	EntityName    string
}

type Query1Result struct {
	FromBank    string
	FromAccount string
	ToBank      string
	ToAccount   string
	AmountPaid  float64
}

type Query2Result struct {
	FromBank    string
	FromAccount string
	BankName    string
	AmountPaid  float64
}

type Query3Result struct {
	FromBank      string
	FromAccount   string
	PaymentFormat string
	AmountPaid    float64
}

type Query4Result struct {
	BankID string
	ID     string
}
type Query5Result struct {
	Count int64
}

func (t *Transaction) GetOriginId() string {
	return t.FromAccount + "-" + t.FromBank
}

func (t *Transaction) GetDestId() string {
	return t.ToAccount + "-" + t.ToBank
}

func (t *Transaction) GetTransactionId() string {
	return t.GetOriginId() + "-" + t.GetDestId() + "-" + t.Timestamp
}

func (t *Transaction) GetTransactionField(field string) string {
	switch field {
	case "origin":
		if t.FromAccount != "" && t.FromBank != "" {
			return t.GetOriginId()
		}
	case "dest":
		if t.ToAccount != "" && t.ToBank != "" {
			return t.GetDestId()
		}
	case "BankID":
		if t.FromBank != "" {
			return t.FromBank
		}
	case "ID":
		if t.FromAccount != "" {
			return t.FromAccount
		}
	}
	return ""
}
