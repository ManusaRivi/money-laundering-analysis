package external

type MsgType uint8

const (
	MsgTypeInvalid       MsgType = 0
	MsgAccountsBatch     MsgType = 1
	MsgAccountsEOF       MsgType = 2
	MsgTransactionsBatch MsgType = 3
	MsgTransactionsEOF   MsgType = 4

	MsgQuery1Result MsgType = 5
	MsgQuery2Result MsgType = 6
	MsgQuery3Result MsgType = 7
	MsgQuery4Result MsgType = 8
	MsgQuery5Result MsgType = 9

	MsgQuery1ResultEOF MsgType = 10
	MsgQuery2ResultEOF MsgType = 11
	MsgQuery3ResultEOF MsgType = 12
	MsgQuery4ResultEOF MsgType = 13
	MsgQuery5ResultEOF MsgType = 14
)

type Envelope struct {
	MsgType MsgType
	Payload []byte
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
