package protocol

type MsgType uint8

const (
	MsgTypeInvalid       MsgType = 0
	MsgTransactionsBatch MsgType = 1
	MsgAccountsBatch     MsgType = 2
	MsgEOF               MsgType = 3
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
