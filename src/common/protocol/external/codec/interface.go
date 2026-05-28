package codec

import "github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"

type Codec interface {
	EncodeEnvelope(envelope external.Envelope) ([]byte, error)

	EncodeAccountBatch(accounts []external.AccountData) ([]byte, error)
	DecodeAccountBatch(payload []byte) ([]external.AccountData, error)

	EncodeTransactionBatch(transactions []external.Transaction) ([]byte, error)
	DecodeTransactionBatch(payload []byte) ([]external.Transaction, error)

	EncodeQuery1ResultBatch(result []external.Query1Result) ([]byte, error)
	DecodeQuery1ResultBatch(payload []byte) ([]external.Query1Result, error)

	EncodeQuery2ResultBatch(result []external.Query2Result) ([]byte, error)
	DecodeQuery2ResultBatch(payload []byte) ([]external.Query2Result, error)

	EncodeQuery3ResultBatch(result []external.Query3Result) ([]byte, error)
	DecodeQuery3ResultBatch(payload []byte) ([]external.Query3Result, error)

	EncodeQuery5ResultBatch(result []external.Query5Result) ([]byte, error)
	DecodeQuery5ResultBatch(payload []byte) ([]external.Query5Result, error)
}
