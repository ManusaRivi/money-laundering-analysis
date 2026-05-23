package codec

import "github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"

type Codec interface {
	EncodeEnvelope(envelope protocol.Envelope) ([]byte, error)

	EncodeAccountBatch(accounts []protocol.AccountData) ([]byte, error)
	DecodeAccountBatch(payload []byte) ([]protocol.AccountData, error)

	EncodeTransactionBatch(transactions []protocol.Transaction) ([]byte, error)
	DecodeTransactionBatch(payload []byte) ([]protocol.Transaction, error)

	EncodeQuery1ResultBatch(results []protocol.Query1Result) ([]byte, error)
	DecodeQuery1ResultBatch(payload []byte) ([]protocol.Query1Result, error)
}
