package codec

import "github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"

type Codec interface {
	EncodeEnvelope(envelope protocol.Envelope) ([]byte, error)

	EncodeTransactionBatch(transactions []protocol.Transaction) ([]byte, error)
	DecodeTransactionBatch(payload []byte) ([]protocol.Transaction, error)
}
