package codec

import "github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"

type Codec interface {
	EncodeEnvelope(envelope external.Envelope) ([]byte, error)

	EncodeTransactionBatch(transactions []external.Transaction) ([]byte, error)
	DecodeTransactionBatch(payload []byte) ([]external.Transaction, error)
}
