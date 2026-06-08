package codec

import (
	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"
)

type Codec interface {
	EncodeExternalEnvelope(envelope external.ExternalEnvelope) ([]byte, error)
	EncodeInternalEnvelope(envelope external.InternalEnvelope) ([]byte, error)

	DecodeInternalEnvelope(payload []byte) (external.InternalEnvelope, error)

	// EncodeEOFCounts/DecodeEOFCounts (de)serialise the per-key message counts
	// that a layer-to-layer EOF carries as its InternalEnvelope payload.
	EncodeEOFCounts(counts map[broker.KeyType]int) ([]byte, error)
	DecodeEOFCounts(payload []byte) (map[broker.KeyType]int, error)

	EncodeAccountBatch(accounts []external.AccountData) ([]byte, error)
	DecodeAccountBatch(payload []byte) ([]external.AccountData, error)

	EncodeTransactionBatch(transactions []external.Transaction) ([]byte, error)
	DecodeTransactionBatch(payload []byte) ([]external.Transaction, error)

	// EncodeTransactionBatchForClient(transactions []external.Transaction, clientId uuid.UUID) ([]byte, error)

	EncodeQuery1ResultBatch(result []external.Query1Result) ([]byte, error)
	DecodeQuery1ResultBatch(payload []byte) ([]external.Query1Result, error)

	EncodeQuery2ResultBatch(result []external.Query2Result) ([]byte, error)
	DecodeQuery2ResultBatch(payload []byte) ([]external.Query2Result, error)

	EncodeQuery3ResultBatch(result []external.Query3Result) ([]byte, error)
	DecodeQuery3ResultBatch(payload []byte) ([]external.Query3Result, error)

	EncodeQuery4ResultBatch(results []external.Query4Result) ([]byte, error)
	DecodeQuery4ResultBatch(payload []byte) ([]external.Query4Result, error)

	EncodeQuery5Result(result external.Query5Result) ([]byte, error)
	DecodeQuery5Result(payload []byte) (external.Query5Result, error)
}
