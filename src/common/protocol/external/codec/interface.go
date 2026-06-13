package codec

import (
	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"
	"github.com/google/uuid"
)

type Codec interface {
	EncodeExternalEnvelope(envelope external.ExternalEnvelope) ([]byte, error)
	EncodeInternalEnvelope(envelope external.InternalEnvelope) ([]byte, error)

	DecodeInternalEnvelope(payload []byte) (external.InternalEnvelope, error)

	// EncodeEOFCounts/DecodeEOFCounts (de)serialise the per-key message counts
	// that a layer-to-layer EOF carries as its InternalEnvelope payload.
	EncodeEOFCounts(counts map[broker.KeyType]int) ([]byte, error)
	DecodeEOFCounts(payload []byte) (map[broker.KeyType]int, error)
	EncodeEOFCountsEnvelope(clientId uuid.UUID, counts map[broker.KeyType]int) ([]byte, error)

	EncodeAccountBatch(accounts []external.AccountData) ([]byte, error)
	DecodeAccountBatch(payload []byte) ([]external.AccountData, error)

	EncodeTransactionBatch(transactions []external.Transaction) ([]byte, error)
	DecodeTransactionBatch(payload []byte) ([]external.Transaction, error)

	EncodeTxQ4PhaseOneEnvelope(clientId uuid.UUID, txQ4 domain.TxQ4PhaseOne) ([]byte, error)
	DecodeTxQ4PhaseOneEnvelope(payload []byte) (domain.TxQ4PhaseOne, error)

	EncodeTxQ4PhaseTwoEnvelope(clientId uuid.UUID, txQ4 domain.TxQ4PhaseTwo) ([]byte, error)
	DecodeTxQ4PhaseTwoEnvelope(payload []byte) (domain.TxQ4PhaseTwo, error)

	EncodeTxQ4PhaseThreeEnvelope(clientId uuid.UUID, txQ4 domain.TxQ4PhaseThree) ([]byte, error)
	DecodeTxQ4PhaseThreeEnvelope(payload []byte) (domain.TxQ4PhaseThree, error)

	EncodeAccountsEnvelope(clientId uuid.UUID, accounts []domain.Account) ([]byte, error)
	DecodeAccountsEnvelope(payload []byte) ([]domain.Account, error)

	// EncodeTransactionBatchForClient(transactions []external.Transaction, clientId uuid.UUID) ([]byte, error)

	EncodeQuery1ResultBatch(result []external.Query1Result) ([]byte, error)
	DecodeQuery1ResultBatch(payload []byte) ([]external.Query1Result, error)

	EncodeQuery2ResultBatch(result []external.Query2Result) ([]byte, error)
	DecodeQuery2ResultBatch(payload []byte) ([]external.Query2Result, error)

	EncodeQuery3ResultBatch(result []external.Query3Result) ([]byte, error)
	DecodeQuery3ResultBatch(payload []byte) ([]external.Query3Result, error)

	// EncodeQuery4ResultBatch(results []external.Query4Result) ([]byte, error)
	EncodeQuery4ResultEnvelope(clientId uuid.UUID, results map[domain.Account]struct{}) ([]byte, error)
	DecodeQuery4ResultPayload(payload []byte) ([]external.Query4Result, error)

	EncodeQuery5Result(result external.Query5Result) ([]byte, error)
	DecodeQuery5Result(payload []byte) (external.Query5Result, error)
}
