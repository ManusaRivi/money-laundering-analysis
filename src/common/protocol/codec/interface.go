package codec

import (
	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/google/uuid"
)

type Codec interface {
	EncodeExternalEnvelope(envelope protocol.ExternalEnvelope) ([]byte, error)
	EncodeInternalEnvelope(envelope protocol.InternalEnvelope) ([]byte, error)

	DecodeInternalEnvelope(payload []byte) (protocol.InternalEnvelope, error)

	EncodeEOFCounts(counts map[broker.KeyType]int) ([]byte, error)
	DecodeEOFCounts(payload []byte) (map[broker.KeyType]int, error)
	EncodeEOFCountsEnvelope(clientId uuid.UUID, counts map[broker.KeyType]int) ([]byte, error)

	EncodeAccountBatch(accounts []protocol.AccountData) ([]byte, error)
	DecodeAccountBatch(payload []byte) ([]protocol.AccountData, error)

	EncodeAccountsEnvelope(clientId uuid.UUID, accounts []domain.Account) ([]byte, error)
	DecodeAccountsEnvelope(payload []byte) ([]domain.Account, error)

	EncodeTransactionBatch(transactions []protocol.Transaction) ([]byte, error)
	DecodeTransactionBatch(payload []byte) ([]protocol.Transaction, error)

	EncodeTxQ4PhaseOneBatchEnvelope(clientId uuid.UUID, txType domain.TypeTxQ4, txs []protocol.Transaction) ([]byte, error)
	DecodeTxQ4PhaseOneBatch(payload []byte) (domain.TypeTxQ4, []protocol.Transaction, error)

	EncodeTxQ4PhaseTwoBatchEnvelope(clientId uuid.UUID, pairs []domain.TxQ4PairCount) ([]byte, error)
	DecodeTxQ4PhaseTwoBatch(payload []byte) ([]domain.TxQ4PairCount, error)

	EncodeTxQ4PhaseThreeEnvelope(clientId uuid.UUID, txQ4 domain.TxQ4PhaseThree) ([]byte, error)
	DecodeTxQ4PhaseThreeEnvelope(payload []byte) (domain.TxQ4PhaseThree, error)

	EncodeQuery1ResultBatch(result []protocol.Query1Result) ([]byte, error)
	DecodeQuery1ResultBatch(payload []byte) ([]protocol.Query1Result, error)

	EncodeQuery2ResultBatch(result []protocol.Query2Result) ([]byte, error)
	DecodeQuery2ResultBatch(payload []byte) ([]protocol.Query2Result, error)

	EncodeQuery3ResultBatch(result []protocol.Query3Result) ([]byte, error)
	DecodeQuery3ResultBatch(payload []byte) ([]protocol.Query3Result, error)

	EncodeQuery4ResultEnvelope(clientId uuid.UUID, results map[domain.Account]struct{}) ([]byte, error)
	DecodeQuery4ResultPayload(payload []byte) ([]protocol.Query4Result, error)

	EncodeQuery5Result(result protocol.Query5Result) ([]byte, error)
	DecodeQuery5Result(payload []byte) (protocol.Query5Result, error)
}
