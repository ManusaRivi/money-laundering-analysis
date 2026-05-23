package data

import (
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external/codec"
)

// accountStream adapts a type accounts BatchReader to DatasetStream.
type AccountStream struct {
	reader *BatchReader[external.AccountData]
	codec  codec.Codec
}

func NewAccountStream(reader *BatchReader[external.AccountData], codec codec.Codec) *AccountStream {
	return &AccountStream{reader: reader, codec: codec}
}

func (a *AccountStream) GetNextBatch() ([]byte, error) {
	batch, err := a.reader.Next()
	if err != nil {
		return nil, err
	}
	return a.codec.EncodeAccountBatch(batch)
}

func (a *AccountStream) BatchMsgType() external.MsgType { return external.MsgAccountsBatch }
func (a *AccountStream) EOFMsgType() external.MsgType   { return external.MsgAccountsEOF }
func (a *AccountStream) Name() string                   { return "accounts" }
func (a *AccountStream) Close() error {
	return a.reader.Close()
}

// TransactionStream adapts a typed transactions BatchReader to DatasetStream.
type TransactionStream struct {
	reader *BatchReader[external.Transaction]
	codec  codec.Codec
}

func NewTransactionStream(reader *BatchReader[external.Transaction], codec codec.Codec) *TransactionStream {
	return &TransactionStream{reader: reader, codec: codec}
}

func (t *TransactionStream) GetNextBatch() ([]byte, error) {
	batch, err := t.reader.Next()
	if err != nil {
		return nil, err
	}
	return t.codec.EncodeTransactionBatch(batch)
}

func (t *TransactionStream) BatchMsgType() external.MsgType { return external.MsgTransactionsBatch }
func (t *TransactionStream) EOFMsgType() external.MsgType   { return external.MsgTransactionsEOF }
func (t *TransactionStream) Name() string                   { return "transactions" }
func (t *TransactionStream) Close() error {
	return t.reader.Close()
}
