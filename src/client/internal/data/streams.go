package data

import (
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
)

// accountStream adapts a type accounts BatchReader to DatasetStream.
type AccountStream struct {
	reader *BatchReader[protocol.AccountData]
	codec  *codec.BinaryCodec
}

func NewAccountStream(reader *BatchReader[protocol.AccountData], codec *codec.BinaryCodec) *AccountStream {
	return &AccountStream{reader: reader, codec: codec}
}

func (a *AccountStream) GetNextBatch() ([]byte, error) {
	batch, err := a.reader.Next()
	if err != nil {
		return nil, err
	}
	return a.codec.EncodeAccountBatch(batch)
}

func (a *AccountStream) BatchMsgType() protocol.MsgType { return protocol.MsgAccountsBatch }
func (a *AccountStream) EOFMsgType() protocol.MsgType   { return protocol.MsgAccountsEOF }
func (a *AccountStream) Name() string                   { return "accounts" }

// TransactionStream adapts a typed transactions BatchReader to DatasetStream.
type TransactionStream struct {
	reader *BatchReader[protocol.Transaction]
	codec  *codec.BinaryCodec
}

func NewTransactionStream(reader *BatchReader[protocol.Transaction], codec *codec.BinaryCodec) *TransactionStream {
	return &TransactionStream{reader: reader, codec: codec}
}

func (t *TransactionStream) GetNextBatch() ([]byte, error) {
	batch, err := t.reader.Next()
	if err != nil {
		return nil, err
	}
	return t.codec.EncodeTransactionBatch(batch)
}

func (t *TransactionStream) BatchMsgType() protocol.MsgType { return protocol.MsgTransactionsBatch }
func (t *TransactionStream) EOFMsgType() protocol.MsgType   { return protocol.MsgTransactionsEOF }
func (t *TransactionStream) Name() string                   { return "transactions" }
