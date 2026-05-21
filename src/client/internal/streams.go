package client

import (
	"github.com/ManusaRivi/money-laundering-analysis/src/client/internal/data"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
)

type accountStream struct {
	reader *data.BatchReader[protocol.AccountData]
	codec  *codec.BinaryCodec
}

func NewAccountStream(reader *data.BatchReader[protocol.AccountData], codec *codec.BinaryCodec) *accountStream {
	return &accountStream{reader: reader, codec: codec}
}

func (a *accountStream) Next() ([]byte, error) {
	batch, err := a.reader.Next()
	if err != nil {
		return nil, err
	}
	return a.codec.EncodeAccountBatch(batch)
}

func (a *accountStream) BatchMsgType() protocol.MsgType { return protocol.MsgAccountsBatch }
func (a *accountStream) EOFMsgType() protocol.MsgType   { return protocol.MsgAccountsEOF }
func (a *accountStream) Name() string                   { return "accounts" }

// transactionStream adapts a typed transactions BatchReader to DatasetStream.
type transactionStream struct {
	reader *data.BatchReader[protocol.Transaction]
	codec  *codec.BinaryCodec
}

func NewTransactionStream(reader *data.BatchReader[protocol.Transaction], codec *codec.BinaryCodec) *transactionStream {
	return &transactionStream{reader: reader, codec: codec}
}

func (t *transactionStream) Next() ([]byte, error) {
	batch, err := t.reader.Next()
	if err != nil {
		return nil, err
	}
	return t.codec.EncodeTransactionBatch(batch)
}

func (t *transactionStream) BatchMsgType() protocol.MsgType { return protocol.MsgTransactionsBatch }
func (t *transactionStream) EOFMsgType() protocol.MsgType   { return protocol.MsgTransactionsEOF }
func (t *transactionStream) Name() string                   { return "transactions" }
