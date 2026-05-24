package messagehandler

import (
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"
)

const TransactionResultBatchSize = 10

type MessageHandler struct {
	accountsTotal        int
	transactionsTotal    int
	filteredTransactions []external.Transaction
	nextResultIdx        int
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{
		accountsTotal:     0,
		transactionsTotal: 0,
	}
}

func (messageHandler *MessageHandler) HandleTransactionsBatch(transactions []external.Transaction) {
	for _, transaction := range transactions {
		if transaction.PaymentCurrency == "US Dollar" && transaction.AmountPaid < 50 {
			messageHandler.filteredTransactions = append(messageHandler.filteredTransactions, transaction)
		}
	}
	messageHandler.transactionsTotal += len(transactions)
}

// Receiving info from middleware would happen here
func (messageHandler *MessageHandler) GetTransactionResultBatch() []external.Transaction {
	remaining := len(messageHandler.filteredTransactions) - messageHandler.nextResultIdx
	if remaining <= 0 {
		return nil
	}
	size := TransactionResultBatchSize
	size = min(size, remaining)
	batch := messageHandler.filteredTransactions[messageHandler.nextResultIdx : messageHandler.nextResultIdx+size]
	messageHandler.nextResultIdx += size
	return batch
}

func (messageHandler *MessageHandler) HandleAccountsBatch(accounts []external.AccountData) {
	messageHandler.accountsTotal += len(accounts)
}

func (messageHandler *MessageHandler) HandleTransactionsEOF() {
	slog.Info("Received all transactions", slog.Int("total", messageHandler.transactionsTotal))
}

func (messageHandler *MessageHandler) HandleAccountsEOF() {
	slog.Info("Received all accounts", slog.Int("total", messageHandler.accountsTotal))
}

func (messageHandler *MessageHandler) SerializeDataMessage() (*broker.Message, error) {
	// TODO: Implement me!
	return nil, nil
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*broker.Message, error) {
	// TODO: Implement me!
	return nil, nil
}

func (messageHandler *MessageHandler) DeserializeResultMessage() {
	// TODO: Implement me!
}
