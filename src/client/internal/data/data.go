package data

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
)

// RecordParser turns a single CSV row into a typed record.
type RecordParser[T any] func(row []string) (T, error)

// BatchReader reads a CSV file and yields records in fixed-size batches.
// T is the concrete record type (e.g. protocol.Transaction, protocol.AccountData).
type BatchReader[T any] struct {
	file      *os.File
	reader    *csv.Reader
	parse     RecordParser[T]
	batchSize int
	done      bool
}

// NewBatchReader opens path and prepares a CSV reader that produces batches of
// `batchSize` records parsed via `parse`. The first row is assumed to be a
// header and is skipped.
func NewBatchReader[T any](path string, batchSize int, parse RecordParser[T]) (*BatchReader[T], error) {
	if batchSize <= 0 {
		return nil, fmt.Errorf("batch size must be positive, got %d", batchSize)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", path, err)
	}
	r := csv.NewReader(f)
	r.ReuseRecord = true
	if _, err := r.Read(); err != nil {
		f.Close()
		if err == io.EOF {
			return nil, fmt.Errorf("file %s is empty", path)
		}
		return nil, fmt.Errorf("reading header from %s: %w", path, err)
	}
	return &BatchReader[T]{
		file:      f,
		reader:    r,
		parse:     parse,
		batchSize: batchSize,
	}, nil
}

// Next returns the next batch of records. The returned slice has up to
// batchSize elements; a shorter slice signals the final batch. When the file
// is exhausted, Next returns (nil, io.EOF).
func (b *BatchReader[T]) Next() ([]T, error) {
	if b.done {
		return nil, io.EOF
	}
	batch := make([]T, 0, b.batchSize)
	for len(batch) < b.batchSize {
		row, err := b.reader.Read()
		if err == io.EOF {
			b.done = true
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading csv row: %w", err)
		}
		rec, err := b.parse(row)
		if err != nil {
			return nil, fmt.Errorf("parsing csv row: %w", err)
		}
		batch = append(batch, rec)
	}
	if len(batch) == 0 {
		return nil, io.EOF
	}
	return batch, nil
}

func (b *BatchReader[T]) Close() error {
	return b.file.Close()
}

const (
	TransactionColumnAmount = 11

	FloatBitSize = 64

	TimestampColumnIndex         = 0
	FromBankColumnIndex          = 1
	FromAccountColumnIndex       = 2
	ToBankColumnIndex            = 3
	ToAccountColumnIndex         = 4
	AmountReceivedColumnIndex    = 5
	ReceivingCurrencyColumnIndex = 6
	AmountPaidColumnIndex        = 7
	PaymentCurrencyColumnIndex   = 8
	PaymentFormatColumnIndex     = 9
	IsLaunderingColumnIndex      = 10

	AccountColumnAmount = 5

	BankNameColumnIndex   = 0
	BankIDColumnIndex     = 1
	AccountNumberIndex    = 2
	EntityIDColumnIndex   = 3
	EntityNameColumnIndex = 4
)

// ParseTransaction parses a CSV row into a protocol.Transaction.
// Expected columns (in order):
//
//	Timestamp, FromBank, FromAccount, ToBank, ToAccount,
//	AmountReceived, ReceivingCurrency, AmountPaid, PaymentCurrency,
//	PaymentFormat, IsLaundering
func ParseTransaction(row []string) (protocol.Transaction, error) {
	const expected = TransactionColumnAmount
	if len(row) != expected {
		return protocol.Transaction{}, fmt.Errorf("expected %d columns, got %d", expected, len(row))
	}
	amountReceived, err := strconv.ParseFloat(row[AmountReceivedColumnIndex], FloatBitSize)
	if err != nil {
		return protocol.Transaction{}, fmt.Errorf("amount received: %w", err)
	}
	amountPaid, err := strconv.ParseFloat(row[AmountPaidColumnIndex], FloatBitSize)
	if err != nil {
		return protocol.Transaction{}, fmt.Errorf("amount paid: %w", err)
	}
	isLaundering, err := strconv.ParseBool(row[IsLaunderingColumnIndex])
	if err != nil {
		return protocol.Transaction{}, fmt.Errorf("is laundering: %w", err)
	}
	return protocol.Transaction{
		Timestamp:         row[TimestampColumnIndex],
		FromBank:          row[FromBankColumnIndex],
		FromAccount:       row[FromAccountColumnIndex],
		ToBank:            row[ToBankColumnIndex],
		ToAccount:         row[ToAccountColumnIndex],
		AmountReceived:    amountReceived,
		ReceivingCurrency: row[ReceivingCurrencyColumnIndex],
		AmountPaid:        amountPaid,
		PaymentCurrency:   row[PaymentCurrencyColumnIndex],
		PaymentFormat:     row[PaymentFormatColumnIndex],
		IsLaundering:      isLaundering,
	}, nil
}

// ParseAccount parses a CSV row into a protocol.AccountData.
// Expected columns: BankName, BankID, AccountNumber, EntityID, EntityName.
func ParseAccount(row []string) (protocol.AccountData, error) {
	const expected = AccountColumnAmount
	if len(row) != expected {
		return protocol.AccountData{}, fmt.Errorf("expected %d columns, got %d", expected, len(row))
	}
	return protocol.AccountData{
		BankName:      row[BankNameColumnIndex],
		BankID:        row[BankIDColumnIndex],
		AccountNumber: row[AccountNumberIndex],
		EntityID:      row[EntityIDColumnIndex],
		EntityName:    row[EntityNameColumnIndex],
	}, nil
}

func WriteResultsToOutput(path string, results <-chan []string) {
	// TODO: Implement me!!
}
