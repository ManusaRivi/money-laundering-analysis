package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sort"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/google/uuid"
)

const (
	ExternalHeaderSize = 5
	InternalHeaderSize = 21

	MsgTypeHeaderBytes     = 1
	ClientIDHeaderBytes    = 16
	PayloadSizeHeaderBytes = 4
)

func DecodeExternalHeader(header []byte) (protocol.MsgType, uint32) {
	msgType := protocol.MsgType(header[0])
	payloadLen := binary.BigEndian.Uint32(header[MsgTypeHeaderBytes : MsgTypeHeaderBytes+PayloadSizeHeaderBytes])
	return msgType, payloadLen
}

func DecodeInternalHeader(header []byte) (protocol.MsgType, uuid.UUID, uint32) {
	msgType := protocol.MsgType(header[0])
	clientId, err := uuid.FromBytes(header[MsgTypeHeaderBytes : MsgTypeHeaderBytes+ClientIDHeaderBytes])
	if err != nil {
		return msgType, uuid.Nil, 0
	}
	payloadLen := binary.BigEndian.Uint32(header[MsgTypeHeaderBytes+ClientIDHeaderBytes : MsgTypeHeaderBytes+ClientIDHeaderBytes+PayloadSizeHeaderBytes])
	return msgType, clientId, payloadLen
}

type BinaryCodec struct{}

func New() *BinaryCodec { return &BinaryCodec{} }

// --- envelope ---

// TODO: Client ID: FIrst item in Internal header

func (BinaryCodec) EncodeExternalEnvelope(envelope protocol.ExternalEnvelope) ([]byte, error) {
	buffer := make([]byte, MsgTypeHeaderBytes+PayloadSizeHeaderBytes+len(envelope.Payload))
	buffer[0] = byte(envelope.MsgType)
	binary.BigEndian.PutUint32(buffer[MsgTypeHeaderBytes:MsgTypeHeaderBytes+PayloadSizeHeaderBytes], uint32(len(envelope.Payload)))
	copy(buffer[MsgTypeHeaderBytes+PayloadSizeHeaderBytes:], envelope.Payload)
	return buffer, nil
}

func (BinaryCodec) EncodeInternalEnvelope(envelope protocol.InternalEnvelope) ([]byte, error) {
	buffer := make([]byte, MsgTypeHeaderBytes+ClientIDHeaderBytes+PayloadSizeHeaderBytes+len(envelope.Payload))
	buffer[0] = byte(envelope.MsgType)
	copy(buffer[MsgTypeHeaderBytes:MsgTypeHeaderBytes+ClientIDHeaderBytes], envelope.ClientId[:])
	binary.BigEndian.PutUint32(buffer[MsgTypeHeaderBytes+ClientIDHeaderBytes:MsgTypeHeaderBytes+ClientIDHeaderBytes+PayloadSizeHeaderBytes], uint32(len(envelope.Payload)))
	copy(buffer[MsgTypeHeaderBytes+ClientIDHeaderBytes+PayloadSizeHeaderBytes:], envelope.Payload)
	return buffer, nil
}

func (BinaryCodec) DecodeInternalEnvelope(message []byte) (protocol.InternalEnvelope, error) {
	msgType, ClientId, payloadLen := DecodeInternalHeader(message[:InternalHeaderSize])
	if uint32(len(message)-InternalHeaderSize) < payloadLen {
		return protocol.InternalEnvelope{}, fmt.Errorf("payload length mismatch: header says %d bytes but only %d bytes remain", payloadLen, len(message)-InternalHeaderSize)
	}
	payload := message[InternalHeaderSize : InternalHeaderSize+payloadLen]
	return protocol.InternalEnvelope{
		MsgType:  msgType,
		ClientId: ClientId,
		Payload:  payload,
	}, nil
}

// ====================
// ===== Accounts =====
// ====================

func (BinaryCodec) EncodeAccountBatch(accounts []protocol.AccountData) ([]byte, error) {
	var batch bytes.Buffer
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], uint32(len(accounts)))
	batch.Write(count[:])

	for i, a := range accounts {
		var accBuf bytes.Buffer
		if err := encodeAccountData(&accBuf, a.BankName, a.BankID, a.AccountNumber, a.EntityID, a.EntityName); err != nil {
			return nil, fmt.Errorf("encoding account %d: %w", i, err)
		}
		if accBuf.Len() > math.MaxUint16 {
			return nil, fmt.Errorf("account %d too large: %d bytes (max %d)", i, accBuf.Len(), math.MaxUint16)
		}
		var length [2]byte
		binary.BigEndian.PutUint16(length[:], uint16(accBuf.Len()))
		batch.Write(length[:])
		batch.Write(accBuf.Bytes())
	}
	return batch.Bytes(), nil
}

func (BinaryCodec) DecodeAccountBatch(payload []byte) ([]protocol.AccountData, error) {
	r := bytes.NewReader(payload)
	var countBytes [4]byte
	if _, err := io.ReadFull(r, countBytes[:]); err != nil {
		return nil, fmt.Errorf("reading batch count: %w", err)
	}
	count := binary.BigEndian.Uint32(countBytes[:])

	accounts := make([]protocol.AccountData, 0, count)
	for i := uint32(0); i < count; i++ {
		var lengthBytes [2]byte
		if _, err := io.ReadFull(r, lengthBytes[:]); err != nil {
			return nil, fmt.Errorf("reading length of account %d: %w", i, err)
		}
		length := binary.BigEndian.Uint16(lengthBytes[:])

		accBytes := make([]byte, length)
		if _, err := io.ReadFull(r, accBytes); err != nil {
			return nil, fmt.Errorf("reading account %d body: %w", i, err)
		}
		account, err := decodeAccountData(bytes.NewReader(accBytes))
		if err != nil {
			return nil, fmt.Errorf("decoding account %d: %w", i, err)
		}
		accounts = append(accounts, account)
	}
	return accounts, nil
}

// Layout:
//   [uint32 count][uint16 len][tx bytes][uint16 len][tx bytes]...

// ====================
// === Transactions ===
// ====================

func (BinaryCodec) EncodeTransactionBatch(transactions []protocol.Transaction) ([]byte, error) {
	var batch bytes.Buffer
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], uint32(len(transactions)))
	batch.Write(count[:])

	for i, t := range transactions {
		var txBuf bytes.Buffer
		if err := encodeTransaction(&txBuf, t); err != nil {
			return nil, fmt.Errorf("encoding tx %d: %w", i, err)
		}
		if txBuf.Len() > math.MaxUint16 {
			return nil, fmt.Errorf("tx %d too large: %d bytes (max %d)", i, txBuf.Len(), math.MaxUint16)
		}
		var length [2]byte
		binary.BigEndian.PutUint16(length[:], uint16(txBuf.Len()))
		batch.Write(length[:])
		batch.Write(txBuf.Bytes())
	}
	return batch.Bytes(), nil
}

func (BinaryCodec) DecodeTransactionBatch(payload []byte) ([]protocol.Transaction, error) {
	r := bytes.NewReader(payload)
	var countBytes [4]byte
	if _, err := io.ReadFull(r, countBytes[:]); err != nil {
		return nil, fmt.Errorf("reading batch count: %w", err)
	}
	count := binary.BigEndian.Uint32(countBytes[:])

	txs := make([]protocol.Transaction, 0, count)
	for i := uint32(0); i < count; i++ {
		var lengthBytes [2]byte
		if _, err := io.ReadFull(r, lengthBytes[:]); err != nil {
			return nil, fmt.Errorf("reading length of tx %d: %w", i, err)
		}
		length := binary.BigEndian.Uint16(lengthBytes[:])

		txBytes := make([]byte, length)
		if _, err := io.ReadFull(r, txBytes); err != nil {
			return nil, fmt.Errorf("reading tx %d body: %w", i, err)
		}
		tx, err := decodeTransaction(bytes.NewReader(txBytes))
		if err != nil {
			return nil, fmt.Errorf("decoding tx %d: %w", i, err)
		}
		txs = append(txs, tx)
	}
	return txs, nil
}

func encodeAccountData(buf *bytes.Buffer, bankName, bankID, accountNumber, entityID, entityName string) error {
	if err := writeString(buf, bankName); err != nil {
		return err
	}
	if err := writeString(buf, bankID); err != nil {
		return err
	}
	if err := writeString(buf, accountNumber); err != nil {
		return err
	}
	if err := writeString(buf, entityID); err != nil {
		return err
	}
	return writeString(buf, entityName)
}

func decodeAccountData(r *bytes.Reader) (protocol.AccountData, error) {
	var a protocol.AccountData
	var err error
	if a.BankName, err = readString(r); err != nil {
		return a, fmt.Errorf("bank name: %w", err)
	}
	if a.BankID, err = readString(r); err != nil {
		return a, fmt.Errorf("bank id: %w", err)
	}
	if a.AccountNumber, err = readString(r); err != nil {
		return a, fmt.Errorf("account number: %w", err)
	}
	if a.EntityID, err = readString(r); err != nil {
		return a, fmt.Errorf("entity id: %w", err)
	}
	if a.EntityName, err = readString(r); err != nil {
		return a, fmt.Errorf("entity name: %w", err)
	}
	return a, nil
}

func encodeTransactionAccount(buf *bytes.Buffer, bank, number string) error {
	if err := writeString(buf, bank); err != nil {
		return err
	}
	return writeString(buf, number)
}

func decodeTransactionAccount(r *bytes.Reader) (string, string, error) {
	bank, err := readString(r)
	if err != nil {
		return "", "", err
	}
	number, err := readString(r)
	if err != nil {
		return "", "", err
	}
	return bank, number, nil
}

func encodeMoney(buf *bytes.Buffer, amount float64, currency string) error {
	writeFloat64(buf, amount)
	return writeString(buf, currency)
}

func decodeMoney(r *bytes.Reader) (float64, string, error) {
	amount, err := readFloat64(r)
	if err != nil {
		return 0, "", err
	}
	currency, err := readString(r)
	if err != nil {
		return 0, "", err
	}
	return amount, currency, nil
}

func encodeTransaction(buf *bytes.Buffer, t protocol.Transaction) error {
	if err := writeString(buf, t.Timestamp); err != nil {
		return err
	}
	// From Bank
	if err := encodeTransactionAccount(buf, t.FromBank, t.FromAccount); err != nil {
		return err
	}
	// To Bank
	if err := encodeTransactionAccount(buf, t.ToBank, t.ToAccount); err != nil {
		return err
	}
	// Receiving Money
	if err := encodeMoney(buf, t.AmountReceived, t.ReceivingCurrency); err != nil {
		return err
	}
	if err := encodeMoney(buf, t.AmountPaid, t.PaymentCurrency); err != nil {
		return err
	}
	// Payment Money
	if err := writeString(buf, t.PaymentFormat); err != nil {
		return err
	}
	writeBool(buf, t.IsLaundering)
	return nil
}

func decodeTransaction(r *bytes.Reader) (protocol.Transaction, error) {
	var t protocol.Transaction
	var err error
	if t.Timestamp, err = readString(r); err != nil {
		return t, err
	}
	if t.FromBank, t.FromAccount, err = decodeTransactionAccount(r); err != nil {
		return t, err
	}
	if t.ToBank, t.ToAccount, err = decodeTransactionAccount(r); err != nil {
		return t, err
	}
	if t.AmountReceived, t.ReceivingCurrency, err = decodeMoney(r); err != nil {
		return t, err
	}
	if t.AmountPaid, t.PaymentCurrency, err = decodeMoney(r); err != nil {
		return t, err
	}
	if t.PaymentFormat, err = readString(r); err != nil {
		return t, err
	}
	if t.IsLaundering, err = readBool(r); err != nil {
		return t, err
	}
	return t, nil
}

// ====================
// =====   txQ4   =====
// ====================

// Phase-one messages are batched: a single TxQ4 type (scatter|gather) followed
// by a transaction batch. The spliter groups transactions by shard so each
// phase-one worker receives one message per (shard, type) instead of one per
// transaction.
//
// Layout: [writeString type][transaction batch]
func (p *BinaryCodec) EncodeTxQ4PhaseOneBatchEnvelope(clientId uuid.UUID, txType domain.TypeTxQ4, txs []protocol.Transaction) ([]byte, error) {
	var buf bytes.Buffer
	if err := writeString(&buf, string(txType)); err != nil {
		return nil, fmt.Errorf("failed to encode TxQ4 batch type: %w", err)
	}
	txBatch, err := p.EncodeTransactionBatch(txs)
	if err != nil {
		return nil, fmt.Errorf("failed to encode TxQ4 batch transactions: %w", err)
	}
	buf.Write(txBatch)
	envelope := protocol.InternalEnvelope{
		MsgType:  protocol.MsgTxQ4,
		ClientId: clientId,
		Payload:  buf.Bytes(),
	}
	return p.EncodeInternalEnvelope(envelope)
}

func (p *BinaryCodec) DecodeTxQ4PhaseOneBatch(payload []byte) (domain.TypeTxQ4, []protocol.Transaction, error) {
	r := bytes.NewReader(payload)
	txTypeStr, err := readString(r)
	if err != nil {
		return "", nil, fmt.Errorf("failed to read TxQ4 batch type: %w", err)
	}
	// Whatever readString did not consume is the transaction batch.
	txs, err := p.DecodeTransactionBatch(payload[len(payload)-r.Len():])
	if err != nil {
		return "", nil, fmt.Errorf("failed to decode TxQ4 batch transactions: %w", err)
	}
	return domain.TypeTxQ4(txTypeStr), txs, nil
}

// Phase-two messages are batched: the aggregator groups the pairs destined for
// the same accumulator shard and sends them as one message. Each entry is just
// the pair key and its partial bridge count — the two accounts are recoverable
// from the key (each side is an Account.GetID()), so they are not re-sent.
//
// Layout: standard batch framing over ( [string Src][string Dst][int64 Count] ).
func encodeTxQ4PairCount(buf *bytes.Buffer, pc domain.TxQ4PairCount) error {
	if err := writeString(buf, pc.Key.Src); err != nil {
		return err
	}
	if err := writeString(buf, pc.Key.Dst); err != nil {
		return err
	}
	writeInt64(buf, int64(pc.Count))
	return nil
}

func decodeTxQ4PairCount(r *bytes.Reader) (domain.TxQ4PairCount, error) {
	var pc domain.TxQ4PairCount
	var err error
	if pc.Key.Src, err = readString(r); err != nil {
		return pc, fmt.Errorf("pair source: %w", err)
	}
	if pc.Key.Dst, err = readString(r); err != nil {
		return pc, fmt.Errorf("pair destination: %w", err)
	}
	count, err := readInt64(r)
	if err != nil {
		return pc, fmt.Errorf("pair count: %w", err)
	}
	pc.Count = int(count)
	return pc, nil
}

func (p *BinaryCodec) EncodeTxQ4PhaseTwoBatchEnvelope(clientId uuid.UUID, pairs []domain.TxQ4PairCount) ([]byte, error) {
	payload, err := encodeBatch(pairs, encodeTxQ4PairCount)
	if err != nil {
		return nil, fmt.Errorf("failed to encode TxQ4 phase-two batch: %w", err)
	}
	envelope := protocol.InternalEnvelope{
		MsgType:  protocol.MsgTxQ4,
		ClientId: clientId,
		Payload:  payload,
	}
	return p.EncodeInternalEnvelope(envelope)
}

func (BinaryCodec) DecodeTxQ4PhaseTwoBatch(payload []byte) ([]domain.TxQ4PairCount, error) {
	return decodeBatch(payload, decodeTxQ4PairCount)
}

//	type TxQ4PairEntry struct {
//		Count      int
//		SrcAccount Account
//		DstAccount Account
//	}
//
//	type TxQ4PhaseThree struct {
//		ScatterGather map[string]*TxQ4PairEntry
//	}
func encodeTxQ4PhaseThree(buf *bytes.Buffer, txQ4 domain.TxQ4PhaseThree) error {
	keys := make([]string, 0, len(txQ4.ScatterGather))
	for k := range txQ4.ScatterGather {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	writeInt64(buf, int64(len(keys)))
	for _, k := range keys {
		entry := txQ4.ScatterGather[k]
		if err := writeString(buf, k); err != nil {
			return err
		}
		writeInt64(buf, int64(entry.Count))
		if err := encodeTransactionAccount(buf, entry.SrcAccount.BankID, entry.SrcAccount.ID); err != nil {
			return err
		}
		if err := encodeTransactionAccount(buf, entry.DstAccount.BankID, entry.DstAccount.ID); err != nil {
			return err
		}
	}
	return nil
}

func (p *BinaryCodec) EncodeTxQ4PhaseThreeEnvelope(clientId uuid.UUID, txQ4 domain.TxQ4PhaseThree) ([]byte, error) {
	buf := bytes.Buffer{}
	err := encodeTxQ4PhaseThree(&buf, txQ4)
	if err != nil {
		return nil, fmt.Errorf("failed to encode TxQ4PhaseThree: %w", err)
	}
	envelope := protocol.InternalEnvelope{
		MsgType:  protocol.MsgTxQ4,
		ClientId: clientId,
		Payload:  buf.Bytes(),
	}
	return p.EncodeInternalEnvelope(envelope)
}

func (p *BinaryCodec) DecodeTxQ4PhaseThreeEnvelope(payload []byte) (domain.TxQ4PhaseThree, error) {
	r := bytes.NewReader(payload)
	count, err := readInt64(r)
	if err != nil {
		return domain.TxQ4PhaseThree{}, fmt.Errorf("failed to read count in TxQ4PhaseThree: %w", err)
	}

	scatterGather := make(map[string]*domain.TxQ4PairEntry, count)
	for i := int64(0); i < count; i++ {
		key, err := readString(r)
		if err != nil {
			return domain.TxQ4PhaseThree{}, fmt.Errorf("failed to read key of entry %d in TxQ4PhaseThree: %w", i, err)
		}
		entryCount, err := readInt64(r)
		if err != nil {
			return domain.TxQ4PhaseThree{}, fmt.Errorf("failed to read count of entry %d in TxQ4PhaseThree: %w", i, err)
		}
		srcBank, srcAccountId, err := decodeTransactionAccount(r)
		if err != nil {
			return domain.TxQ4PhaseThree{}, fmt.Errorf("failed to decode source account of entry %d in TxQ4PhaseThree: %w", i, err)
		}
		dstBank, dstAccountId, err := decodeTransactionAccount(r)
		if err != nil {
			return domain.TxQ4PhaseThree{}, fmt.Errorf("failed to decode destination account of entry %d in TxQ4PhaseThree: %w", i, err)
		}
		scatterGather[key] = &domain.TxQ4PairEntry{
			Count: int(entryCount),
			SrcAccount: domain.Account{
				BankID: srcBank,
				ID:     srcAccountId,
			},
			DstAccount: domain.Account{
				BankID: dstBank,
				ID:     dstAccountId,
			},
		}
	}

	return domain.TxQ4PhaseThree{
		ScatterGather: scatterGather,
	}, nil
}

//	type Account struct {
//		BankID string
//		ID     string
//	}
func (p *BinaryCodec) EncodeAccountsEnvelope(clientID uuid.UUID, accounts []domain.Account) ([]byte, error) {
	buf := bytes.Buffer{}
	for _, account := range accounts {
		if err := encodeTransactionAccount(&buf, account.BankID, account.ID); err != nil {
			return nil, fmt.Errorf("encoding account %s-%s: %w", account.BankID, account.ID, err)
		}
	}
	envelope := protocol.InternalEnvelope{
		MsgType:  protocol.MsgTxAccounts,
		ClientId: clientID,
		Payload:  buf.Bytes(),
	}
	return p.EncodeInternalEnvelope(envelope)
}

func (p *BinaryCodec) DecodeAccountsEnvelope(payload []byte) ([]domain.Account, error) {
	r := bytes.NewReader(payload)
	accounts := []domain.Account{}
	for r.Len() > 0 {
		bankID, accountID, err := decodeTransactionAccount(r)
		if err != nil {
			return nil, fmt.Errorf("decoding account: %w", err)
		}
		accounts = append(accounts, domain.Account{
			BankID: bankID,
			ID:     accountID,
		})
	}
	return accounts, nil
}

// ====================
// ===== Query  1 =====
// ====================

func encodeQuery1Result(buf *bytes.Buffer, r protocol.Query1Result) error {
	if err := encodeTransactionAccount(buf, r.FromBank, r.FromAccount); err != nil {
		return err
	}
	if err := encodeTransactionAccount(buf, r.ToBank, r.ToAccount); err != nil {
		return err
	}
	writeFloat64(buf, r.AmountPaid)
	return nil
}

func decodeQuery1Result(r *bytes.Reader) (protocol.Query1Result, error) {
	var res protocol.Query1Result
	var err error
	if res.FromBank, res.FromAccount, err = decodeTransactionAccount(r); err != nil {
		return res, fmt.Errorf("from account: %w", err)
	}
	if res.ToBank, res.ToAccount, err = decodeTransactionAccount(r); err != nil {
		return res, fmt.Errorf("to account: %w", err)
	}
	if res.AmountPaid, err = readFloat64(r); err != nil {
		return res, fmt.Errorf("amount paid: %w", err)
	}
	return res, nil
}

func (BinaryCodec) EncodeQuery1ResultBatch(results []protocol.Query1Result) ([]byte, error) {
	return encodeBatch(results, encodeQuery1Result)
}

func (BinaryCodec) DecodeQuery1ResultBatch(payload []byte) ([]protocol.Query1Result, error) {
	return decodeBatch(payload, decodeQuery1Result)
}

// ====================
// ===== Query  2 =====
// ====================

func encodeQuery2Result(buf *bytes.Buffer, r protocol.Query2Result) error {
	if err := encodeTransactionAccount(buf, r.FromBank, r.FromAccount); err != nil {
		return err
	}
	writeString(buf, r.BankName)
	writeFloat64(buf, r.AmountPaid)
	return nil
}

func decodeQuery2Result(r *bytes.Reader) (protocol.Query2Result, error) {
	var res protocol.Query2Result
	var err error
	if res.FromBank, res.FromAccount, err = decodeTransactionAccount(r); err != nil {
		return res, fmt.Errorf("from account: %w", err)
	}
	if res.BankName, err = readString(r); err != nil {
		return res, fmt.Errorf("bank name: %w", err)
	}
	if res.AmountPaid, err = readFloat64(r); err != nil {
		return res, fmt.Errorf("amount paid: %w", err)
	}
	return res, nil
}

func (BinaryCodec) EncodeQuery2ResultBatch(results []protocol.Query2Result) ([]byte, error) {
	return encodeBatch(results, encodeQuery2Result)
}

func (BinaryCodec) DecodeQuery2ResultBatch(payload []byte) ([]protocol.Query2Result, error) {
	return decodeBatch(payload, decodeQuery2Result)
}

// ====================
// ===== Query  3 =====
// ====================
func encodeQuery3Result(buf *bytes.Buffer, r protocol.Query3Result) error {
	if err := encodeTransactionAccount(buf, r.FromBank, r.FromAccount); err != nil {
		return err
	}
	if err := writeString(buf, r.PaymentFormat); err != nil {
		return err
	}
	writeFloat64(buf, r.AmountPaid)
	return nil

}

func decodeQuery3Result(r *bytes.Reader) (protocol.Query3Result, error) {
	var res protocol.Query3Result
	var err error
	if res.FromBank, res.FromAccount, err = decodeTransactionAccount(r); err != nil {
		return res, fmt.Errorf("from account: %w", err)
	}
	if res.PaymentFormat, err = readString(r); err != nil {
		return res, fmt.Errorf("payment format: %w", err)
	}
	if res.AmountPaid, err = readFloat64(r); err != nil {
		return res, fmt.Errorf("amount paid: %w", err)
	}
	return res, nil
}

func (BinaryCodec) EncodeQuery3ResultBatch(results []protocol.Query3Result) ([]byte, error) {
	var batch bytes.Buffer
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], uint32(len(results)))
	batch.Write(count[:])

	for i, r := range results {
		var resBuf bytes.Buffer
		if err := encodeQuery3Result(&resBuf, r); err != nil {
			return nil, fmt.Errorf("encoding result %d: %w", i, err)
		}
		if resBuf.Len() > math.MaxUint16 {
			return nil, fmt.Errorf("result %d too large: %d bytes (max %d)", i, resBuf.Len(), math.MaxUint16)
		}
		var length [2]byte
		binary.BigEndian.PutUint16(length[:], uint16(resBuf.Len()))
		batch.Write(length[:])
		batch.Write(resBuf.Bytes())
	}
	return batch.Bytes(), nil
}

func (BinaryCodec) DecodeQuery3ResultBatch(payload []byte) ([]protocol.Query3Result, error) {
	r := bytes.NewReader(payload)
	var countBytes [4]byte
	if _, err := io.ReadFull(r, countBytes[:]); err != nil {
		return nil, fmt.Errorf("reading batch count: %w", err)
	}
	count := binary.BigEndian.Uint32(countBytes[:])

	results := make([]protocol.Query3Result, 0, count)
	for i := uint32(0); i < count; i++ {
		var lengthBytes [2]byte
		if _, err := io.ReadFull(r, lengthBytes[:]); err != nil {
			return nil, fmt.Errorf("reading length of result %d: %w", i, err)
		}
		length := binary.BigEndian.Uint16(lengthBytes[:])

		resBytes := make([]byte, length)
		if _, err := io.ReadFull(r, resBytes); err != nil {
			return nil, fmt.Errorf("reading result %d body: %w", i, err)
		}
		result, err := decodeQuery3Result(bytes.NewReader(resBytes))
		if err != nil {
			return nil, fmt.Errorf("decoding result %d: %w", i, err)
		}
		results = append(results, result)
	}
	return results, nil
}

// ====================
// ===== Query  4 =====
// ====================

func encodeQuery4Result(buf *bytes.Buffer, r domain.Account) error {
	if err := writeString(buf, r.BankID); err != nil {
		return err
	}
	return writeString(buf, r.ID)
}

func decodeQuery4Result(r *bytes.Reader) (protocol.Query4Result, error) {
	var res protocol.Query4Result
	var err error
	if res.BankID, err = readString(r); err != nil {
		return res, fmt.Errorf("bank id: %w", err)
	}
	if res.ID, err = readString(r); err != nil {
		return res, fmt.Errorf("id: %w", err)
	}
	return res, nil
}

func (p *BinaryCodec) EncodeQuery4ResultEnvelope(clientId uuid.UUID, results map[domain.Account]struct{}) ([]byte, error) {
	var batch bytes.Buffer
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], uint32(len(results)))
	batch.Write(count[:])

	i := 0
	for r := range results {
		var resBuf bytes.Buffer
		if err := encodeQuery4Result(&resBuf, r); err != nil {
			return nil, fmt.Errorf("encoding result %d: %w", i, err)
		}
		if resBuf.Len() > math.MaxUint16 {
			return nil, fmt.Errorf("result %d too large: %d bytes (max %d)", i, resBuf.Len(), math.MaxUint16)
		}
		var length [2]byte
		binary.BigEndian.PutUint16(length[:], uint16(resBuf.Len()))
		batch.Write(length[:])
		batch.Write(resBuf.Bytes())
		i++
	}
	envelope := protocol.InternalEnvelope{
		MsgType:  protocol.MsgQuery4Result,
		ClientId: clientId,
		Payload:  batch.Bytes(),
	}
	return p.EncodeInternalEnvelope(envelope)
}

func (BinaryCodec) DecodeQuery4ResultPayload(payload []byte) ([]protocol.Query4Result, error) {
	r := bytes.NewReader(payload)
	var countBytes [4]byte
	if _, err := io.ReadFull(r, countBytes[:]); err != nil {
		return nil, fmt.Errorf("reading batch count: %w", err)
	}
	count := binary.BigEndian.Uint32(countBytes[:])

	results := make([]protocol.Query4Result, 0, count)
	for i := uint32(0); i < count; i++ {
		var lengthBytes [2]byte
		if _, err := io.ReadFull(r, lengthBytes[:]); err != nil {
			return nil, fmt.Errorf("reading length of result %d: %w", i, err)
		}
		length := binary.BigEndian.Uint16(lengthBytes[:])

		resBytes := make([]byte, length)
		if _, err := io.ReadFull(r, resBytes); err != nil {
			return nil, fmt.Errorf("reading result %d body: %w", i, err)
		}
		result, err := decodeQuery4Result(bytes.NewReader(resBytes))
		if err != nil {
			return nil, fmt.Errorf("decoding result %d: %w", i, err)
		}
		results = append(results, result)
	}
	return results, nil
}

// ====================
// ===== Query  5 =====
// ====================

func encodeQuery5Result(buf *bytes.Buffer, r protocol.Query5Result) error {
	writeInt64(buf, r.Count)
	return nil
}

func decodeQuery5Result(r *bytes.Reader) (protocol.Query5Result, error) {
	var res protocol.Query5Result
	count, err := readInt64(r)
	if err != nil {
		return res, fmt.Errorf("count: %w", err)
	}
	res.Count = count
	return res, nil
}

func (BinaryCodec) EncodeQuery5Result(result protocol.Query5Result) ([]byte, error) {
	return encodeBatch([]protocol.Query5Result{result}, encodeQuery5Result)
}

func (BinaryCodec) DecodeQuery5Result(payload []byte) (protocol.Query5Result, error) {
	result, err := decodeBatch(payload, decodeQuery5Result)
	return result[0], err
}

// ====================
// ===== EOF Counts ===
// ====================

// EncodeEOFCounts serialises the per-key message counts carried by a
// layer-to-layer EOF (the payload of an InternalEnvelope tagged as an EOF type).
// Keys are written in sorted order so the encoding is deterministic for a given
// map, which keeps logs stable and makes the bytes comparable in tests.
//
// Layout:
//
//	[uint32 entryCount] then entryCount × ( [uint8 keyLen][key bytes][int64 count] )
func (BinaryCodec) EncodeEOFCounts(counts map[broker.KeyType]int) ([]byte, error) {
	var buf bytes.Buffer
	var entryCount [4]byte
	binary.BigEndian.PutUint32(entryCount[:], uint32(len(counts)))
	buf.Write(entryCount[:])

	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, string(key))
	}
	sort.Strings(keys)

	for _, key := range keys {
		if err := writeString(&buf, key); err != nil {
			return nil, fmt.Errorf("encoding eof count key %q: %w", key, err)
		}
		writeInt64(&buf, int64(counts[broker.KeyType(key)]))
	}
	return buf.Bytes(), nil
}

func (p *BinaryCodec) EncodeEOFCountsEnvelope(clientId uuid.UUID, counts map[broker.KeyType]int) ([]byte, error) {
	payload, err := p.EncodeEOFCounts(counts)
	if err != nil {
		return nil, fmt.Errorf("encoding EOF counts: %w", err)
	}
	envelope := protocol.InternalEnvelope{
		MsgType:  protocol.MsgTransactionsEOF,
		ClientId: clientId,
		Payload:  payload,
	}
	return p.EncodeInternalEnvelope(envelope)
}

// DecodeEOFCounts is the inverse of EncodeEOFCounts. It always returns a
// non-nil map (empty when entryCount is zero) so callers can range over it
// without a nil check.
func (BinaryCodec) DecodeEOFCounts(payload []byte) (map[broker.KeyType]int, error) {
	r := bytes.NewReader(payload)
	var entryCountBytes [4]byte
	if _, err := io.ReadFull(r, entryCountBytes[:]); err != nil {
		return nil, fmt.Errorf("reading eof counts length: %w", err)
	}
	entryCount := binary.BigEndian.Uint32(entryCountBytes[:])

	counts := make(map[broker.KeyType]int, entryCount)
	for i := range entryCount {
		key, err := readString(r)
		if err != nil {
			return nil, fmt.Errorf("reading eof count key %d: %w", i, err)
		}
		value, err := readInt64(r)
		if err != nil {
			return nil, fmt.Errorf("reading eof count value for key %q: %w", key, err)
		}
		counts[broker.KeyType(key)] = int(value)
	}
	return counts, nil
}

// --- batch framing ---

// encodeBatch serialises a slice of T into the standard result-batch layout
// used across every query: a uint32 element count followed by each element
// length-prefixed with a uint16. The per-element body is produced by
// encodeOne, which is the only piece that differs between queries.
func encodeBatch[T any](items []T, encodeOne func(*bytes.Buffer, T) error) ([]byte, error) {
	var batch bytes.Buffer
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], uint32(len(items)))
	batch.Write(count[:])

	for i, item := range items {
		var itemBuf bytes.Buffer
		if err := encodeOne(&itemBuf, item); err != nil {
			return nil, fmt.Errorf("encoding result %d: %w", i, err)
		}
		if itemBuf.Len() > math.MaxUint16 {
			return nil, fmt.Errorf("result %d too large: %d bytes (max %d)", i, itemBuf.Len(), math.MaxUint16)
		}
		var length [2]byte
		binary.BigEndian.PutUint16(length[:], uint16(itemBuf.Len()))
		batch.Write(length[:])
		batch.Write(itemBuf.Bytes())
	}
	return batch.Bytes(), nil
}

// decodeBatch is the inverse of encodeBatch. decodeOne reads a single element
// body from its own bounded reader, so individual element decoders can't
// overflow into the next element's body.
func decodeBatch[T any](payload []byte, decodeOne func(*bytes.Reader) (T, error)) ([]T, error) {
	r := bytes.NewReader(payload)
	var countBytes [4]byte
	if _, err := io.ReadFull(r, countBytes[:]); err != nil {
		return nil, fmt.Errorf("reading batch count: %w", err)
	}
	count := binary.BigEndian.Uint32(countBytes[:])

	items := make([]T, 0, count)
	for i := range count {
		var lengthBytes [2]byte
		if _, err := io.ReadFull(r, lengthBytes[:]); err != nil {
			return nil, fmt.Errorf("reading length of result %d: %w", i, err)
		}
		length := binary.BigEndian.Uint16(lengthBytes[:])

		itemBytes := make([]byte, length)
		if _, err := io.ReadFull(r, itemBytes); err != nil {
			return nil, fmt.Errorf("reading result %d body: %w", i, err)
		}
		item, err := decodeOne(bytes.NewReader(itemBytes))
		if err != nil {
			return nil, fmt.Errorf("decoding result %d: %w", i, err)
		}
		items = append(items, item)
	}
	return items, nil
}

// --- primitives ---

func writeString(buf *bytes.Buffer, s string) error {
	if len(s) > 255 {
		return fmt.Errorf("string too long: %d bytes (max 255)", len(s))
	}
	buf.WriteByte(byte(len(s)))
	buf.WriteString(s)
	return nil
}

func readString(r *bytes.Reader) (string, error) {
	length, err := r.ReadByte()
	if err != nil {
		return "", fmt.Errorf("reading string length: %w", err)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", fmt.Errorf("reading string body: %w", err)
	}
	return string(buf), nil
}

func writeFloat64(buf *bytes.Buffer, f float64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], math.Float64bits(f))
	buf.Write(b[:])
}

func readFloat64(r *bytes.Reader) (float64, error) {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return math.Float64frombits(binary.BigEndian.Uint64(b[:])), nil
}

func writeInt64(buf *bytes.Buffer, v int64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	buf.Write(b[:])
}

func readInt64(r *bytes.Reader) (int64, error) {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(b[:])), nil
}

func writeBool(buf *bytes.Buffer, b bool) {
	if b {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
}

func readBool(r *bytes.Reader) (bool, error) {
	b, err := r.ReadByte()
	if err != nil {
		return false, err
	}
	return b == 1, nil
}
