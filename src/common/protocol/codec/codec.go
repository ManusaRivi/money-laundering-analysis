package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
)

const (
	HeaderSize         = 5
	PayloadLengthBytes = 4
)

// DecodeHeader parses a 5-byte frame header into its message type and payload
// length. Framing is uniform across codec implementations, so this is a
// package-level function rather than an interface method.
func DecodeHeader(header []byte) (protocol.MsgType, uint32) {
	msgType := protocol.MsgType(header[0])
	payloadLen := binary.BigEndian.Uint32(header[1:HeaderSize])
	return msgType, payloadLen
}

// BinaryCodec is the default hand-rolled binary codec.
type BinaryCodec struct{}

func New() *BinaryCodec { return &BinaryCodec{} }

// --- envelope ---

func (BinaryCodec) EncodeEnvelope(envelope protocol.Envelope) ([]byte, error) {
	buffer := make([]byte, HeaderSize+len(envelope.Payload))
	buffer[0] = byte(envelope.MsgType)
	binary.BigEndian.PutUint32(buffer[1:HeaderSize], uint32(len(envelope.Payload)))
	copy(buffer[HeaderSize:], envelope.Payload)
	return buffer, nil
}

// --- batch ---

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

// --- model codecs ---

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
