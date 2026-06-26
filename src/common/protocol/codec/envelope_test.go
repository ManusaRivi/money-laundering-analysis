package codec

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
)

func TestInternalEnvelopeRoundTrip(t *testing.T) {
	c := New()
	clientID := uuid.New()
	msgID := protocol.DeriveMsgID(protocol.SourceMsgID(clientID, protocol.StreamTransactions, 7), "tx.usd", 3)
	payload := []byte("a length-prefixed batch payload would go here")

	encoded, err := c.EncodeInternalEnvelope(protocol.InternalEnvelope{
		MsgType:  protocol.MsgTransactionsBatch,
		ClientId: clientID,
		MsgID:    msgID,
		Payload:  payload,
	})
	if err != nil {
		t.Fatalf("EncodeInternalEnvelope: %v", err)
	}
	if len(encoded) != InternalHeaderSize+len(payload) {
		t.Fatalf("encoded length = %d, want %d", len(encoded), InternalHeaderSize+len(payload))
	}

	got, err := c.DecodeInternalEnvelope(encoded)
	if err != nil {
		t.Fatalf("DecodeInternalEnvelope: %v", err)
	}
	if got.MsgType != protocol.MsgTransactionsBatch {
		t.Errorf("MsgType = %v, want %v", got.MsgType, protocol.MsgTransactionsBatch)
	}
	if got.ClientId != clientID {
		t.Errorf("ClientId = %v, want %v", got.ClientId, clientID)
	}
	if got.MsgID != msgID {
		t.Errorf("MsgID = %x, want %x", got.MsgID, msgID)
	}
	if !bytes.Equal(got.Payload, payload) {
		t.Errorf("Payload = %q, want %q", got.Payload, payload)
	}
}

func TestSetEnvelopeMsgID(t *testing.T) {
	c := New()
	clientID := uuid.New()
	payload := []byte("payload")

	// Envelope built with a zero MsgID, as the PublishRaw path does.
	encoded, err := c.EncodeInternalEnvelope(protocol.InternalEnvelope{
		MsgType:  protocol.MsgTransactionsBatch,
		ClientId: clientID,
		Payload:  payload,
	})
	if err != nil {
		t.Fatalf("EncodeInternalEnvelope: %v", err)
	}

	want := protocol.SourceMsgID(clientID, protocol.StreamTransactions, 42)
	SetEnvelopeMsgID(encoded, want)

	got, err := c.DecodeInternalEnvelope(encoded)
	if err != nil {
		t.Fatalf("DecodeInternalEnvelope: %v", err)
	}
	if got.MsgID != want {
		t.Errorf("stamped MsgID = %x, want %x", got.MsgID, want)
	}
	// Stamping must not corrupt the rest of the envelope.
	if got.ClientId != clientID || !bytes.Equal(got.Payload, payload) {
		t.Errorf("envelope corrupted by stamping: client=%v payload=%q", got.ClientId, got.Payload)
	}
}

func TestDecodeInternalEnvelopeTooShort(t *testing.T) {
	c := New()
	// The decoder also accepts the pre-MsgID old format (21-byte header), so
	// the minimum valid buffer is oldInternalHeaderSize. Anything shorter must
	// always fail.
	if _, err := c.DecodeInternalEnvelope(make([]byte, oldInternalHeaderSize-1)); err == nil {
		t.Fatal("expected error decoding a buffer shorter than any known header format")
	}
}

func TestDecodeInternalEnvelopeOldFormat(t *testing.T) {
	c := New()
	clientID := uuid.New()
	payload := []byte("old-format payload")
	buf := make([]byte, oldInternalHeaderSize+len(payload))
	buf[0] = byte(protocol.MsgTransactionsBatch)
	copy(buf[1:], clientID[:])
	binary.BigEndian.PutUint32(buf[oldPayloadLenOffset:], uint32(len(payload)))
	copy(buf[oldInternalHeaderSize:], payload)

	got, err := c.DecodeInternalEnvelope(buf)
	if err != nil {
		t.Fatalf("DecodeInternalEnvelope old format: %v", err)
	}
	if got.MsgType != protocol.MsgTransactionsBatch {
		t.Errorf("MsgType = %v, want %v", got.MsgType, protocol.MsgTransactionsBatch)
	}
	if got.ClientId != clientID {
		t.Errorf("ClientId = %v, want %v", got.ClientId, clientID)
	}
	if got.MsgID != (protocol.MsgID{}) {
		t.Errorf("MsgID = %x, want zero", got.MsgID)
	}
	if !bytes.Equal(got.Payload, payload) {
		t.Errorf("Payload = %q, want %q", got.Payload, payload)
	}
}
