package protocol

import (
	"testing"

	"github.com/google/uuid"
)

func TestSourceMsgIDDeterministic(t *testing.T) {
	c := uuid.New()
	if SourceMsgID(c, StreamTransactions, 5) != SourceMsgID(c, StreamTransactions, 5) {
		t.Fatal("SourceMsgID not deterministic for identical inputs")
	}
	// Distinct seq / stream / client must (overwhelmingly) differ.
	if SourceMsgID(c, StreamTransactions, 5) == SourceMsgID(c, StreamTransactions, 6) {
		t.Error("SourceMsgID collides across seq")
	}
	if SourceMsgID(c, StreamTransactions, 5) == SourceMsgID(c, StreamAccounts, 5) {
		t.Error("SourceMsgID collides across stream")
	}
	if SourceMsgID(c, StreamTransactions, 5) == SourceMsgID(uuid.New(), StreamTransactions, 5) {
		t.Error("SourceMsgID collides across client")
	}
}

func TestDeriveMsgIDDeterministic(t *testing.T) {
	parent := SourceMsgID(uuid.New(), StreamTransactions, 1)
	if DeriveMsgID(parent, "tx.usd", 0) != DeriveMsgID(parent, "tx.usd", 0) {
		t.Fatal("DeriveMsgID not deterministic for identical inputs")
	}
	if DeriveMsgID(parent, "tx.usd", 0) == DeriveMsgID(parent, "tx.usd", 1) {
		t.Error("DeriveMsgID collides across outIdx")
	}
	if DeriveMsgID(parent, "a", 0) == DeriveMsgID(parent, "b", 0) {
		t.Error("DeriveMsgID collides across discriminator")
	}
}

// The length-prefixed discriminator must remove boundary ambiguity: ("ab", n)
// and ("a", m) must never produce the same id by byte-concatenation accident.
func TestDeriveMsgIDNoBoundaryCollision(t *testing.T) {
	parent := SourceMsgID(uuid.New(), StreamTransactions, 1)
	if DeriveMsgID(parent, "ab", 0) == DeriveMsgID(parent, "a", 0x62 /* 'b' */) {
		t.Error("DeriveMsgID has a discriminator/outIdx boundary collision")
	}
}
