package eof

import (
	"testing"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/google/uuid"
)

func mkID(b byte) protocol.MsgID {
	var m protocol.MsgID
	m[0] = b
	return m
}

func TestControlMessageRoundTripCounts(t *testing.T) {
	msg := ControlMessage{
		Type:        MsgTypeAmountResponse,
		ClientID:    uuid.New(),
		RequesterID: 2,
		SenderID:    1,
		ReceivedIds: map[protocol.MsgID]int{mkID(1): 100, mkID(2): 50},
		SentCountByKeyIds: map[broker.KeyType]map[protocol.MsgID]int{
			broker.KeyNil:            {mkID(1): 100, mkID(2): 50},
			broker.KeyType("tx.usd"): {mkID(1): 30},
		},
	}

	bm, err := MarshalControlMessage(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := UnmarshalControlMessage(*bm)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got.ReceivedIds[mkID(1)] != 100 || got.ReceivedIds[mkID(2)] != 50 {
		t.Fatalf("received counts not preserved: %v", got.ReceivedIds)
	}
	if got.SentCountByKeyIds[broker.KeyNil][mkID(1)] != 100 ||
		got.SentCountByKeyIds[broker.KeyType("tx.usd")][mkID(1)] != 30 {
		t.Fatalf("sent counts not preserved: %v", got.SentCountByKeyIds)
	}
}

func TestBuildSentCountByKeyMapSumsAndFilters(t *testing.T) {
	received := map[protocol.MsgID]int{mkID(1): 100, mkID(2): 50}
	sent := map[broker.KeyType]map[protocol.MsgID]int{
		// id(9) was never received, so it must be excluded from the sent total.
		broker.KeyNil: {mkID(1): 10, mkID(2): 5, mkID(9): 7},
	}
	got := buildSentCountByKeyMap(received, sent)
	if got[broker.KeyNil] != 15 {
		t.Fatalf("sent[KeyNil] = %d; want 15 (10+5, id(9) excluded)", got[broker.KeyNil])
	}
}
