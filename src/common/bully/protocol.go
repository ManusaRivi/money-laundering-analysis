package bully

import (
	"fmt"
)

type MsgType byte

const (
	MsgPing        MsgType = iota
	MsgPong       
	MsgAlive      
	MsgElection   
	MsgCoordinator
)

func (t MsgType) String() string {
	switch t {
	case MsgPing:
		return "Ping"
	case MsgPong:
		return "Pong"
	case MsgAlive:
		return "Alive"
	case MsgElection:
		return "Election"
	case MsgCoordinator:
		return "Coordinator"
	default:
		return fmt.Sprintf("Unknown(%d)", t)
	}
}

type Message struct {
	Type     MsgType
	SenderID int
	LeaderID int
}

func Encode(msg Message) []byte {
	return []byte{byte(msg.Type), byte(msg.SenderID), byte(msg.LeaderID)}
}

func Decode(data []byte) (Message, error) {
	if len(data) < 3 {
		return Message{}, fmt.Errorf("bully: message too short: %d bytes", len(data))
	}
	return Message{
		Type:     MsgType(data[0]),
		SenderID: int(data[1]),
		LeaderID: int(data[2]),
	}, nil
}
