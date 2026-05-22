package inner

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
)

var (
	ErrInvalidPacket = errors.New("Invalid packet")
)

func MarshalTransactionPacket(clientID string, tx domain.Transaction) (*broker.Message, error) {
	data, err := json.Marshal(tx)
	if err != nil {
		return nil, err
	}

	msg := Packet{
		ClientID: clientID,
		Type:     TypeTransaction,
		Data:     data,
	}

	serializedMsg, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return &broker.Message{Body: string(serializedMsg)}, nil
}

func MarshalEOFPacket(clientID string) (*broker.Message, error) {
	msg := Packet{
		ClientID: clientID,
		Type:     TypeEOF,
		Data:     nil,
	}

	serializedMsg, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return &broker.Message{Body: string(serializedMsg)}, nil
}

func MarshalBankInfoPacket(clientID string, bankInfo domain.BankInfo) (*broker.Message, error) {
	data, err := json.Marshal(bankInfo)
	if err != nil {
		return nil, err
	}
	msg := Packet{
		ClientID: clientID,
		Type:     TypeBankInfo,
		Data:     data,
	}

	serializedMsg, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return &broker.Message{Body: string(serializedMsg)}, nil
}

func UnmarshalPacket(msg broker.Message) (*Packet, error) {
	var packet Packet
	err := json.Unmarshal([]byte(msg.Body), &packet)
	if err != nil {
		return nil, err
	}
	if packet.ClientID == "" {
		return nil, fmt.Errorf("%w: missing ClientID field", ErrInvalidPacket)
	}

	return &packet, nil
}

// UnmarshalData is a helper method to unmarshal the Data field of the Packet into the provided destination struct.
func (p *Packet) UnmarshalData(dest any) error {
	return json.Unmarshal(p.Data, dest)
}
