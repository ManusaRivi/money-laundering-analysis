package inner

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
)

var (
	ErrInvalidPacket = errors.New("Invalid packet")
)

func MarshalTransactionPacket(clientID uuid.UUID, routingKey broker.KeyType, tx domain.Transaction) (*broker.Message, error) {
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
	return &broker.Message{RoutingKey: routingKey, Body: serializedMsg}, nil
}

func MarshalEOFPacket(clientID uuid.UUID, eof_counts domain.EOFCounts) (*broker.Message, error) {
	data, err := json.Marshal(eof_counts)
	if err != nil {
		return nil, err
	}
	msg := Packet{
		ClientID: clientID,
		Type:     TypeEOF,
		Data:     data,
	}

	serializedMsg, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return &broker.Message{RoutingKey: broker.KeyControlEOF, Body: serializedMsg}, nil
}

func MarshalBankInfoPacket(clientID uuid.UUID, routingKey broker.KeyType, bankInfo domain.BankInfo) (*broker.Message, error) {
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
	return &broker.Message{RoutingKey: routingKey, Body: serializedMsg}, nil
}

func MarshalBankInfoEOFPacket(clientID uuid.UUID, routingKey broker.KeyType) (*broker.Message, error) {
	msg := Packet{
		ClientID: clientID,
		Type:     TypeAccountsEOF,
		Data:     nil,
	}

	serializedMsg, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return &broker.Message{RoutingKey: routingKey, Body: serializedMsg}, nil
}

func MarshalQuery1ResultPacket(clientID uuid.UUID, result domain.Query1Result) (*broker.Message, error) {
	data, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	msg := Packet{
		ClientID: clientID,
		Type:     TypeQuery1Result,
		Data:     data,
	}

	serializedMsg, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return &broker.Message{RoutingKey: broker.KeyNil, Body: serializedMsg}, nil
}

func MarshalQuery1EOFPacket(clientID uuid.UUID) (*broker.Message, error) {
	msg := Packet{
		ClientID: clientID,
		Type:     TypeQuery1EOF,
		Data:     nil,
	}

	serializedMsg, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return &broker.Message{RoutingKey: broker.KeyControlEOF, Body: serializedMsg}, nil
}

func MarshalQuery2ResultPacket(clientID uuid.UUID, result domain.Query2Result) (*broker.Message, error) {
	data, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	msg := Packet{
		ClientID: clientID,
		Type:     TypeQuery2Result,
		Data:     data,
	}

	serializedMsg, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return &broker.Message{Body: serializedMsg}, nil
}

func MarshalQuery2EOFPacket(clientID uuid.UUID) (*broker.Message, error) {
	msg := Packet{
		ClientID: clientID,
		Type:     TypeQuery2EOF,
		Data:     nil,
	}

	serializedMsg, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return &broker.Message{Body: serializedMsg}, nil
}

func UnmarshalPacket(msg broker.Message) (*Packet, error) {
	var packet Packet
	err := json.Unmarshal(msg.Body, &packet)
	if err != nil {
		return nil, err
	}
	if packet.ClientID == uuid.Nil {
		return nil, fmt.Errorf("%w: missing ClientID field", ErrInvalidPacket)
	}

	return &packet, nil
}

// UnmarshalData is a helper method to unmarshal the Data field of the Packet into the provided destination struct.
func (p *Packet) UnmarshalData(dest any) error {
	return json.Unmarshal(p.Data, dest)
}
