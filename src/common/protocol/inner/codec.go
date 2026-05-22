package inner

import (
	"encoding/json"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
)

func SerializeTransactionMessage(clientID string, tx domain.Transaction) (*broker.Message, error) {
	var msg InnerMessage
	msg.ClientID = clientID
	msg.Type = MsgTypeTransaction

	data, err := json.Marshal(tx)
	if err != nil {
		return nil, err
	}
	msg.Data = data
	serializedMsg, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return &broker.Message{Body: string(serializedMsg)}, nil
}

