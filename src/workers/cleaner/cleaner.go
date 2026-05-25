package cleaner

import (
	"fmt"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/domain"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/inner"
)

type Cleaner struct {
	Broker        broker.Broker
	fieldsToClean []string
}

func NewCleaner(params map[string]any, broker broker.Broker) *Cleaner {
	fieldsToClean := make([]string, 0)
	if field, ok := params["field"]; ok {
		if fieldList, ok := field.([]any); ok {
			for _, f := range fieldList {
				if str, ok := f.(string); ok {
					fieldsToClean = append(fieldsToClean, str)
				}
			}
		}
	}
	return &Cleaner{Broker: broker, fieldsToClean: fieldsToClean}
}

func (c *Cleaner) Run() error {
	defer func() {
		c.Broker.StopConsuming()
	}()

	return c.Broker.StartConsuming(func(msg broker.Message, ack, nack func()) {
		if err := c.handleMessage(msg); err != nil {
			nack()
			return
		}
		ack()
	})
}

func (c *Cleaner) Stop() {}

func (c *Cleaner) handleMessage(msg broker.Message) error {
	pkt, err := inner.UnmarshalPacket(msg)

	if err != nil {
		slog.Error("Error unmarshalling packet", "error", err)
		return err
	}

	switch pkt.Type {
	case inner.TypeTransaction:
		var tx domain.Transaction
		if err := pkt.UnmarshalData(&tx); err != nil {
			slog.Error("Error unmarshalling transaction data", "error", err)
			return err
		}

		for _, f := range c.fieldsToClean {
			tx.CutColumn(f)
		}

		msg, err := inner.MarshalTransactionPacket(pkt.ClientID, "", tx)

		if err != nil {
			slog.Error("Error marshalling cleaned packet", "error", err)
			return err
		}

		return c.Broker.Send(*msg)
	case inner.TypeEOF:
		slog.Debug("Received EOF packet, forwarding to next worker...")
		// Propagar en varios cleaners?
		eofMsg, err := inner.MarshalEOFPacket(pkt.ClientID, "")
		if err != nil {
			slog.Error("Error marshalling EOF packet", "error", err)
			return err
		}
		if err := c.Broker.Send(*eofMsg); err != nil {
			slog.Error("Error sending EOF packet to broker", "error", err)
			return err
		}
	default:
		return fmt.Errorf("unexpected inbound packet type: %v", pkt.Type)
	}
	return nil
}
