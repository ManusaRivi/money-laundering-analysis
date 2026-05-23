package client

import (
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/network"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external/codec"
)

type DatasetStream interface {
	GetNextBatch() ([]byte, error)
	BatchMsgType() external.MsgType
	EOFMsgType() external.MsgType
	Name() string
}

type Sender struct {
	conn  *network.Connection
	codec codec.Codec
}

func NewSender(conn *network.Connection, codec codec.Codec) *Sender {
	return &Sender{conn: conn, codec: codec}
}

func (s *Sender) StreamDataset(ds DatasetStream) error {
	for {
		payload, err := ds.GetNextBatch()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("reading %s batch: %w", ds.Name(), err)
		}

		envelope, err := s.codec.EncodeEnvelope(external.Envelope{
			MsgType: ds.BatchMsgType(),
			Payload: payload,
		})
		if err != nil {
			return fmt.Errorf("encoding %s envelope: %w", ds.Name(), err)
		}

		if err := s.conn.Send(envelope); err != nil {
			return fmt.Errorf("sending %s batch: %w", ds.Name(), err)
		}
	}

	slog.Debug("Finished streaming dataset batches", "dataset", ds.Name())

	eof, err := s.codec.EncodeEnvelope(external.Envelope{
		MsgType: ds.EOFMsgType(),
		Payload: nil,
	})
	if err != nil {
		return fmt.Errorf("encoding %s EOF: %w", ds.Name(), err)
	}
	if err := s.conn.Send(eof); err != nil {
		return fmt.Errorf("sending %s EOF: %w", ds.Name(), err)
	}

	slog.Debug("Sent EOF message", "dataset", ds.Name())
	return nil
}
