package client

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/network"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
)

type DatasetStream interface {
	GetNextBatch() ([]byte, error)
	BatchMsgType() protocol.MsgType
	EOFMsgType() protocol.MsgType
	Name() string
}

type Sender struct {
	conn  *network.Connection
	codec codec.Codec
}

func NewSender(conn *network.Connection, codec codec.Codec) *Sender {
	return &Sender{conn: conn, codec: codec}
}

func (s *Sender) StreamDataset(ds DatasetStream, running *atomic.Bool) error {
	slog.Debug("Started streaming dataset batches", "dataset", ds.Name())
	for {
		if !running.Load() {
			return fmt.Errorf("client stopped by signal")
		}

		payload, err := ds.GetNextBatch()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("reading %s batch: %w", ds.Name(), err)
		}

		envelope, err := s.codec.EncodeExternalEnvelope(protocol.ExternalEnvelope{
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

	eof, err := s.codec.EncodeExternalEnvelope(protocol.ExternalEnvelope{
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
