package client

import (
	"log/slog"
	"strconv"

	"github.com/ManusaRivi/money-laundering-analysis/src/client/internal/data"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/network"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
)

type Receiver struct {
	conn      *network.Connection
	codec     *codec.BinaryCodec
	outputDir string
	done      chan struct{}
	writers   map[protocol.MsgType]*data.QueryWriter
}

func NewReceiver(conn *network.Connection, codec *codec.BinaryCodec, outputDir string) *Receiver {
	return &Receiver{
		conn:      conn,
		codec:     codec,
		outputDir: outputDir,
		done:      make(chan struct{}),
		writers:   make(map[protocol.MsgType]*data.QueryWriter),
	}
}

func (r *Receiver) Done() <-chan struct{} {
	return r.done
}

// For each query result type, the receiver starts a QueryWriter that listens on a channel for
// incoming rows (query results) and writes them to the corresponding output file.
// When an EOF is received for a query result type, the corresponding QueryWriter is closed,
// which signals it to finish writing and exit.
func (r *Receiver) Listen() {
	defer close(r.done)

	r.startWriters()
	defer r.shutdownWriters()

	pendingEOFs := len(data.GetQueryResultData())

	for pendingEOFs > 0 {
		header, err := r.conn.Receive(codec.HeaderSize)
		if err != nil {
			slog.Info("Receiver stopping", "err", err)
			return
		}

		msgType, payloadSize := codec.DecodeHeader(header)

		payload, err := r.conn.Receive(int(payloadSize))
		if err != nil {
			slog.Info("Receiver stopping", "err", err)
			return
		}

		switch msgType {
		case protocol.MsgQuery1Result:
			results, err := r.codec.DecodeQuery1Result(payload)
			if err != nil {
				slog.Warn("Failed to decode query 1 result", "err", err)
				continue
			}
			r.writeRows(protocol.MsgQuery1Result, query1RowsToString(results))
		case protocol.MsgQuery1ResultEOF:
			slog.Info("Received Query 1 EOF")
			if w, ok := r.writers[protocol.MsgQuery1Result]; ok {
				w.Close()
			}
			pendingEOFs--
		default:
			slog.Warn("Unknown message type received", "msgType", msgType)
		}
	}
}

func (r *Receiver) startWriters() {
	querySpecs := data.GetQueryResultData()
	for resultType, spec := range querySpecs {
		w := data.NewQueryWriter()
		r.writers[resultType] = w
		w.Start(spec, r.outputDir)
	}
}

func (r *Receiver) shutdownWriters() {
	for _, w := range r.writers {
		w.Close()
	}
	for _, w := range r.writers {
		<-w.Done()
	}
}

func (r *Receiver) writeRows(resultType protocol.MsgType, rows [][]string) {
	w, ok := r.writers[resultType]
	if !ok {
		return
	}
	w.WriteRows(rows)
}

func query1RowsToString(results []protocol.Query1Result) [][]string {
	rows := make([][]string, 0, len(results))
	for _, res := range results {
		rows = append(rows, []string{
			res.FromBank,
			res.FromAccount,
			res.ToBank,
			res.ToAccount,
			strconv.FormatFloat(res.AmountPaid, 'f', -1, 64),
		})
	}
	return rows
}
