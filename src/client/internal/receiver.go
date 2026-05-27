package client

import (
	"log/slog"
	"strconv"

	"github.com/ManusaRivi/money-laundering-analysis/src/client/internal/data"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/network"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external/codec"
)

type Receiver struct {
	conn      *network.Connection
	codec     codec.Codec
	outputDir string
	done      chan struct{}
	writers   map[external.MsgType]*data.QueryWriter
}

func NewReceiver(conn *network.Connection, codec codec.Codec, outputDir string) *Receiver {
	return &Receiver{
		conn:      conn,
		codec:     codec,
		outputDir: outputDir,
		done:      make(chan struct{}),
		writers:   make(map[external.MsgType]*data.QueryWriter),
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
		case external.MsgQuery1Result:
			results, err := r.codec.DecodeQuery1ResultBatch(payload)
			if err != nil {
				r.closeWriter(external.MsgQuery1Result)
				slog.Warn("Failed to decode query 1 result", "err", err)
				continue
			}
			r.writeRows(external.MsgQuery1Result, query1RowsToString(results))
		case external.MsgQuery1ResultEOF:
			slog.Info("Received Query 1 EOF")
			if w, ok := r.writers[external.MsgQuery1Result]; ok {
				w.Close()
			}
			pendingEOFs--
		case external.MsgQuery2Result:
			results, err := r.codec.DecodeQuery2ResultBatch(payload)
			if err != nil {
				r.closeWriter(external.MsgQuery2Result)
				slog.Warn("Failed to decode query 2 result", "err", err)
				continue
			}
			r.writeRows(external.MsgQuery2Result, query2RowsToString(results))
		case external.MsgQuery2ResultEOF:
			slog.Info("Received Query 2 EOF")
			if w, ok := r.writers[external.MsgQuery2Result]; ok {
				w.Close()
			}
			pendingEOFs--
		case external.MsgQuery5Result:
			results, err := r.codec.DecodeQuery5ResultBatch(payload)
			if err != nil {
				r.closeWriter(external.MsgQuery5Result)
				slog.Warn("Failed to decode query 5 result", "err", err)
				continue
			}
			r.writeRows(external.MsgQuery5Result, query5RowsToString(results))
		case external.MsgQuery5ResultEOF:
			slog.Info("Received Query 5 EOF")
			if w, ok := r.writers[external.MsgQuery5Result]; ok {
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

func (r *Receiver) closeWriter(resultType external.MsgType) {
	if w, ok := r.writers[resultType]; ok {
		w.Close()
	}
}

func (r *Receiver) writeRows(resultType external.MsgType, rows [][]string) {
	w, ok := r.writers[resultType]
	if !ok {
		return
	}
	w.WriteRows(rows)
}

func query1RowsToString(results []external.Query1Result) [][]string {
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

func query2RowsToString(results []external.Query2Result) [][]string {
	rows := make([][]string, 0, len(results))
	for _, res := range results {
		rows = append(rows, []string{
			res.FromBank,
			res.FromAccount,
			res.BankName,
			strconv.FormatFloat(res.AmountPaid, 'f', -1, 64),
		})
	}
	return rows
}

func query5RowsToString(results []external.Query5Result) [][]string {
	rows := make([][]string, 0, len(results))
	for _, res := range results {
		rows = append(rows, []string{strconv.FormatInt(res.Count, 10)})
	}
	return rows
}
