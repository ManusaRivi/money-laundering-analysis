package client

import (
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/ManusaRivi/money-laundering-analysis/src/client/internal/data"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/network"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/codec"
)

type Receiver struct {
	conn      *network.Connection
	codec     codec.Codec
	outputDir string
	done      chan struct{}
	writers   map[protocol.MsgType]*data.QueryWriter
	running   *atomic.Bool
}

func NewReceiver(conn *network.Connection, codec codec.Codec, outputDir string, running *atomic.Bool) *Receiver {
	return &Receiver{
		conn:      conn,
		codec:     codec,
		outputDir: outputDir,
		done:      make(chan struct{}),
		writers:   make(map[protocol.MsgType]*data.QueryWriter),
		running:   running,
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

	start := time.Now()

	for pendingEOFs > 0 {
		if !r.running.Load() {
			slog.Info("Receiver stopping by signal")
			return
		}

		header, err := r.conn.Receive(codec.ExternalHeaderSize)
		if err != nil {
			slog.Info("Receiver stopping", "err", err)
			return
		}

		msgType, payloadSize := codec.DecodeExternalHeader(header)

		payload, err := r.conn.Receive(int(payloadSize))
		if err != nil {
			slog.Info("Receiver stopping", "err", err)
			return
		}

		slog.Debug("Received message", "msgType", msgType, "payloadSize", payloadSize)

		switch msgType {
		case protocol.MsgQuery1Result:
			results, err := r.codec.DecodeQuery1ResultBatch(payload)
			if err != nil {
				r.closeWriter(protocol.MsgQuery1Result)
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
			eof1Time := time.Since(start)
			slog.Info("Time to receive Query 1 EOF", "duration", eof1Time)
		case protocol.MsgQuery2Result:
			results, err := r.codec.DecodeQuery2ResultBatch(payload)
			if err != nil {
				r.closeWriter(protocol.MsgQuery2Result)
				slog.Warn("Failed to decode query 2 result", "err", err)
				continue
			}
			r.writeRows(protocol.MsgQuery2Result, query2RowsToString(results))
		case protocol.MsgQuery2ResultEOF:
			slog.Info("Received Query 2 EOF")
			if w, ok := r.writers[protocol.MsgQuery2Result]; ok {
				w.Close()
			}
			pendingEOFs--
			eof2Time := time.Since(start)
			slog.Info("Time to receive Query 2 EOF", "duration", eof2Time)
		case protocol.MsgQuery3Result:
			results, err := r.codec.DecodeQuery3ResultBatch(payload)
			if err != nil {
				r.closeWriter(protocol.MsgQuery3Result)
				slog.Warn("Failed to decode query 3 result", "err", err)
				continue
			}
			r.writeRows(protocol.MsgQuery3Result, query3RowsToString(results))
		case protocol.MsgQuery3ResultEOF:
			slog.Info("Received Query 3 EOF")
			if w, ok := r.writers[protocol.MsgQuery3Result]; ok {
				w.Close()
			}
			pendingEOFs--
			eof3Time := time.Since(start)
			slog.Info("Time to receive Query 3 EOF", "duration", eof3Time)
		case protocol.MsgQuery4Result:
			slog.Debug("Decoding Query 4 result")
			results, err := r.codec.DecodeQuery4ResultPayload(payload)
			if err != nil {
				r.closeWriter(protocol.MsgQuery4Result)
				slog.Warn("Failed to decode query 4 result", "err", err)
				continue
			}
			slog.Debug("Decoded Query 4 result", "numResults", len(results))
			r.writeRows(protocol.MsgQuery4Result, query4RowsToString(results))
		case protocol.MsgQuery4ResultEOF:
			slog.Info("Received Query 4 EOF")
			if w, ok := r.writers[protocol.MsgQuery4Result]; ok {
				w.Close()
			}
			pendingEOFs--
			eof4Time := time.Since(start)
			slog.Info("Time to receive Query 4 EOF", "duration", eof4Time)
		case protocol.MsgQuery5Result:
			result, err := r.codec.DecodeQuery5Result(payload)
			if err != nil {
				r.closeWriter(protocol.MsgQuery5Result)
				slog.Warn("Failed to decode query 5 result", "err", err)
				continue
			}
			r.writeRows(protocol.MsgQuery5Result, query5RowsToString(result))
		case protocol.MsgQuery5ResultEOF:
			slog.Info("Received Query 5 EOF")
			if w, ok := r.writers[protocol.MsgQuery5Result]; ok {
				w.Close()
			}
			pendingEOFs--
			eof5Time := time.Since(start)
			slog.Info("Time to receive Query 5 EOF", "duration", eof5Time)
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

func (r *Receiver) closeWriter(resultType protocol.MsgType) {
	if w, ok := r.writers[resultType]; ok {
		w.Close()
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

func query2RowsToString(results []protocol.Query2Result) [][]string {
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

func query3RowsToString(results []protocol.Query3Result) [][]string {
	rows := make([][]string, 0, len(results))
	for _, res := range results {
		rows = append(rows, []string{
			res.FromBank,
			res.FromAccount,
			res.PaymentFormat,
			strconv.FormatFloat(res.AmountPaid, 'f', -1, 64),
		})
	}
	return rows
}

func query4RowsToString(results []protocol.Query4Result) [][]string {
	rows := make([][]string, 0, len(results))
	for _, res := range results {
		rows = append(rows, []string{
			res.BankID,
			res.ID,
		})
	}
	return rows
}

func query5RowsToString(result protocol.Query5Result) [][]string {
	rows := make([][]string, 0, 1)
	rows = append(rows, []string{strconv.FormatInt(result.Count, 10)})
	return rows
}
