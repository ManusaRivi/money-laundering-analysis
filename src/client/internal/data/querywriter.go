package data

import (
	"log/slog"
	"path/filepath"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"
)

const resultChannelBuffer = 16

type QueryResultData struct {
	filename string
	header   []string
	eofType  protocol.MsgType
}

type QueryWriter struct {
	ch     chan []string
	closed bool
	done   chan struct{}
}

func NewQueryWriter() *QueryWriter {
	return &QueryWriter{
		ch:     make(chan []string, resultChannelBuffer),
		closed: false,
		done:   make(chan struct{}),
	}
}

func (qw *QueryWriter) WriteRows(rows [][]string) {
	if qw.closed {
		return
	}
	for _, row := range rows {
		qw.ch <- row
	}
}

func GetQueryResultData() map[protocol.MsgType]QueryResultData {
	return map[protocol.MsgType]QueryResultData{
		protocol.MsgQuery1Result: {
			filename: "query1.csv",
			header:   []string{"from_bank", "from_account", "to_bank", "to_account", "total_amount"},
			eofType:  protocol.MsgQuery1ResultEOF,
		},
	}
}

// Start spawns the writer goroutine. It runs until the row channel is closed
// (via Close) and signals completion by closing the Done channel.
func (qw *QueryWriter) Start(spec QueryResultData, outputDir string) {
	path := filepath.Join(outputDir, spec.filename)
	header := spec.header
	ch := qw.ch

	go func() {
		defer close(qw.done)
		if err := WriteResultsToOutput(path, header, ch); err != nil {
			slog.Error("Result writer failed", "path", path, "err", err)
			// Drain so producers don't block if writing fails early.
			for range ch {
			}
		}
	}()
}

// Done returns a channel that is closed when the writer goroutine has
// finished flushing and closing its output file.
func (qw *QueryWriter) Done() <-chan struct{} {
	return qw.done
}

func (qw *QueryWriter) Close() {
	if !qw.closed {
		close(qw.ch)
		qw.closed = true
	}
}
