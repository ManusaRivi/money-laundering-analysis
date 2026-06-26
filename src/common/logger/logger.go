package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
)

const (
	ansiReset  = "\033[0m"
	ansiRed    = "\033[31m"
	ansiGreen  = "\033[32m"
	ansiYellow = "\033[33m"
	ansiCyan   = "\033[36m"
)

type coloredHandler struct {
	mu  sync.Mutex
	w   io.Writer
	lvl slog.Leveler
}

func newColoredHandler(w io.Writer, lvl slog.Leveler) *coloredHandler {
	return &coloredHandler{w: w, lvl: lvl}
}

func (h *coloredHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.lvl.Level()
}

func (h *coloredHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	ts := r.Time.Format("15:04:05")

	var levelColor string
	var levelText string
	switch r.Level {
	case slog.LevelDebug:
		levelColor = ansiCyan
		levelText = "DEBUG"
	case slog.LevelInfo:
		levelColor = ansiGreen
		levelText = "INFO "
	case slog.LevelWarn:
		levelColor = ansiYellow
		levelText = "WARN "
	case slog.LevelError:
		levelColor = ansiRed
		levelText = "ERROR"
	}

	var attrs string
	r.Attrs(func(a slog.Attr) bool {
		attrs += fmt.Sprintf(" %s=%v", a.Key, a.Value.Any())
		return true
	})

	msg := fmt.Sprintf("msg=%q", r.Message)

	fmt.Fprintf(h.w, "%s %s%s%s %s%s\n", ts, levelColor, levelText, ansiReset, msg, attrs)
	return nil
}

func (h *coloredHandler) WithAttrs(_ []slog.Attr) slog.Handler {
	return &coloredHandler{w: h.w, lvl: h.lvl}
}

func (h *coloredHandler) WithGroup(_ string) slog.Handler {
	return &coloredHandler{w: h.w, lvl: h.lvl}
}

func SetupLogger() {
	levelStr := os.Getenv("LOG_LEVEL")
	var level slog.Level
	if err := level.UnmarshalText([]byte(levelStr)); err != nil {
		level = slog.LevelInfo
	}
	h := newColoredHandler(os.Stdout, level)
	slog.SetDefault(slog.New(h))
}
