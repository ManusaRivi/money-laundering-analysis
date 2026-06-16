package heartbeat

import (
	"context"
	"encoding/binary"
	"log/slog"
	"net"
	"time"
)

func Start(ctx context.Context, prefix string, id int, addr string, interval time.Duration) {
	raddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		slog.Error("heartbeat: failed to resolve", "addr", addr, "error", err)
		return
	}
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		slog.Error("heartbeat: failed to dial", "error", err)
		return
	}
	defer conn.Close()

	payload := make([]byte, 2+len(prefix)+4)
	binary.BigEndian.PutUint16(payload[0:2], uint16(len(prefix)))
	copy(payload[2:], prefix)
	binary.BigEndian.PutUint32(payload[2+len(prefix):], uint32(id))

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	slog.Debug("[heartbeat] sender started", "prefix", prefix, "id", id, "addr", addr)

	for {
		select {
		case <-ctx.Done():
			slog.Debug("[heartbeat] sender stopped", "prefix", prefix, "id", id)
			return
		case <-ticker.C:
			slog.Debug("[heartbeat] sending heartbeat", "prefix", prefix, "id", id)
			if _, err := conn.Write(payload); err != nil {
				slog.Warn("heartbeat: send error", "error", err)
			}
		}
	}
}
