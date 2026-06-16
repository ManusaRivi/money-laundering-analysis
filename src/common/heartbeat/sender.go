package heartbeat

import (
	"context"
	"encoding/binary"
	"log/slog"
	"net"
	"time"
)

func Start(ctx context.Context, prefix string, id int, addrs []string, interval time.Duration) {
	var raddrs []*net.UDPAddr
	for _, addr := range addrs {
		raddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			slog.Error("heartbeat: failed to resolve", "addr", addr, "error", err)
			return
		}
		raddrs = append(raddrs, raddr)
	}

	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		slog.Error("heartbeat: failed to open socket", "error", err)
		return
	}
	defer conn.Close()

	payload := make([]byte, 2+len(prefix)+4)
	binary.BigEndian.PutUint16(payload[0:2], uint16(len(prefix)))
	copy(payload[2:], prefix)
	binary.BigEndian.PutUint32(payload[2+len(prefix):], uint32(id))

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	slog.Debug("heartbeat sender started", "prefix", prefix, "id", id, "addrs", addrs)

	for {
		select {
		case <-ctx.Done():
			slog.Debug("heartbeat sender stopped", "prefix", prefix, "id", id)
			return
		case <-ticker.C:
			for _, raddr := range raddrs {
				if _, err := conn.WriteTo(payload, raddr); err != nil {
					slog.Warn("heartbeat: send error", "addr", raddr, "error", err)
				}
			}
		}
	}
}
