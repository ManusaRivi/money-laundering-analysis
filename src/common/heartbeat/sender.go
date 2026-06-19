package heartbeat

import (
	"context"
	"encoding/binary"
	"log/slog"
	"net"
	"sync"
	"time"
)

func Start(ctx context.Context, prefix string, id int, addrs []string, interval time.Duration) {
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

	var mu sync.Mutex
	var raddrs []*net.UDPAddr
	var pending []string
	for _, addr := range addrs {
		raddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			pending = append(pending, addr)
		} else {
			raddrs = append(raddrs, raddr)
		}
	}

	if len(pending) > 0 {
		slog.Warn("heartbeat: some addresses unresolved, will retry", "pending", pending)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	slog.Debug("heartbeat sender started", "prefix", prefix, "id", id, "addrs", addrs, "resolved", len(raddrs))

	for {
		select {
		case <-ctx.Done():
			slog.Debug("heartbeat sender stopped", "prefix", prefix, "id", id)
			return
		case <-ticker.C:
			if len(pending) > 0 {
				mu.Lock()
				raddrs, pending = resolvePendingAddresses(raddrs, pending)
				mu.Unlock()
			}

			for _, raddr := range raddrs {
				if _, err := conn.WriteTo(payload, raddr); err != nil {
					slog.Warn("heartbeat: send error", "addr", raddr, "error", err)
				}
			}
		}
	}
}

func resolvePendingAddresses(raddrs []*net.UDPAddr, pending []string) ([]*net.UDPAddr, []string) {
	var stillPending []string
	for _, addr := range pending {
		raddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			stillPending = append(stillPending, addr)
		} else {
			raddrs = append(raddrs, raddr)
			slog.Info("heartbeat: resolved previously pending address", "addr", addr)
		}
	}
	return raddrs, stillPending
}
