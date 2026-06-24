package monitoring

import (
	"context"
	"encoding/binary"
	"log/slog"
	"net"
	"time"
)

func Listen(ctx context.Context, port int, prefix string, id int) error {
	addr := &net.UDPAddr{
		IP:   net.IPv4zero,
		Port: port,
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	payload := make([]byte, 2+len(prefix)+4)
	binary.BigEndian.PutUint16(payload[0:2], uint16(len(prefix)))
	copy(payload[2:], prefix)
	binary.BigEndian.PutUint32(payload[2+len(prefix):], uint32(id))

	slog.Debug("monitoring listener started", "port", port, "prefix", prefix, "id", id)

	buf := make([]byte, 256)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		conn.SetReadDeadline(time.Now().Add(time.Second))
		n, raddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			continue
		}
		if n < 6 {
			conn.WriteToUDP(payload, raddr)
		}
	}
}
