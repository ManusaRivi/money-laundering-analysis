package monitoring

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"
)

type PingerConfig struct {
	Workers  []string
	Port     int
	Interval time.Duration
	Timeout  time.Duration
	OnResult func(key string, ok bool)
}

type Pinger struct {
	cfg    PingerConfig
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewPinger(cfg PingerConfig) *Pinger {
	return &Pinger{cfg: cfg}
}

func (p *Pinger) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	payload := []byte{0x00}
	ticker := time.NewTicker(p.cfg.Interval)
	defer ticker.Stop()

	slog.Debug("monitoring pinger started", "workers", len(p.cfg.Workers), "port", p.cfg.Port, "interval", p.cfg.Interval)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, worker := range p.cfg.Workers {
				p.wg.Add(1)
				go func(w string) {
					defer p.wg.Done()
					p.pingWorker(ctx, w, payload)
				}(worker)
			}
		}
	}
}

func (p *Pinger) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
}

func (p *Pinger) pingWorker(ctx context.Context, worker string, payload []byte) {
	addr := net.JoinHostPort(worker, fmt.Sprint(p.cfg.Port))
	conn, err := net.DialTimeout("udp", addr, p.cfg.Timeout)
	if err != nil {
		p.notify(worker, false)
		return
	}
	defer conn.Close()

	if _, err := conn.Write(payload); err != nil {
		p.notify(worker, false)
		return
	}

	conn.SetReadDeadline(time.Now().Add(p.cfg.Timeout))
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil {
		p.notify(worker, false)
		return
	}

	data := buf[:n]
	if len(data) < 6 {
		p.notify(worker, false)
		return
	}
	prefixLen := int(binary.BigEndian.Uint16(data[0:2]))
	if len(data) < 2+prefixLen+4 {
		p.notify(worker, false)
		return
	}
	prefix := string(data[2 : 2+prefixLen])
	id := int(binary.BigEndian.Uint32(data[2+prefixLen:]))
	key := fmt.Sprintf("%s_%d", prefix, id)

	p.notify(key, true)
}

func (p *Pinger) notify(key string, ok bool) {
	if p.cfg.OnResult != nil {
		p.cfg.OnResult(key, ok)
	}
}
