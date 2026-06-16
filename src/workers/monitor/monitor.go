package monitor

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"os/exec"
	"sync"
	"time"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
)

type heartbeatState struct {
	mu             sync.RWMutex
	lastSeen       map[string]time.Time
	failures       map[string]int
	pendingRestart map[string]time.Time
}

type Monitor struct {
	cfg     *config.MonitorConfig
	state   *heartbeatState
	conn    *net.UDPConn
	cancel  context.CancelFunc
	selfKey string
	failureThreshold int
}

func NewMonitor(cfg *config.MonitorConfig, selfKey string) *Monitor {
	return &Monitor{
		cfg: cfg,
		state: &heartbeatState{
			lastSeen:       make(map[string]time.Time),
			failures:       make(map[string]int),
			pendingRestart: make(map[string]time.Time),
		},
		selfKey: selfKey,
		failureThreshold: cfg.FailureThreshold,
	}
}

func (m *Monitor) Run() error {
	timeout, err := time.ParseDuration(m.cfg.HeartbeatTimeout)
	if err != nil {
		return fmt.Errorf("monitor: invalid heartbeat_timeout: %w", err)
	}
	checkInterval, err := time.ParseDuration(m.cfg.CheckInterval)
	if err != nil {
		return fmt.Errorf("monitor: invalid check_interval: %w", err)
	}
	cooldown, err := time.ParseDuration(m.cfg.RestartCooldown)
	if err != nil {
		return fmt.Errorf("monitor: invalid restart_cooldown: %w", err)
	}

	addr := &net.UDPAddr{
		IP:   net.ParseIP(m.cfg.UdpHost),
		Port: m.cfg.UdpPort,
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("monitor: listen %s:%d: %w", m.cfg.UdpHost, m.cfg.UdpPort, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	m.conn = conn
	m.cancel = cancel

	slog.Info("monitor listening for heartbeats", "addr", addr, "self", m.selfKey)

	go m.checker(ctx, timeout, checkInterval, cooldown)
	go m.rabbitChecker(ctx, checkInterval)

	buf := make([]byte, 1024)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		slog.Debug("monitor waiting for heartbeat...")
		n, _, err := conn.ReadFromUDP(buf)
		slog.Debug("monitor received heartbeat", "bytes", n, "error", err)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Warn("monitor: read error", "error", err)
			continue
		}

		m.handleHeartbeat(buf[:n])
	}
}

func (m *Monitor) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	if m.conn != nil {
		m.conn.Close()
	}
}

func (m *Monitor) handleHeartbeat(data []byte) {
	if len(data) < 6 {
		return
	}
	prefixLen := int(binary.BigEndian.Uint16(data[0:2]))
	if len(data) < 2+prefixLen+4 {
		return
	}
	prefix := string(data[2 : 2+prefixLen])
	id := int(binary.BigEndian.Uint32(data[2+prefixLen:]))
	key := fmt.Sprintf("%s_%d", prefix, id)

	if key == m.selfKey {
		return
	}
	slog.Debug("monitor: heartbeat received", "key", key)
	m.state.mu.Lock()
	m.state.lastSeen[key] = time.Now()
	m.state.failures[key] = 0
	delete(m.state.pendingRestart, key)
	m.state.mu.Unlock()
}

func (m *Monitor) checker(ctx context.Context, timeout, interval, cooldown time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	slog.Info("monitor checker started", "timeout", timeout, "interval", interval, "cooldown", cooldown)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.check(timeout, cooldown)
		}
	}
}

func (m *Monitor) check(timeout, cooldown time.Duration) {
	now := time.Now()
	var toStart []string

	m.state.mu.Lock()

	for key := range m.state.lastSeen {
		last := m.state.lastSeen[key]
		if now.Sub(last) <= timeout {
			m.state.failures[key] = 0
			delete(m.state.pendingRestart, key)
			continue
		}

		if lastAttempt, ok := m.state.pendingRestart[key]; ok {
			if now.Sub(lastAttempt) < cooldown {
				continue
			}
			m.state.pendingRestart[key] = now
			toStart = append(toStart, key)
			continue
		}

		m.state.failures[key]++


		if m.state.failures[key] >= m.failureThreshold {
			slog.Warn("monitor: restarting container", "container", key, "failures", m.state.failures[key])
			m.state.pendingRestart[key] = now
			toStart = append(toStart, key)
		} else {
			slog.Warn("monitor: heartbeat expired", "container", key, "failure", m.state.failures[key], "threshold", m.failureThreshold)
		}
	}

	m.state.mu.Unlock()

	for _, key := range toStart {
		go m.restartContainer(key)
	}
}

func (m *Monitor) rabbitChecker(ctx context.Context, interval time.Duration) {
	if m.cfg.RabbitMQHost == "" || m.cfg.RabbitMQPort == 0 {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	addr := fmt.Sprintf("%s:%d", m.cfg.RabbitMQHost, m.cfg.RabbitMQPort)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				slog.Warn("monitor: rabbitmq unreachable", "addr", addr, "error", err)
			} else {
				conn.Close()
			}
		}
	}
}

func (m *Monitor) restartContainer(key string) {
	slog.Info("monitor: executing docker start", "container", key)
	cmd := exec.Command("docker", "start", key)
	if err := cmd.Run(); err != nil {
		slog.Error("monitor: docker start failed", "container", key, "error", err)
	}
}
