package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/bully"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/monitoring"
	"gopkg.in/yaml.v3"
)

type monitorState struct {
	mu       sync.RWMutex
	failures map[string]int
}

type Monitor struct {
	params           *config.MonitorWorkerParams
	state            *monitorState
	bully            *bully.Bully
	pinger           *monitoring.Pinger
	cancel           context.CancelFunc
	selfKey          string
	selfID           int
	failureThreshold int
}

type workersFile struct {
	Workers []string `yaml:"workers"`
}

func New(mp *config.MonitorWorkerParams, selfKey string, selfID int, workerPrefix string, workerAmount int) (*Monitor, error) {
	if mp.Monitoring.FailureThreshold == 0 {
		mp.Monitoring.FailureThreshold = 3
	}
	if mp.Monitoring.PingInterval == "" {
		mp.Monitoring.PingInterval = "3s"
	}
	if mp.Monitoring.PingTimeout == "" {
		mp.Monitoring.PingTimeout = "2s"
	}
	if mp.Bully.TcpHost == "" {
		mp.Bully.TcpHost = "0.0.0.0"
	}
	if mp.Bully.TcpPort == 0 {
		mp.Bully.TcpPort = 9001
	}
	if mp.Bully.PingInterval == "" {
		mp.Bully.PingInterval = "1.5s"
	}
	if mp.Bully.PingTimeout == "" {
		mp.Bully.PingTimeout = "500ms"
	}
	if mp.Monitoring.UdpPort == 0 {
		mp.Monitoring.UdpPort = 9000
	}

	bullyInst, err := newBully(mp.Bully, workerPrefix, workerAmount, selfID)
	if err != nil {
		return nil, err
	}

	return &Monitor{
		params: mp,
		state: &monitorState{
			failures: make(map[string]int),
		},
		selfKey:          selfKey,
		selfID:           selfID,
		bully:            bullyInst,
		failureThreshold: mp.Monitoring.FailureThreshold,
	}, nil
}

func newBully(mp config.BullyParams, workerPrefix string, workerAmount int, selfID int) (*bully.Bully, error) {
	pingInterval, err := time.ParseDuration(mp.PingInterval)
	if err != nil {
		return nil, fmt.Errorf("invalid bully.ping_interval: %w", err)
	}
	pingTimeout, err := time.ParseDuration(mp.PingTimeout)
	if err != nil {
		return nil, fmt.Errorf("invalid bully.ping_timeout: %w", err)
	}

	var peers []bully.Peer
	for id := range workerAmount {
		if id == selfID {
			continue
		}
		peers = append(peers, bully.Peer{
			ID:   id,
			Addr: fmt.Sprintf("%s_%d:%d", workerPrefix, id, mp.TcpPort),
		})
	}

	return bully.New(bully.Config{
		SelfID:       selfID,
		Peers:        peers,
		ListenAddr:   fmt.Sprintf("%s:%d", mp.TcpHost, mp.TcpPort),
		PingInterval: pingInterval,
		PingTimeout:  pingTimeout,
	}), nil
}

func loadWorkers() ([]string, error) {
	path := os.Getenv("WORKERS_CONFIG_PATH")
	if path == "" {
		path = "/app/workers.yaml"
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read workers config: %w", err)
	}
	var wf workersFile
	if err := yaml.Unmarshal(data, &wf); err != nil {
		return nil, fmt.Errorf("parse workers config: %w", err)
	}
	return wf.Workers, nil
}

func (m *Monitor) Run() error {
	pingInterval, err := time.ParseDuration(m.params.Monitoring.PingInterval)
	if err != nil {
		return fmt.Errorf("invalid monitoring.ping_interval: %w", err)
	}
	pingTimeout, err := time.ParseDuration(m.params.Monitoring.PingTimeout)
	if err != nil {
		return fmt.Errorf("invalid monitoring.ping_timeout: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	slog.Info("monitor started", "self", m.selfKey)

	if err := m.bully.Start(ctx); err != nil {
		cancel()
		return fmt.Errorf("bully start: %w", err)
	}

	go monitoring.Listen(ctx, m.params.Monitoring.UdpPort, "monitor", m.selfID)

	workers, err := loadWorkers()
	if err != nil {
		slog.Warn("monitor: failed to load workers list, will retry", "error", err)
	}

	go m.leaderLoop(ctx, workers, pingInterval, pingTimeout)

	<-ctx.Done()
	return nil
}

func (m *Monitor) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	if m.bully != nil {
		m.bully.Stop()
	}
	m.stopPinger()
}

func (m *Monitor) leaderLoop(ctx context.Context, workers []string, pingInterval, pingTimeout time.Duration) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if m.bully.IsLeader() && m.pinger == nil {
				m.startPinger(ctx, workers, pingInterval, pingTimeout)
			} else if !m.bully.IsLeader() && m.pinger != nil {
				m.stopPinger()
			}
		}
	}
}

func (m *Monitor) startPinger(ctx context.Context, workers []string, pingInterval, pingTimeout time.Duration) {
	var filtered []string
	for _, w := range workers {
		if w != m.selfKey {
			filtered = append(filtered, w)
		}
	}
	if len(filtered) == 0 {
		slog.Warn("monitor: no workers to ping (all filtered out)")
		return
	}

	m.pinger = monitoring.NewPinger(monitoring.PingerConfig{
		Workers:  filtered,
		Port:     m.params.Monitoring.UdpPort,
		Interval: pingInterval,
		Timeout:  pingTimeout,
		OnResult: func(key string, ok bool) {
			m.handleResult(key, ok)
		},
	})
	go m.pinger.Start(ctx)
	slog.Info("monitor: started pinger as leader", "workers", len(workers))
}

func (m *Monitor) stopPinger() {
	if m.pinger != nil {
		m.pinger.Stop()
		m.pinger = nil
		slog.Info("monitor: stopped pinger, no longer leader")
	}
}

func (m *Monitor) handleResult(key string, ok bool) {
	if ok {
		m.state.mu.Lock()
		m.state.failures[key] = 0
		m.state.mu.Unlock()
		return
	}

	m.state.mu.Lock()
	m.state.failures[key]++
	fails := m.state.failures[key]
	m.state.mu.Unlock()

	slog.Warn("monitor: ping failed", "container", key, "failure", fails, "threshold", m.failureThreshold)

	if fails >= m.failureThreshold {
		m.state.mu.Lock()
		delete(m.state.failures, key)
		m.state.mu.Unlock()

		m.restartContainer(key)
	}
}

func (m *Monitor) restartContainer(key string) {
	slog.Info("monitor: executing docker start", "container", key)
	cmd := exec.Command("docker", "start", key)
	if err := cmd.Run(); err != nil {
		slog.Error("monitor: docker start failed", "container", key, "error", err)
	}
}
