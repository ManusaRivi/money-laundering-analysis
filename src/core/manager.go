package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/heartbeat"
)

type Worker interface {
	Run() error
	Stop()
}

type Manager struct {
	Worker  Worker
	cnfg    *config.Config
	broker  broker.Broker
	hbCancel context.CancelFunc
}

func NewManager(cfg *config.Config) (*Manager, error) {
	if cfg == nil {
		return nil, errors.New("config is required")
	}

	if cfg.Worker.Type == "Monitor" {
		worker, err := workerFactory(cfg, nil)
		if err != nil {
			return nil, err
		}
		m := &Manager{Worker: worker, cnfg: cfg}
		m.startHeartbeat(cfg)
		return m, nil
	}

	communicationBroker, err := broker.NewBroker(cfg.Broker)
	if err != nil {
		return nil, err
	}

	worker, err := workerFactory(cfg, communicationBroker)
	if err != nil {
		return nil, err
	}

	m := &Manager{
		Worker: worker,
		cnfg:   cfg,
		broker: communicationBroker,
	}
	m.startHeartbeat(cfg)

	return m, nil
}

func (m *Manager) startHeartbeat(cfg *config.Config) {
	if cfg.Heartbeat == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.hbCancel = cancel
	addr := fmt.Sprintf("%s:%d", cfg.Heartbeat.MonitorHost, cfg.Heartbeat.MonitorPort)
	interval := time.Duration(cfg.Heartbeat.Interval) * time.Second
	go heartbeat.Start(ctx, cfg.Worker.WorkerPrefix, cfg.Worker.WorkerID, addr, interval)
}

func (m *Manager) Run() error {
	if m.Worker == nil {
		return errors.New("manager has no worker")
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		slog.Info("Received signal, shutting down worker...", "signal", sig)

		if m.hbCancel != nil {
			m.hbCancel()
		}

		go m.Worker.Stop()

		select {
		case <-time.After(5 * time.Second):
			slog.Warn("Shutdown timed out, exiting forcefully")
			os.Exit(0)
		}
	}()

	err := m.Worker.Run()
	signal.Stop(sigCh)
	return err
}
