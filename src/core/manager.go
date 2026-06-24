package core

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/monitoring"
)

type Worker interface {
	Run() error
	Stop()
}

type Manager struct {
	Worker   Worker
	cnfg     *config.Config
	broker   broker.Broker
	mlCancel context.CancelFunc
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
	m.startMonitoringListener()

	return m, nil
}

func (m *Manager) startMonitoringListener() {
	if m.cnfg.Monitoring == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.mlCancel = cancel
	go monitoring.Listen(ctx, m.cnfg.Monitoring.Port, m.cnfg.Worker.WorkerPrefix, m.cnfg.Worker.WorkerID)
}

func (m *Manager) Run() error {
	if m.Worker == nil {
		return errors.New("manager has no worker")
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	errCh := make(chan error, 1)
	go func() {
		errCh <- m.Worker.Run()
	}()

	select {
	case sig := <-sigCh:
		slog.Info("Received signal, shutting down worker...", "signal", sig)
		if m.mlCancel != nil {
			m.mlCancel()
		}
		m.Worker.Stop()
		if m.broker != nil {
			m.broker.Close()
		}
		select {
		case <-errCh:
		case <-time.After(5 * time.Second):
			slog.Warn("Shutdown timed out, exiting forcefully")
		}
	case err := <-errCh:
		return err
	}

	return nil
}
