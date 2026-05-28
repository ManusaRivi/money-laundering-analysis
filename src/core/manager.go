package core

import (
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
)

type Worker interface {
	Run() error
	Stop()
}

type Manager struct {
	Worker Worker
	cnfg   *config.Config
	broker broker.Broker
}

func NewManager(cfg *config.Config) (*Manager, error) {
	if cfg == nil {
		return nil, errors.New("config is required")
	}

	communicationBroker, err := broker.NewBroker(cfg.Broker)
	if err != nil {
		return nil, err
	}

	worker, err := workerFactory(cfg.Worker, communicationBroker)
	if err != nil {
		return nil, err
	}

	return &Manager{
		Worker: worker,
		cnfg:   cfg,
		broker: communicationBroker,
	}, nil
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
