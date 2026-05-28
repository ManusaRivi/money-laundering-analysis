package core

import (
	"errors"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
)

// Worker is the interface that all workers must implement.
// TODO: Implement Stop across workers
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

	worker, err := workerFactory(cfg, communicationBroker)
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
	return m.Worker.Run()
}
