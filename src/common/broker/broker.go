package broker

import (
	"errors"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/config"
)

var (
	ErrMessageBrokerMessage      = errors.New("message broker: message error")
	ErrMessageBrokerDisconnected = errors.New("message broker: disconnected")
	ErrMessageBrokerClose        = errors.New("message broker: close error")
)

type Message struct {
	Body []byte
}

type ConnSettings struct {
	Hostname string
	Port     int
}

type Broker interface {

	//Comienza a escuchar a la cola/exchange e invoca a callbackFunc tras
	//cada mensaje de datos o de control con el cuerpo del mensaje.
	//callbackFunc tiene como parámetro:
	// msg - El struct tal y como lo recibe el método Send.
	// ack - Una función que hace ACK del mensaje recibido.
	// nack - Una función que hace NACK del mensaje recibido.
	//Si se pierde la conexión con el middleware devuelve ErrMessageBrokerDisconnected.
	//Si ocurre un error interno que no puede resolverse devuelve ErrMessageBrokerMessage.
	StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error

	//Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
	//no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
	//Si se pierde la conexión con el middleware devuelve ErrMessageBrokerDisconnected.
	StopConsuming() error

	//Envía un mensaje a la cola o a los tópicos con el que se inicializó el exchange.
	//Si se pierde la conexión con el middleware devuelve ErrMessageBrokerDisconnected.
	//Si ocurre un error interno que no puede resolverse devuelve ErrMessageBrokerMessage.
	Send(msg Message) error

	//Se desconecta de la cola o exchange al que estaba conectado.
	//Si ocurre un error interno que no puede resolverse devuelve ErrMessageBrokerClose.
	Close() error
}

func NewBroker(cfg config.BrokerConfig) (Broker, error) {
	cfg = parseBrokerDefaults(cfg)
	switch cfg.Type {
	case "q_q":
		return NewQueueToQueueBroker(cfg)
	case "q_e":
		return NewQueueToExchangeBroker(cfg)
	default:
		return nil, errors.New("unsupported broker type: " + cfg.Type)
	}
}

func parseBrokerDefaults(cfg config.BrokerConfig) config.BrokerConfig {
	if cfg.ExchangeType == "" {
		cfg.ExchangeType = "direct"
	}
	if cfg.Prefetch == 0 {
		cfg.Prefetch = 30
	}
	return cfg
}
