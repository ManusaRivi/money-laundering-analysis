package batch

import (
	"github.com/google/uuid"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
)

const DefaultSize = 100

// FlushFunc recibe un lote listo para encodear y publicar. Se invoca con
// todos los items acumulados para un (cliente, routing key) cuando el bucket
// llega a `size` o cuando se fuerza el flush (p. ej. antes de emitir EOF).
type FlushFunc[T any] func(clientID uuid.UUID, key broker.KeyType, items []T) error

// Buffer acumula items por (cliente, routing key) para publicarlos en lotes.
// Un mensaje AMQP tiene una única routing key y un único cliente, por lo que
// los buckets nunca se mezclan. No es thread-safe: está pensado para usarse
// desde el único goroutine consumidor de un worker.
type Buffer[T any] struct {
	size    int
	flush   FlushFunc[T]
	buckets map[uuid.UUID]map[broker.KeyType][]T
}

func NewBuffer[T any](size int, flush FlushFunc[T]) *Buffer[T] {
	if size <= 0 {
		size = DefaultSize
	}
	return &Buffer[T]{
		size:    size,
		flush:   flush,
		buckets: make(map[uuid.UUID]map[broker.KeyType][]T),
	}
}

// Add acumula un item y dispara el flush del bucket si alcanzó el tamaño de lote.
func (b *Buffer[T]) Add(clientID uuid.UUID, key broker.KeyType, item T) error {
	byKey, ok := b.buckets[clientID]
	if !ok {
		byKey = make(map[broker.KeyType][]T)
		b.buckets[clientID] = byKey
	}
	items := append(byKey[key], item)
	if len(items) >= b.size {
		delete(byKey, key)
		return b.flush(clientID, key, items)
	}
	byKey[key] = items
	return nil
}

// FlushClient publica todos los buckets parciales de un cliente y libera su
// estado. DEBE llamarse antes de emitir el EOF de ese cliente: el conteo del
// EOF promete que todos los resultados previos ya fueron publicados.
func (b *Buffer[T]) FlushClient(clientID uuid.UUID) error {
	byKey, ok := b.buckets[clientID]
	if !ok {
		return nil
	}
	delete(b.buckets, clientID)
	for key, items := range byKey {
		if len(items) == 0 {
			continue
		}
		if err := b.flush(clientID, key, items); err != nil {
			return err
		}
	}
	return nil
}
