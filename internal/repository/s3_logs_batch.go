package repository

import (
	"context"
	"log"
	"sync"
	"time"

	"orchestrator/internal/models"
)

const (
	// batchCheckEvery es cada cuanto el worker revisa lotes vencidos y sube
	// los pendientes.
	batchCheckEvery = 15 * time.Second
	// maxClosedBatches limita la memoria si S3 falla por mucho tiempo: al
	// pasarse, se descartan los lotes mas antiguos.
	maxClosedBatches = 200
	// shutdownFlushTimeout es el tiempo maximo para el volcado final al apagar.
	shutdownFlushTimeout = 15 * time.Second
)

// errorBatch agrupa los errores de un robot. La key se asigna al crear el
// lote, por eso puede referenciarse en el STATE antes de que el objeto exista.
type errorBatch struct {
	key       string
	startedAt time.Time
	entries   []errorLogEntry
}

// BatchingErrorLogStore acumula en memoria los errores de cada robot y sube un
// unico objeto JSON Lines por lote. Un lote se sube cuando cumple
// flushInterval desde su primer error, cuando llega a maxEntries, o al apagar.
// Esta pensado para el proceso EC2 (long-running); en Lambda no sirve porque
// el entorno puede congelarse o destruirse con errores aun en memoria.
type BatchingErrorLogStore struct {
	store         *ErrorLogStore
	flushInterval time.Duration
	maxEntries    int

	mu     sync.Mutex
	open   map[string]*errorBatch // un lote abierto por robot
	closed []*errorBatch          // lotes listos para subir (o por reintentar)
	wake   chan struct{}
}

// NewBatchingErrorLogStore crea el almacen por lotes sobre un ErrorLogStore.
// Hay que ejecutar Run en una goroutine para que los lotes se suban.
func NewBatchingErrorLogStore(store *ErrorLogStore, flushInterval time.Duration, maxEntries int) *BatchingErrorLogStore {
	if maxEntries < 1 {
		maxEntries = 1
	}
	return &BatchingErrorLogStore{
		store:         store,
		flushInterval: flushInterval,
		maxEntries:    maxEntries,
		open:          make(map[string]*errorBatch),
		wake:          make(chan struct{}, 1),
	}
}

// SaveError agrega el error al lote abierto del robot (creandolo si no existe)
// y retorna la key que tendra el objeto. No hace I/O, asi que no agrega
// latencia al ciclo de ejecucion.
func (b *BatchingErrorLogStore) SaveError(_ context.Context, robotId string, now time.Time, result models.ExecutionResult) (string, error) {
	execID, err := newExecutionID()
	if err != nil {
		return "", err
	}

	entry := newErrorLogEntry(robotId, execID, now, result)

	b.mu.Lock()
	batch, ok := b.open[robotId]
	if !ok {
		batch = &errorBatch{
			key:       b.store.objectKey(robotId, now, execID, "jsonl"),
			startedAt: now,
		}
		b.open[robotId] = batch
	}
	batch.entries = append(batch.entries, entry)
	key := batch.key

	full := len(batch.entries) >= b.maxEntries
	if full {
		b.closeLocked(robotId)
	}
	b.mu.Unlock()

	if full {
		select {
		case b.wake <- struct{}{}:
		default:
		}
	}

	return key, nil
}

// closeLocked mueve el lote abierto del robot a la cola de subida.
// El llamador debe tener b.mu tomado.
func (b *BatchingErrorLogStore) closeLocked(robotId string) {
	if batch, ok := b.open[robotId]; ok {
		b.closed = append(b.closed, batch)
		delete(b.open, robotId)
	}
}

func (b *BatchingErrorLogStore) closeExpired(now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for robotId, batch := range b.open {
		if now.Sub(batch.startedAt) >= b.flushInterval {
			b.closeLocked(robotId)
		}
	}
}

func (b *BatchingErrorLogStore) closeAll() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for robotId := range b.open {
		b.closeLocked(robotId)
	}
}

// upload sube los lotes cerrados. Los que fallan se reintentan en la
// siguiente pasada.
func (b *BatchingErrorLogStore) upload(ctx context.Context) {
	b.mu.Lock()
	pending := b.closed
	b.closed = nil
	b.mu.Unlock()

	var failed []*errorBatch
	for _, batch := range pending {
		if err := b.store.putBatch(ctx, batch); err != nil {
			log.Printf("WARN: no se pudo subir el lote %s (%d errores): %v", batch.key, len(batch.entries), err)
			failed = append(failed, batch)
		}
	}

	if len(failed) == 0 {
		return
	}

	b.mu.Lock()
	b.closed = append(failed, b.closed...)
	if over := len(b.closed) - maxClosedBatches; over > 0 {
		log.Printf("WARN: descartando %d lotes antiguos por fallas repetidas al subir a S3", over)
		b.closed = b.closed[over:]
	}
	b.mu.Unlock()
}

// Run es el worker que cierra y sube lotes. Bloquea hasta que ctx se cancele;
// entonces cierra todos los lotes abiertos, los sube con un timeout propio y
// retorna. Cancela ctx solo cuando ya no se vayan a registrar mas ejecuciones.
func (b *BatchingErrorLogStore) Run(ctx context.Context) {
	ticker := time.NewTicker(batchCheckEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownFlushTimeout)
			b.closeAll()
			b.upload(flushCtx)
			cancel()
			return

		case <-ticker.C:
			b.closeExpired(time.Now().UTC())
			b.upload(ctx)

		case <-b.wake:
			b.upload(ctx)
		}
	}
}
