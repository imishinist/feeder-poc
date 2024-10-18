package internal

import (
	"fmt"
	"time"

	"github.com/reugn/go-streams"
)

type Batch[T any] struct {
	maxBatchSize int
	timeInterval time.Duration
	in           chan any
	out          chan any

	reloaded chan struct{}
}

var _ streams.Flow = (*Batch[any])(nil)

func NewBatch[T any](maxBatchSize int, timeInterval time.Duration) *Batch[T] {
	if maxBatchSize < 1 {
		panic(fmt.Sprintf("nonpositive maxBatchSize: %d", maxBatchSize))
	}
	batchFlow := &Batch[T]{
		maxBatchSize: maxBatchSize,
		timeInterval: timeInterval,
		in:           make(chan any),
		out:          make(chan any),
		reloaded:     make(chan struct{}),
	}
	go batchFlow.batchStream()

	return batchFlow
}

func (b *Batch[T]) Via(flow streams.Flow) streams.Flow {
	go b.transmit(flow)
	return flow
}

func (b *Batch[T]) To(sink streams.Sink) {
	b.transmit(sink)
}

func (b *Batch[T]) Out() <-chan any {
	return b.out
}

func (b *Batch[T]) In() chan<- any {
	return b.in
}

func (b *Batch[T]) transmit(inlet streams.Inlet) {
	for batch := range b.out {
		inlet.In() <- batch
	}
	close(inlet.In())
}

func (b *Batch[T]) batchStream() {
	ticker := time.NewTicker(b.timeInterval)
	defer ticker.Stop()
	defer close(b.out)

	maxBatchSize := b.maxBatchSize
	timeInterval := b.timeInterval
	batch := make([]T, 0, maxBatchSize)
	for {
		select {
		case <-b.reloaded:
			maxBatchSize = b.maxBatchSize
			timeInterval = b.timeInterval
		case element, ok := <-b.in:
			if !ok {
				if len(batch) > 0 {
					b.out <- batch
				}
				return
			}

			batch = append(batch, element.(T))
			if len(batch) >= maxBatchSize {
				b.out <- batch
				batch = make([]T, 0, maxBatchSize)
			}
			ticker.Reset(timeInterval)
		case <-ticker.C:
			if len(batch) > 0 {
				b.out <- batch
				batch = make([]T, 0, b.maxBatchSize)
			}
		}
	}
}

func (b *Batch[T]) SetConfig(maxBatchSize int, timeInterval time.Duration) {
	b.maxBatchSize = maxBatchSize
	b.timeInterval = timeInterval
	go func() {
		b.reloaded <- struct{}{}
	}()
}
