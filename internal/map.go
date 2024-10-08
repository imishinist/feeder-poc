package internal

import (
	"fmt"
	"sync/atomic"

	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

type Map[T, R any] struct {
	mapFunction flow.MapFunction[T, R]
	in          chan any
	out         chan any
	parallelism int

	workers  atomic.Int32
	reloaded chan struct{}
}

var _ streams.Flow = (*Map[any, any])(nil)

func NewMap[T, R any](mapFunction flow.MapFunction[T, R], parallelism int) *Map[T, R] {
	if parallelism < 1 {
		panic(fmt.Sprintf("nonpositive Map parallelism: %d", parallelism))
	}
	mapFlow := &Map[T, R]{
		mapFunction: mapFunction,
		in:          make(chan any),
		out:         make(chan any),
		parallelism: parallelism,
		workers:     atomic.Int32{},
		reloaded:    make(chan struct{}),
	}
	go mapFlow.doStream()

	return mapFlow
}

func (m *Map[T, R]) Via(flow streams.Flow) streams.Flow {
	go m.transmit(flow)
	return flow
}

func (m *Map[T, R]) To(sink streams.Sink) {
	m.transmit(sink)
}

func (m *Map[T, R]) Out() <-chan any {
	return m.out
}

func (m *Map[T, R]) In() chan<- any {
	return m.in
}

func (m *Map[T, R]) transmit(inlet streams.Inlet) {
	for element := range m.Out() {
		inlet.In() <- element
	}
	close(inlet.In())
}

func (m *Map[T, R]) doStream() {
	sem := NewDynamicSemaphore(m.parallelism)

	for elem := range m.in {
		select {
		case <-m.reloaded:
			sem.Set(m.parallelism)
		default:
		}
		sem.Acquire()
		m.workers.Add(1)
		go func(element T) {
			defer sem.Release()
			defer m.workers.Add(-1)

			result := m.mapFunction(element)
			select {
			case <-m.reloaded:
				sem.Set(m.parallelism)
				m.out <- result
			case m.out <- result:
			}
		}(elem.(T))
	}
	close(m.out)
}

func (m *Map[T, R]) SetParallelism(parallelism int) {
	m.parallelism = parallelism
	go func() {
		m.reloaded <- struct{}{}
	}()
}
