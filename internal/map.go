package internal

import (
	"fmt"
	"sync"
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
	defer close(m.out)
	defer close(m.reloaded)

	go func() {
		for {
			select {
			case _, ok := <-m.reloaded:
				if !ok {
					return
				}
				sem.Set(m.parallelism)
			}
		}
	}()

	wg := new(sync.WaitGroup)
	for elem := range m.in {
		sem.Acquire()
		wg.Add(1)
		m.workers.Add(1)
		go func(element T) {
			defer func() {
				m.workers.Add(-1)
				wg.Done()
				sem.Release()
			}()
			
			m.out <- m.mapFunction(element)
		}(elem.(T))
	}

	wg.Wait()
}

func (m *Map[T, R]) SetParallelism(parallelism int) {
	m.parallelism = parallelism
	go func() {
		m.reloaded <- struct{}{}
	}()
}
