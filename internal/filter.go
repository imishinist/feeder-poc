package internal

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

type Filter[T any] struct {
	filterPredicate flow.FilterPredicate[T]
	in              chan any
	out             chan any
	parallelism     int

	workers  atomic.Int32
	reloaded chan struct{}
}

var _ streams.Flow = (*Filter[any])(nil)

func NewFilter[T any](filterPredicate flow.FilterPredicate[T], parallelism int) *Filter[T] {
	if parallelism < 1 {
		panic(fmt.Sprintf("nonpositive Filter parallelism: %d", parallelism))
	}
	filter := &Filter[T]{
		filterPredicate: filterPredicate,
		in:              make(chan any),
		out:             make(chan any),
		parallelism:     parallelism,
		workers:         atomic.Int32{},
		reloaded:        make(chan struct{}),
	}
	go filter.doStream()

	return filter
}

func (f *Filter[T]) Via(flow streams.Flow) streams.Flow {
	go f.transmit(flow)
	return flow
}

func (f *Filter[T]) To(sink streams.Sink) {
	f.transmit(sink)
}

func (f *Filter[T]) Out() <-chan any {
	return f.out
}

func (f *Filter[T]) In() chan<- any {
	return f.in
}

func (f *Filter[T]) transmit(inlet streams.Inlet) {
	for element := range f.Out() {
		inlet.In() <- element
	}
	close(inlet.In())
}

// doStream discards items that don't match the filter predicate.
func (f *Filter[T]) doStream() {
	sem := NewDynamicSemaphore(f.parallelism)
	defer close(f.out)
	defer close(f.reloaded)

	go func() {
		for {
			select {
			case _, ok := <-f.reloaded:
				if !ok {
					return
				}
				sem.Set(f.parallelism)
			}
		}
	}()

	wg := new(sync.WaitGroup)
	for elem := range f.in {
		sem.Acquire()
		wg.Add(1)
		f.workers.Add(1)
		go func(element T) {
			defer func() {
				f.workers.Add(-1)
				wg.Done()
				sem.Release()
			}()

			if f.filterPredicate(element) {
				f.out <- element
			}
		}(elem.(T))
	}
	wg.Wait()
}

func (f *Filter[T]) SetParallelism(parallelism int) {
	f.parallelism = parallelism
	go func() {
		f.reloaded <- struct{}{}
	}()
}
