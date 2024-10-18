package internal

import (
	"sync"
)

type DynamicSemaphore struct {
	max     int
	current int
	mu      sync.Mutex
	cond    *sync.Cond
}

// NewDynamicSemaphore creates a new DynamicSemaphore with the specified initial size.
func NewDynamicSemaphore(initialSize int) *DynamicSemaphore {
	ds := &DynamicSemaphore{
		max:     initialSize,
		current: initialSize,
	}
	ds.cond = sync.NewCond(&ds.mu)
	return ds
}

// Acquire tries to acquire a semaphore slot, blocking until one becomes available.
func (ds *DynamicSemaphore) Acquire() {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	for ds.current <= 0 {
		ds.cond.Wait()
	}
	ds.current--
}

// Release releases a semaphore slot, signaling any waiting goroutines.
func (ds *DynamicSemaphore) Release() {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	ds.current++
	ds.cond.Signal()
}

// Add adds or removes slots from the semaphore.
func (ds *DynamicSemaphore) Add(n int) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if n < 0 {
		ds.decrease(-n)
		return
	}
	ds.increase(n)
}

// Set sets the maximum size of the semaphore.
func (ds *DynamicSemaphore) Set(max int) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if max < ds.max {
		ds.decrease(ds.max - max)
		return
	}
	ds.increase(max - ds.max)
}

// increase increases the maximum size of the semaphore.
func (ds *DynamicSemaphore) increase(n int) {
	ds.max += n
	ds.current += n
	ds.cond.Broadcast()
}

// decrease decreases the maximum size of the semaphore.
// It waits until enough slots are available to safely reduce the max size.
func (ds *DynamicSemaphore) decrease(n int) {
	for ds.current < n {
		ds.cond.Wait()
	}
	ds.max -= n
	ds.current -= n
}
