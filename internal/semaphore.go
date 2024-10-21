package internal

import (
	"sync"
)

type DynamicSemaphore struct {
	capacity int
	count    int
	mu       sync.Mutex
	cond     *sync.Cond
}

// NewDynamicSemaphore creates a new DynamicSemaphore with the specified initial size.
func NewDynamicSemaphore(initialCapacity int) *DynamicSemaphore {
	ds := &DynamicSemaphore{
		capacity: initialCapacity,
	}
	ds.cond = sync.NewCond(&ds.mu)
	return ds
}

// Acquire tries to acquire a semaphore slot, blocking until one becomes available.
func (ds *DynamicSemaphore) Acquire() {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	for ds.count >= ds.capacity {
		ds.cond.Wait()
	}
	ds.count++
}

// Release releases a semaphore slot, signaling any waiting goroutines.
func (ds *DynamicSemaphore) Release() {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.count > 0 {
		ds.count--
		ds.cond.Signal()
	}
}

// Set sets the maximum size of the semaphore.
func (ds *DynamicSemaphore) Set(capacity int) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	ds.capacity = capacity
	ds.cond.Broadcast()
}

func (ds *DynamicSemaphore) Capacity() int {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	return ds.capacity
}
