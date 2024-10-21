package internal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	BlockTimeout = 100 * time.Millisecond
)

func isBlocked(done chan struct{}) bool {
	select {
	case <-done:
		return false
	case <-time.After(BlockTimeout):
		return true
	}
}

func TestDynamicSemaphore(t *testing.T) {
	t.Run("has capacity", func(t *testing.T) {
		t.Run("Acquire", func(t *testing.T) {
			sem := NewDynamicSemaphore(3)
			done := make(chan struct{})
			go func() {
				sem.Acquire()
				done <- struct{}{}
			}()
			if isBlocked(done) {
				t.Errorf("Acquire() should not be blocked when there is capacity")
			}
			assert.Equal(t, 3, sem.Capacity(), "capacity should be 3")
			assert.Equal(t, 1, sem.count, "count should be 1")
		})

		t.Run("Set 3 to 2 (decrease)", func(t *testing.T) {
			sem := NewDynamicSemaphore(3)
			done := make(chan struct{})
			go func() {
				sem.Set(2)
				done <- struct{}{}
			}()
			if isBlocked(done) {
				t.Errorf("Set() should not be blocked when there is capacity")
			}
			assert.Equal(t, 2, sem.Capacity(), "capacity should be 2")
			assert.Equal(t, 0, sem.count, "count should be 1")
		})

		t.Run("Set 3 to 4 (increase)", func(t *testing.T) {
			sem := NewDynamicSemaphore(3)
			done := make(chan struct{})
			go func() {
				sem.Set(4)
				done <- struct{}{}
			}()
			if isBlocked(done) {
				t.Errorf("Set() should not be blocked otherwise there is capacity")
			}
			assert.Equal(t, 4, sem.Capacity(), "capacity should be 4")
			assert.Equal(t, 0, sem.count, "count should be 1")
		})
	})

	t.Run("no capacity", func(t *testing.T) {
		t.Run("Acquire", func(t *testing.T) {
			sem := NewDynamicSemaphore(3)
			for i := 0; i < 3; i++ {
				sem.Acquire()
			}
			done := make(chan struct{})

			go func() {
				sem.Acquire()
				done <- struct{}{}
			}()
			if !isBlocked(done) {
				t.Errorf("Acquire() should be blocked when there is no capacity")
			}
			assert.Equal(t, 3, sem.Capacity(), "capacity should be 3")
			assert.Equal(t, 3, sem.count, "could should be 3")

			go func() {
				sem.Release()
				done <- struct{}{}
			}()
			if isBlocked(done) {
				t.Errorf("Release() should not be blocked otherwise there is capacity")
			}
			assert.Equal(t, 2, sem.count, "could should be 2")
		})

		t.Run("Set 3 to 2 (decrease)", func(t *testing.T) {
			sem := NewDynamicSemaphore(3)
			for i := 0; i < 3; i++ {
				sem.Acquire()
			}
			done := make(chan struct{})

			go func() {
				sem.Set(2)
				done <- struct{}{}
			}()
			if isBlocked(done) {
				t.Fatalf("Set() (decrease) should not be blocked when there is no capacity")
			}
			assert.Equal(t, 2, sem.Capacity(), "capacity should be 2")
			assert.Equal(t, 3, sem.count, "could should be 3")

			sem.Release()
			assert.Equal(t, 2, sem.Capacity(), "capacity should be 2")
			assert.Equal(t, 2, sem.count, "could should be 2")
		})

		t.Run("Set 3 to 4 (increase)", func(t *testing.T) {
			sem := NewDynamicSemaphore(3)
			for i := 0; i < 3; i++ {
				sem.Acquire()
			}
			done := make(chan struct{})

			go func() {
				sem.Set(4)
				done <- struct{}{}
			}()
			if isBlocked(done) {
				t.Errorf("Set() (increase) should not be blocked otherwise there is capacity")
			}
			assert.Equal(t, 4, sem.Capacity(), "capacity should be 4")
			assert.Equal(t, 3, sem.count, "could should be 3")
		})
	})
}
