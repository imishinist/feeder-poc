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
			assert.Equal(t, 2, sem.current, "current should be 2")
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
			assert.Equal(t, 2, sem.current, "current should be 2")
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
			assert.Equal(t, 4, sem.current, "current should be 4")
		})

		t.Run("Add -1", func(t *testing.T) {
			sem := NewDynamicSemaphore(3)
			done := make(chan struct{})
			go func() {
				sem.Add(-1)
				done <- struct{}{}
			}()
			if isBlocked(done) {
				t.Errorf("Add() should not be blocked when there is capacity")
			}
			assert.Equal(t, 2, sem.current, "current should be 2")
		})

		t.Run("Add 1", func(t *testing.T) {
			sem := NewDynamicSemaphore(3)
			done := make(chan struct{})
			go func() {
				sem.Add(1)
				done <- struct{}{}
			}()
			if isBlocked(done) {
				t.Errorf("Add() should not be blocked otherwise there is capacity")
			}
			assert.Equal(t, 4, sem.current, "current should be 4")
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
			assert.Equal(t, 0, sem.current, "current should be 0")

			go func() {
				sem.Release()
				done <- struct{}{}
			}()
			if isBlocked(done) {
				t.Errorf("Release() should not be blocked otherwise there is capacity")
			}
			assert.Equal(t, 1, sem.current, "current should be 1")
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
			if !isBlocked(done) {
				t.Errorf("Set() (decrease) should be blocked when there is no capacity")
			}
			assert.Equal(t, 0, sem.current, "current should be 0")

			sem.Release()
			if isBlocked(done) {
				t.Errorf("Set() (decrease) go through after Release() has been called")
			}
			assert.Equal(t, 0, sem.current, "current should be 0")
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
			assert.Equal(t, 1, sem.current, "current should be 1")
		})
	})
}
