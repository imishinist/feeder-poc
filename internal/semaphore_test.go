package internal

import (
	"testing"
	"time"
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
			if sem.current != 2 {
				t.Errorf("current should be 2 but got %d", sem.current)
			}
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
			if sem.current != 2 {
				t.Errorf("current should be 2 but got %d", sem.current)
			}
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
			if sem.current != 4 {
				t.Errorf("current should be 4 but got %d", sem.current)
			}
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
			if sem.current != 2 {
				t.Errorf("current should be 2 but got %d", sem.current)
			}
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
			if sem.current != 4 {
				t.Errorf("current should be 4 but got %d", sem.current)
			}
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
			if sem.current != 0 {
				t.Errorf("current should be 0 but got %d", sem.current)
			}

			go func() {
				sem.Release()
				done <- struct{}{}
			}()
			if isBlocked(done) {
				t.Errorf("Release() should not be blocked otherwise there is capacity")
			}
			if sem.current != 1 {
				t.Errorf("current should be 1 but got %d", sem.current)
			}
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
			if sem.current != 0 {
				t.Errorf("current should be 0 but got %d", sem.current)
			}

			sem.Release()
			if isBlocked(done) {
				t.Errorf("Set() (decrease) go through after Release() has been called")
			}
			if sem.current != 0 {
				t.Errorf("current should be 0 but got %d", sem.current)
			}
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
			if sem.current != 1 {
				t.Errorf("current should be 1 but got %d", sem.current)
			}
		})
	})
}
