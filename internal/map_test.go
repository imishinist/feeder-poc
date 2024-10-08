package internal

import (
	"sync"
	"testing"
	"time"

	ext "github.com/reugn/go-streams/extension"
)

func TestMap(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		in := make(chan any)
		out := make(chan any, 10)

		source := ext.NewChanSource(in)
		mf := NewMap(func(in int) any {
			time.Sleep(30 * time.Millisecond)
			return in * in
		}, 1)
		sink := ext.NewChanSink(out)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			source.Via(mf).To(sink)
		}()

		for i := 0; i < 3; i++ {
			in <- i
			if mf.workers.Load() > 1 {
				t.Fatalf("workers should less than 1 but got %d", mf.workers.Load())
			}
		}
		mf.SetParallelism(3)
		for i := 0; i < 3; i++ {
			in <- i
			if mf.workers.Load() > 3 {
				t.Fatalf("workers should less than 3 but got %d", mf.workers.Load())
			}
		}
		mf.SetParallelism(1)
		// 非同期的に workers が減るのを待つので、待ち時間を入れる
		time.Sleep(60 * time.Millisecond)
		for i := 0; i < 3; i++ {
			in <- i
			if mf.workers.Load() > 1 {
				t.Fatalf("workers should less than 1 but got %d", mf.workers.Load())
			}
		}
		close(in)

		wg.Wait()
	})
}
