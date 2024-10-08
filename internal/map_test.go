package internal

import (
	"sort"
	"sync"
	"testing"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/stretchr/testify/assert"
)

func readChanAsSlice[T any](ch <-chan T) []T {
	var result []T
	for v := range ch {
		result = append(result, v)
	}
	return result
}

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
			assert.LessOrEqual(t, mf.workers.Load(), int32(1), "workers should less than or equal to 1")
		}
		mf.SetParallelism(3)
		for i := 3; i < 6; i++ {
			in <- i
			assert.LessOrEqual(t, mf.workers.Load(), int32(3), "workers should less than or equal to 3")
		}
		mf.SetParallelism(1)
		// 非同期的に workers が減るのを待つので、待ち時間を入れる
		time.Sleep(60 * time.Millisecond)
		for i := 6; i < 9; i++ {
			in <- i
			assert.LessOrEqual(t, mf.workers.Load(), int32(1), "workers should less than or equal to 1")
		}
		close(in)

		wg.Wait()

		data := readChanAsSlice(out)
		sort.Slice(data, func(i, j int) bool {
			return data[i].(int) < data[j].(int)
		})
		assert.Equal(t, data, []any{
			0, 1, 4, 9, 16, 25, 36, 49, 64,
		})
	})
}
