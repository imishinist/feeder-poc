package internal

import (
	"sync"
	"testing"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/stretchr/testify/assert"
)

func TestBatch(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		in := make(chan any)
		out := make(chan any, 20)

		source := ext.NewChanSource(in)
		batch := NewBatch[int](4, 100*time.Millisecond)
		sink := ext.NewChanSink(out)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			source.Via(batch).To(sink)
		}()

		for i := 0; i < 5; i++ {
			in <- i
		}
		time.Sleep(200 * time.Millisecond)

		for i := 5; i < 10; i++ {
			in <- i
		}
		time.Sleep(200 * time.Millisecond)

		batch.SetConfig(6, 100*time.Millisecond)

		for i := 10; i < 20; i++ {
			in <- i
		}
		time.Sleep(200 * time.Millisecond)

		batch.SetConfig(4, 100*time.Millisecond)

		close(in)
		wg.Wait()

		var outputValues [][]int
		for e := range sink.Out {
			outputValues = append(outputValues, e.([]int))
		}
		assert.Equal(t, outputValues[:4], [][]int{
			{0, 1, 2, 3},
			{4},
			{5, 6, 7, 8},
			{9},
		})
		assert.Equal(t, len(outputValues), 6)
		for i := 4; i < 6; i++ {
			assert.LessOrEqual(t, len(outputValues[i]), 6)
			assert.GreaterOrEqual(t, len(outputValues[i]), 4)
		}
	})
}
