package internal

import (
	"testing"
)

func TestMap(t *testing.T) {
	/*
		cases := []struct {
			name       string
			mapFlowGen func(chan struct{}) *Map[int, int]
		}{
			{
				name: "normal",
				mapFlowGen: func(ctrl chan struct{}) *Map[int, int] {
					return NewMap(func(i int) int {
						<-ctrl
						return i * i
					}, 1)
				},
			},
		}

		for _, tt := range cases {
			t.Run(tt.name, func(t *testing.T) {
				in := make(chan any)
				out := make(chan any)
				ctrl := make(chan struct{})
				done := make(chan struct{})

				source := ext.NewChanSource(in)
				mf := tt.mapFlowGen(ctrl)
				sink := ext.NewChanSink(out)

				go func() {
					source.Via(mf).To(sink)
				}()

				// ingest
				in <- 1
				go func() {
					in <- 1
					in <- 1
					done <- struct{}{}
					close(in)
				}()
				if !isBlocked(done) {
					t.Fatalf("expected blocked channel")
				}
				if mf.workers.Load() != 1 {
					t.Fatalf("expected 1 worker")
				}

				mf.SetParallelism(2)
				if isBlocked(done) {
					t.Fatalf("expected unblocked channel")
				}
				if mf.workers.Load() != 2 {
					t.Fatalf("expected 2 workers")
				}
			})
		}
	*/
}
