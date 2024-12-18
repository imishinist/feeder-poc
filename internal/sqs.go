package internal

import (
	"context"
	"expvar"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

type QueueMessage[T any] struct {
	ReceiptHandle string
	Message       *T
}

type SQSSourceConfig struct {
	QueueURL string

	MaxNumberOfMessages int
	WaitTimeSeconds     int

	Parallelism int
}

type SQSSource struct {
	client   *sqs.Client
	config   *SQSSourceConfig
	reloaded chan struct{}

	out chan any

	Metrics    *expvar.Map
	goroutines *expvar.Int
}

var _ streams.Source = (*SQSSource)(nil)

func NewSQSSource(ctx context.Context, client *sqs.Client, config *SQSSourceConfig) *SQSSource {
	goroutines := new(expvar.Int)
	metrics := new(expvar.Map)
	metrics.Set("Goroutines", goroutines)

	sqsSource := &SQSSource{
		client:     client,
		config:     config,
		reloaded:   make(chan struct{}),
		out:        make(chan any),
		Metrics:    metrics,
		goroutines: goroutines,
	}
	go sqsSource.receive(ctx)
	return sqsSource
}

func (ss *SQSSource) receive(ctx context.Context) {
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		close(ss.out)
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := NewDynamicSemaphore(ss.config.Parallelism)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ss.reloaded:
			sem.Set(ss.config.Parallelism)
		default:
		}

		sem.Acquire()
		wg.Add(1)
		ss.goroutines.Add(1)
		go func() {
			defer sem.Release()
			defer wg.Done()
			defer ss.goroutines.Add(-1)

			result, err := ss.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:            &ss.config.QueueURL,
				MaxNumberOfMessages: int32(ss.config.MaxNumberOfMessages),
				WaitTimeSeconds:     int32(ss.config.WaitTimeSeconds),
			})
			if err != nil {
				cancel()
				return
			}

			for _, message := range result.Messages {
				m := QueueMessage[string]{
					ReceiptHandle: *message.ReceiptHandle,
					Message:       message.Body,
				}
				select {
				case <-ss.reloaded:
					sem.Set(ss.config.Parallelism)
					ss.out <- m
				case ss.out <- m:
				}
			}
		}()
	}
}

func (ss *SQSSource) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(ss, operator)
	return operator
}

func (ss *SQSSource) Out() <-chan any {
	return ss.out
}

func (ss *SQSSource) ReloadConfig(config *SQSSourceConfig) {
	ss.config = config
	ss.goroutines.Add(1)
	go func() {
		defer ss.goroutines.Add(-1)

		ss.reloaded <- struct{}{}
	}()
}
