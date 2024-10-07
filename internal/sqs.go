package internal

import (
	"context"
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
	client *sqs.Client
	config *SQSSourceConfig

	out chan any
}

var _ streams.Source = (*SQSSource)(nil)

func NewSQSSource(ctx context.Context, client *sqs.Client, config *SQSSourceConfig) *SQSSource {
	sqsSource := &SQSSource{
		client: client,
		config: config,
		out:    make(chan any),
	}
	go sqsSource.receive(ctx)
	return sqsSource
}

func (ss *SQSSource) receive(ctx context.Context) {
	defer close(ss.out)

	var wg sync.WaitGroup
	for i := 0; i < ss.config.Parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				result, err := ss.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
					QueueUrl:            &ss.config.QueueURL,
					MaxNumberOfMessages: int32(ss.config.MaxNumberOfMessages),
					WaitTimeSeconds:     int32(ss.config.WaitTimeSeconds),
				})
				if err != nil {
					return
				}
				for _, message := range result.Messages {
					ss.out <- QueueMessage[string]{
						ReceiptHandle: *message.ReceiptHandle,
						Message:       message.Body,
					}
				}
			}
		}()
	}
	wg.Wait()
}

func (ss *SQSSource) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(ss, operator)
	return operator
}

func (ss *SQSSource) Out() <-chan any {
	return ss.out
}
