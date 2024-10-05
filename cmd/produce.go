package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"github.com/imishinist/feeder-poc/internal"
)

type Producer struct {
	client  *sqs.Client
	limiter *rate.Limiter

	queueURL string
}

func NewProducer(ctx context.Context, limiter *rate.Limiter, queueURL string) *Producer {
	client, err := CreateSQSClient(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "create sqs client error", slog.Any("error", err))
		panic(err)
	}
	return &Producer{
		client:   client,
		limiter:  limiter,
		queueURL: queueURL,
	}
}

func (p *Producer) Produce(ctx context.Context, message internal.Message) error {
	if err := p.limiter.Wait(ctx); err != nil {
		return err
	}

	body, err := message.Encode()
	if err != nil {
		return err
	}
	if _, err := p.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    &p.queueURL,
		MessageBody: &body,
	}); err != nil {
		return err
	}
	return nil
}

var (
	concurrency = 2
	throughput  = 10
)

func SourceMessages(ctx context.Context, n int) chan internal.Message {
	ch := make(chan internal.Message)
	go func() {
		defer close(ch)

		for i := 0; i < n; i++ {
			select {
			case <-ctx.Done():
				return
			case ch <- internal.Message{
				MemberID:  fmt.Sprintf("%d", i),
				Force:     false,
				EnqueueAt: time.Now().Format(time.RFC3339Nano),
			}:
			}
		}
	}()
	return ch
}

func produce(ctx context.Context, producer *Producer, messageCh chan internal.Message) error {
	for message := range messageCh {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		slog.InfoContext(ctx, "produce", slog.Any("message", message))
		if err := producer.Produce(ctx, message); err != nil {
			return err
		}
	}
	return nil
}

func CreateSQSClient(ctx context.Context) (*sqs.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("ap-northeast-1"))
	if err != nil {
		return nil, err
	}

	endpointURL := os.Getenv("SQS_ENDPOINT_URL")
	if endpointURL == "" {
		return sqs.NewFromConfig(cfg), nil
	}

	return sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(endpointURL)
	}), nil
}

// produceCmd represents the producer command
var produceCmd = &cobra.Command{
	Use:  "produce",
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		queueURL := args[0]

		ctx := cmd.Context()
		eg := new(errgroup.Group)
		limiter := rate.NewLimiter(rate.Limit(throughput), 1)
		producer := NewProducer(ctx, limiter, queueURL)

		messageCh := SourceMessages(ctx, 100)
		for i := 0; i < concurrency; i++ {
			eg.Go(func() error {
				return produce(ctx, producer, messageCh)
			})
		}
		if err := eg.Wait(); err != nil {
			slog.ErrorContext(ctx, "produce error", slog.Any("error", err))
			return
		}
	},
}

func init() {
	rootCmd.AddCommand(produceCmd)

	produceCmd.Flags().IntVarP(&concurrency, "concurrency", "c", concurrency, "Number of concurrent producers")
	produceCmd.Flags().IntVarP(&throughput, "throughput", "t", throughput, "Throughput per second")
}
