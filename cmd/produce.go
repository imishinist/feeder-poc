package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
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
	client, err := NewSQSClient(ctx)
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

func (p *Producer) Produce(ctx context.Context, message []*internal.Message) error {
	if err := p.limiter.Wait(ctx); err != nil {
		return err
	}

	entries := make([]types.SendMessageBatchRequestEntry, 0, len(message))
	for i, m := range message {
		body, err := m.Encode()
		if err != nil {
			return err
		}
		id := strconv.Itoa(i)
		entries = append(entries, types.SendMessageBatchRequestEntry{
			Id:          &id,
			MessageBody: &body,
		})
	}

	if _, err := p.client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: &p.queueURL,
		Entries:  entries,
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

		for i := 1; i <= n; i++ {
			select {
			case <-ctx.Done():
				return
			case ch <- internal.Message{
				Collection: "BRent",
				MemberID:   fmt.Sprintf("%d", i),
				Force:      false,
				EnqueueAt:  time.Now(),
			}:
			}
		}
	}()
	return ch
}

func produce(ctx context.Context, producer *Producer, messageCh chan internal.Message) error {
	buf := make([]*internal.Message, 0, 10)
	for message := range messageCh {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		slog.InfoContext(ctx, "produce", slog.Any("message", message))
		buf = append(buf, &message)
		if len(buf) < 10 {
			continue
		}

		if err := producer.Produce(ctx, buf); err != nil {
			return err
		}
		buf = buf[:0]
	}
	if len(buf) > 0 {
		if err := producer.Produce(ctx, buf); err != nil {
			return err
		}
	}
	return nil
}

func NewSQSClient(ctx context.Context) (*sqs.Client, error) {
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
	Args: cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		queueURL := args[0]
		numMessage, err := strconv.Atoi(args[1])
		if err != nil {
			slog.ErrorContext(cmd.Context(), "invalid number of messages", slog.Any("error", err))
			return
		}

		ctx := cmd.Context()
		eg := new(errgroup.Group)
		limiter := rate.NewLimiter(rate.Limit(throughput), 1)
		producer := NewProducer(ctx, limiter, queueURL)

		messageCh := SourceMessages(ctx, numMessage)
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
