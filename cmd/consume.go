package cmd

import (
	"context"
	"log/slog"
	"slices"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/spf13/cobra"

	"github.com/imishinist/feeder-poc/internal"
)

type Consumer struct {
	client *sqs.Client

	queueURL            string
	maxNumberOfMessages int
	waitTimeSeconds     int
}

func NewConsumer(ctx context.Context, queueURL string) *Consumer {
	client, err := NewSQSClient(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "create sqs client error", slog.Any("error", err))
		panic(err)
	}
	return &Consumer{
		client:              client,
		queueURL:            queueURL,
		maxNumberOfMessages: 10,
		waitTimeSeconds:     5,
	}
}

func (c *Consumer) Consume(ctx context.Context) ([]*internal.QueueMessage[internal.Message], error) {
	res, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &c.queueURL,
		MaxNumberOfMessages: int32(c.maxNumberOfMessages),
		WaitTimeSeconds:     int32(c.waitTimeSeconds),
	})
	if err != nil {
		return nil, err
	}

	messages := make([]*internal.QueueMessage[internal.Message], 0, len(res.Messages))
	for _, m := range res.Messages {
		message, err := internal.ParseMessage(*m.Body)
		if err != nil {
			// parse error
			slog.Error("parse message error", slog.Any("error", err), slog.String("body", *m.Body))
			return nil, err
		}
		messages = append(messages, &internal.QueueMessage[internal.Message]{
			ReceiptHandle: *m.ReceiptHandle,
			Message:       message,
		})
	}

	return messages, nil
}

func (c *Consumer) DeleteMessage(ctx context.Context, receiptHandles []string) error {
	if receiptHandles == nil || len(receiptHandles) == 0 {
		return nil
	}

	entriesChunk := make([][]types.DeleteMessageBatchRequestEntry, 0, len(receiptHandles)/10+1)
	for chunk := range slices.Chunk(receiptHandles, 10) {
		entries := make([]types.DeleteMessageBatchRequestEntry, 0, len(chunk))
		for i, rh := range chunk {
			id := strconv.Itoa(i)
			entries = append(entries, types.DeleteMessageBatchRequestEntry{
				Id:            &id,
				ReceiptHandle: &rh,
			})
		}
		if len(entries) != 0 {
			entriesChunk = append(entriesChunk, entries)
		}
	}

	for _, entries := range entriesChunk {
		res, err := c.client.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
			QueueUrl: &c.queueURL,
			Entries:  entries,
		})
		if err != nil {
			return err
		}
		slog.Info("delete message", slog.Int("successful", len(res.Successful)), slog.Int("failed", len(res.Failed)))
	}

	return nil
}

var (
	perMessageInterval = 10 * time.Millisecond
)

// consumeCmd represents the consume command
var consumeCmd = &cobra.Command{
	Use:  "consume",
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		queueURL := args[0]

		ctx := cmd.Context()
		consumer := NewConsumer(ctx, queueURL)
		for {
			messages, err := consumer.Consume(ctx)
			if err != nil {
				slog.ErrorContext(ctx, "consume error", slog.Any("error", err))
				break
			}

			receiptHandles := make([]string, 0)
			for _, m := range messages {
				slog.InfoContext(ctx, "consume", slog.Any("message", m.Message))
				Sleep(perMessageInterval)
				receiptHandles = append(receiptHandles, m.ReceiptHandle)
			}
			if err := consumer.DeleteMessage(ctx, receiptHandles); err != nil {
				slog.ErrorContext(ctx, "delete message error", slog.Any("error", err))
				break
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(consumeCmd)

	consumeCmd.Flags().DurationVar(&perMessageInterval, "per-message-interval", perMessageInterval, "Interval between messages")
}
