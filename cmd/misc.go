package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
	"github.com/spf13/cobra"

	"github.com/imishinist/feeder-poc/internal"
)

type Message internal.QueueMessage[internal.Message]

func Convert(msg internal.QueueMessage[string]) Message {
	message, err := internal.ParseMessage(*msg.Message)
	if err != nil {
		return Message{
			ReceiptHandle: msg.ReceiptHandle,
			Message:       nil,
		}
	}
	return Message{
		ReceiptHandle: msg.ReceiptHandle,
		Message:       message,
	}
}

func Dedupe() func(Message) bool {
	return func(msg Message) bool {
		return true
	}
}

func DeleteMessages(ctx context.Context, client *sqs.Client, queueURL string) func([]Message) []Message {
	return func(msgs []Message) []Message {
		entries := make([]types.DeleteMessageBatchRequestEntry, 0, len(msgs))
		for i, msg := range msgs {
			if msg.Message == nil {
				log.Println(msg.Message)
				continue
			}

			id := strconv.Itoa(i)
			entries = append(entries, types.DeleteMessageBatchRequestEntry{
				Id:            &id,
				ReceiptHandle: &msg.ReceiptHandle,
			})
		}
		if len(entries) == 0 {
			return msgs
		}

		_, _ = client.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
			QueueUrl: &queueURL,
			Entries:  entries,
		})
		return msgs
	}
}

func Do() func(Message) Message {
	return func(msg Message) (ret Message) {
		ret = msg

		if msg.Message == nil {
			return
		}

		enqueueAt := msg.Message.EnqueueAt.UnixMilli()
		force := "0"
		if msg.Message.Force {
			force = "1"
		}

		command := fmt.Sprintf("bin/work.sh %s %s %s %d >> logs/work.out 2>> logs/work.err", msg.Message.Collection, msg.Message.MemberID, force, enqueueAt)
		cmd := exec.Command("/bin/sh", "-c", command)
		if err := cmd.Run(); err != nil {
			log.Println(err)
			return
		}

		return
	}
}

type ConsumerConfig struct {
	QueueURL        string
	BatchSize       int
	WaitTimeSeconds int

	MaxReceiveWorker int
	MaxProcessWorker int
}

var (
	consumerConfig = &ConsumerConfig{
		BatchSize:        10,
		WaitTimeSeconds:  20,
		MaxReceiveWorker: 4,
		MaxProcessWorker: 4,
	}
)

// miscCmd represents the misc command
var miscCmd = &cobra.Command{
	Use:  "misc",
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		consumerConfig.QueueURL = args[0]

		ctx, cancel := context.WithCancel(cmd.Context())
		defer cancel()

		client, err := NewSQSClient(ctx)
		if err != nil {
			panic(err)
		}

		sqsConfig := &internal.SQSSourceConfig{
			QueueURL:            consumerConfig.QueueURL,
			MaxNumberOfMessages: consumerConfig.BatchSize,
			WaitTimeSeconds:     consumerConfig.WaitTimeSeconds,
			Parallelism:         consumerConfig.MaxReceiveWorker,
		}
		source := internal.NewSQSSource(ctx, client, sqsConfig)
		convert := internal.NewMap(Convert, consumerConfig.MaxReceiveWorker)
		dedupe := internal.NewFilter(Dedupe(), consumerConfig.MaxReceiveWorker)
		doFlow := internal.NewMap(Do(), consumerConfig.MaxProcessWorker)
		batchFlow := flow.NewBatch[Message](consumerConfig.BatchSize, time.Second*10)
		deleteFlow := internal.NewMap(DeleteMessages(ctx, client, consumerConfig.QueueURL), consumerConfig.MaxReceiveWorker)

		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGHUP)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-c:
					sqsConfig.Parallelism += 1
					source.ReloadConfig(sqsConfig)

					consumerConfig.MaxReceiveWorker += 1
					consumerConfig.MaxProcessWorker += 1
					convert.SetParallelism(consumerConfig.MaxReceiveWorker)
					dedupe.SetParallelism(consumerConfig.MaxReceiveWorker)
					doFlow.SetParallelism(consumerConfig.MaxProcessWorker)
					deleteFlow.SetParallelism(consumerConfig.MaxReceiveWorker)
				}
			}
		}()

		source.
			Via(convert).
			Via(dedupe).
			Via(doFlow).
			Via(batchFlow).
			Via(deleteFlow).
			To(ext.NewIgnoreSink())
	},
}

func init() {
	rootCmd.AddCommand(miscCmd)

	miscCmd.Flags().IntVarP(&consumerConfig.BatchSize, "batch-size", "b", consumerConfig.BatchSize, "batch size")
	miscCmd.Flags().IntVarP(&consumerConfig.WaitTimeSeconds, "wait-time-seconds", "w", consumerConfig.WaitTimeSeconds, "wait time seconds")
	miscCmd.Flags().IntVarP(&consumerConfig.MaxReceiveWorker, "max-receive-worker", "r", consumerConfig.MaxReceiveWorker, "max receive worker")
	miscCmd.Flags().IntVarP(&consumerConfig.MaxProcessWorker, "max-process-worker", "p", consumerConfig.MaxProcessWorker, "max process worker")
}
