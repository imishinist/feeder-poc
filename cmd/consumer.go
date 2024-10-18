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
	"github.com/spf13/cobra"

	"github.com/imishinist/feeder-poc/config"
	"github.com/imishinist/feeder-poc/internal"
)

type ConsumerWorker struct {
	config *config.ConsumerWorker

	client *sqs.Client

	source      *internal.SQSSource
	convertFlow *internal.Map[internal.QueueMessage[string], Message]
	doFlow      *internal.Map[Message, Message]
	batchFlow   *internal.Batch[Message]
	deleteFlow  *internal.Map[[]Message, []Message]

	batchInterval time.Duration
}

func NewConsumerWorker(ctx context.Context, configFile string) (*ConsumerWorker, error) {
	worker := &ConsumerWorker{}
	if err := worker.LoadConfig(configFile); err != nil {
		return nil, err
	}
	worker.batchInterval = time.Second * 10

	client, err := NewSQSClient(ctx)
	if err != nil {
		return nil, err
	}
	worker.client = client

	sqsConfig := &internal.SQSSourceConfig{
		QueueURL:            worker.config.QueueURL,
		MaxNumberOfMessages: worker.config.BatchSize,
		WaitTimeSeconds:     worker.config.WaitTimeSeconds,
		Parallelism:         worker.config.MaxWorkers,
	}
	worker.source = internal.NewSQSSource(ctx, worker.client, sqsConfig)
	worker.convertFlow = internal.NewMap(worker.Convert, worker.config.MaxWorkers)
	worker.doFlow = internal.NewMap(worker.Do, worker.config.MaxWorkers)
	worker.batchFlow = internal.NewBatch[Message](worker.config.BatchSize, worker.batchInterval)
	worker.deleteFlow = internal.NewMap(worker.DeleteMessage, worker.config.MaxWorkers)

	return worker, nil
}

func (w *ConsumerWorker) Convert(msg internal.QueueMessage[string]) Message {
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

func (w *ConsumerWorker) Do(msg Message) (ret Message) {
	ret = msg

	if msg.Message == nil {
		return
	}

	enqueueAt := fmt.Sprintf("%d", msg.Message.EnqueueAt.UnixMilli())
	force := "0"
	if msg.Message.Force {
		force = "1"
	}

	cmd := exec.Command("bin/work.sh", msg.Message.Collection, msg.Message.MemberID, force, enqueueAt, ">>logs/work.out", "2>>logs/work.err")
	if err := cmd.Run(); err != nil {
		log.Println("command exec error", err)
		return
	}

	return
}

func (w *ConsumerWorker) DeleteMessage(msgs []Message) []Message {
	entries := make([]types.DeleteMessageBatchRequestEntry, 0, len(msgs))
	for i, msg := range msgs {
		// ReceiptHandle が設定されている場合だけ削除する
		if msg.ReceiptHandle == "" {
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
	res, err := w.client.DeleteMessageBatch(context.Background(), &sqs.DeleteMessageBatchInput{
		QueueUrl: &w.config.QueueURL,
		Entries:  entries,
	})
	if err != nil {
		log.Println("delete message error", err)
		return msgs
	}

	log.Printf("delete %d success, %d failed", len(res.Successful), len(res.Failed))
	return msgs

}

func (w *ConsumerWorker) Start() error {
	w.source.
		Via(w.convertFlow).
		Via(w.doFlow).
		Via(w.batchFlow).
		Via(w.deleteFlow).
		To(ext.NewIgnoreSink())
	return nil
}

func (w *ConsumerWorker) ApplyConfig() {
	w.source.ReloadConfig(&internal.SQSSourceConfig{
		QueueURL:            w.config.QueueURL,
		MaxNumberOfMessages: w.config.BatchSize,
		WaitTimeSeconds:     w.config.WaitTimeSeconds,
		Parallelism:         w.config.MaxWorkers,
	})
	w.convertFlow.SetParallelism(w.config.MaxWorkers)
	w.doFlow.SetParallelism(w.config.MaxWorkers)
	w.batchFlow.SetConfig(w.config.BatchSize, w.batchInterval)
	w.deleteFlow.SetParallelism(w.config.MaxWorkers)
}

func (w *ConsumerWorker) LoadConfig(configFile string) error {
	file, err := os.Open(configFile)
	if err != nil {
		return err
	}
	defer file.Close()

	cf, err := config.Parse(file)
	if err != nil {
		return err
	}
	w.config = cf
	return nil
}

// consumerCmd represents the consumer command
var consumerCmd = &cobra.Command{
	Use:  "consumer",
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		configFile := args[0]

		ctx, cancel := context.WithCancel(cmd.Context())
		defer cancel()

		worker, err := NewConsumerWorker(ctx, configFile)
		if err != nil {
			fmt.Println(err)
			return
		}

		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt, syscall.SIGHUP, syscall.SIGTERM)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case sig := <-signals:
					if sig == syscall.SIGHUP {
						if err := worker.LoadConfig(configFile); err != nil {
							log.Println("load config error", err)
							return
						}
						worker.ApplyConfig()
						log.Println("reloaded config")
					} else if sig == os.Interrupt || sig == syscall.SIGTERM {
						log.Println("shutting down")
						cancel()
					}
				}
			}
		}()

		if err := worker.Start(); err != nil {
			fmt.Println(err)
			return
		}
	},
}

func init() {
	rootCmd.AddCommand(consumerCmd)
}
