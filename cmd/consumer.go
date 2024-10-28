package cmd

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	awss "github.com/imishinist/go-streams/aws"
	ext "github.com/imishinist/go-streams/extension"
	"github.com/imishinist/go-streams/flow"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"

	"github.com/imishinist/feeder-poc/config"
	"github.com/imishinist/feeder-poc/internal"
)

type Message awss.QueueMessage[internal.Message]

type ConsumerWorker struct {
	config *config.ConsumerWorker

	client *sqs.Client

	source      *awss.SQSSource[string]
	convertFlow *flow.Map[awss.QueueMessage[string], Message]
	doFlow      *flow.Map[Message, Message]
	batchFlow   *flow.Batch[Message]
	deleteFlow  *flow.Map[[]Message, []Message]

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

	sqsConfig := &awss.SQSSourceConfig[string]{
		QueueURL:            worker.config.QueueURL,
		MaxNumberOfMessages: worker.config.BatchSize,
		WaitTimeSeconds:     worker.config.WaitTimeSeconds,
		Parallelism:         worker.config.MaxWorkers,
		BodyHandler:         worker.bodyHandler,
	}
	worker.source = awss.NewSQSSource(ctx, worker.client, sqsConfig)
	worker.convertFlow = flow.NewMap("convert", worker.Convert, 1)
	worker.doFlow = flow.NewMap("do", worker.Do, worker.config.MaxWorkers)
	worker.batchFlow = flow.NewBatch[Message]("batch", uint(worker.config.BatchSize), worker.batchInterval)
	worker.deleteFlow = flow.NewMap("delete", worker.DeleteMessage, worker.config.MaxWorkers)

	return worker, nil
}

func (w *ConsumerWorker) Convert(msg awss.QueueMessage[string]) Message {
	message, err := internal.ParseMessage(*msg.Body)
	if err != nil {
		return Message{
			ReceiptHandle: msg.ReceiptHandle,
			Body:          message,
		}
	}
	return Message{
		ReceiptHandle: msg.ReceiptHandle,
		Body:          message,
	}
}

func (w *ConsumerWorker) Do(msg Message) (ret Message) {
	ret = msg

	body := msg.Body
	if msg.Body == nil {
		return
	}

	enqueueAt := fmt.Sprintf("%d", body.EnqueueAt.UnixMilli())
	force := "0"
	if body.Force {
		force = "1"
	}

	cmd := exec.Command("/bin/sh", "-c", fmt.Sprintf("bin/work.sh %s %s %s %s >>logs/work.out 2>>logs/work.err", body.Collection, body.MemberID, force, enqueueAt))
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
		if msg.ReceiptHandle == nil {
			continue
		}

		id := strconv.Itoa(i)
		entries = append(entries, types.DeleteMessageBatchRequestEntry{
			Id:            &id,
			ReceiptHandle: msg.ReceiptHandle,
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

func (w *ConsumerWorker) bodyHandler(s *string) *string {
	return s
}

func (w *ConsumerWorker) ApplyConfig() {
	w.source.ReloadConfig(&awss.SQSSourceConfig[string]{
		QueueURL:            w.config.QueueURL,
		MaxNumberOfMessages: w.config.BatchSize,
		WaitTimeSeconds:     w.config.WaitTimeSeconds,
		Parallelism:         w.config.MaxWorkers,
		BodyHandler:         w.bodyHandler,
	})
	// w.convertFlow.SetParallelism(w.config.MaxWorkers)
	w.convertFlow.SetParallelism(1)
	w.doFlow.SetParallelism(w.config.MaxWorkers)
	w.batchFlow.SetConfig(uint(w.config.BatchSize), w.batchInterval)
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

		go func() {
			http.Handle("/metrics", promhttp.Handler())
			http.ListenAndServe(":2112", nil)
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
