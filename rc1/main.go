package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	awss "github.com/imishinist/go-streams/aws"
	ext "github.com/imishinist/go-streams/extension"
	"github.com/imishinist/go-streams/flow"

	"github.com/imishinist/feeder-poc/config"
	"github.com/imishinist/feeder-poc/internal"
	"github.com/imishinist/feeder-poc/logger"
)

func NewSQSClient(ctx context.Context) (*sqs.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion("ap-northeast-1"))
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

type Message = awss.QueueMessage[internal.Message]

type ConsumerWorker struct {
	logger *logger.Logger
	config *config.ConsumerWorker

	client *sqs.Client

	source     *awss.SQSSource[internal.Message]
	feedFlow   *flow.Map[Message, Message]
	batchFlow  *flow.Batch[Message]
	deleteFlow *flow.Map[[]Message, []Message]

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

	worker.logger, err = logger.NewLogger(worker.config.StdoutPath, worker.config.StderrPath)
	if err != nil {
		return nil, err
	}

	sqsConfig := &awss.SQSSourceConfig[internal.Message]{
		QueueURL:            worker.config.QueueURL,
		MaxNumberOfMessages: worker.config.BatchSize,
		WaitTimeSeconds:     worker.config.WaitTimeSeconds,
		Parallelism:         worker.config.MaxWorkers,
		BodyHandler:         worker.Convert,
	}
	worker.source = awss.NewSQSSource(ctx, worker.client, sqsConfig)
	worker.feedFlow = flow.NewMap("feed", worker.Feed, worker.config.MaxWorkers)
	worker.batchFlow = flow.NewBatch[Message]("batch", uint(worker.config.BatchSize), worker.batchInterval)
	worker.deleteFlow = flow.NewMap("delete", worker.DeleteMessage, worker.config.MaxWorkers)

	return worker, nil
}

// Convert は、SQSから取得したメッセージをパースする
// error を返すと、ReceiptHandle が nil になる
func (w *ConsumerWorker) Convert(body *string) (*internal.Message, error) {
	/*
		var data map[string]string
		if err := json.Unmarshal([]byte(*body), &data); err != nil {
			return nil, nil
		}
		message, err := internal.ParseMessage(data["Message"])
	*/
	message, err := internal.ParseMessage(*body)
	if err != nil {
		w.logger.Errorf("parse message error: %v", err)
		// パースに失敗したエラーは無視して処理を続ける
		return nil, nil
	}
	return message, nil
}

func (w *ConsumerWorker) Feed(msg Message) (ret Message) {
	ret = msg

	body := msg.Body
	if body == nil {
		return
	}

	time.Sleep(w.config.PerMessageInterval)

	scriptPath := w.config.ScriptPath
	collection := w.config.Collection
	memberID := body.MemberID
	enqueueAt := fmt.Sprintf("%d", body.Metadata.EnqueueAt.UnixNano())
	stdoutPath := w.config.StdoutPath
	stderrPath := w.config.StderrPath
	cmd := exec.Command("/bin/sh", "-c", fmt.Sprintf("%s %s %s %s >>%s 2>>%s", scriptPath, collection, memberID, enqueueAt, stdoutPath, stderrPath))
	if err := cmd.Run(); err != nil {
		w.logger.Errorf("command exec error: %v", err)
		return
	}
	return
}

func (w *ConsumerWorker) DeleteMessage(msgs []Message) []Message {
	entries := make([]types.DeleteMessageBatchRequestEntry, 0, len(msgs))
	for i, msg := range msgs {
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
		w.logger.Errorf("delete message error: %v", err)
		return msgs
	}

	w.logger.Printf("delete %d success, %d failed", len(res.Successful), len(res.Failed))
	return msgs
}

func (w *ConsumerWorker) Start() error {
	w.source.
		Via(w.feedFlow).
		Via(w.batchFlow).
		Via(w.deleteFlow).
		To(ext.NewIgnoreSink())

	return nil
}

func (w *ConsumerWorker) ApplyConfig() {
	w.source.ReloadConfig(&awss.SQSSourceConfig[internal.Message]{
		QueueURL:            w.config.QueueURL,
		MaxNumberOfMessages: w.config.BatchSize,
		WaitTimeSeconds:     w.config.WaitTimeSeconds,
		Parallelism:         w.config.MaxWorkers,
		BodyHandler:         w.Convert,
	})
	w.feedFlow.SetParallelism(w.config.MaxWorkers)
	w.batchFlow.SetConfig(uint(w.config.BatchSize), w.batchInterval)
	w.deleteFlow.SetParallelism(w.config.MaxWorkers)

	if err := w.logger.ReOpen(w.config.StdoutPath, w.config.StderrPath); err != nil {
		log.Println(err)
	}
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

func (w *ConsumerWorker) Close() error {
	return w.logger.Close()
}

func main() {
	var configFile string
	flag.StringVar(&configFile, "config", "config.yaml", "config file")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker, err := NewConsumerWorker(ctx, configFile)
	if err != nil {
		log.Fatal(err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGHUP, syscall.SIGTERM)
	go func() {
		for {
			select {
			case <-ctx.Done():
				if err := worker.Close(); err != nil {
					log.Println(err)
				}
				return
			case sig := <-signals:
				switch {
				case sig == syscall.SIGHUP:
					if err := worker.LoadConfig(configFile); err != nil {
						log.Println("load config error", err)
						return
					}
					worker.ApplyConfig()
					log.Println("reloaded config")
				case sig == syscall.SIGTERM || sig == os.Interrupt:
					log.Println("shutting down")
					cancel()
				}
			}
		}
	}()

	if err := worker.Start(); err != nil {
		log.Println(err)
		return
	}
}
