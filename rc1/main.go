package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"sync/atomic"
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

var ErrStop = errors.New("STOPファイルが設置されています")

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
	stopping atomic.Bool

	logger logger.Logger

	config *config.ConsumerWorker

	client *sqs.Client

	source     *awss.SQSSource[internal.Message]
	feedFlow   *flow.Map[Message, Message]
	batchFlow  *flow.Batch[Message]
	deleteFlow *flow.Map[[]Message, []Message]

	batchInterval time.Duration

	uuid string
}

func NewConsumerWorker(ctx context.Context, configFile string) (*ConsumerWorker, error) {
	worker := &ConsumerWorker{}
	if err := worker.LoadConfig(configFile); err != nil {
		return nil, err
	}
	worker.stopping.Store(false)
	worker.batchInterval = time.Second * 10

	ctx, cancel := context.WithCancelCause(ctx)

	client, err := NewSQSClient(ctx)
	if err != nil {
		return nil, err
	}
	worker.client = client

	worker.logger = logger.NewStdLogger()
	sqsConfig := &awss.SQSSourceConfig[internal.Message]{
		QueueURL:            worker.config.QueueURL,
		MaxNumberOfMessages: worker.config.BatchSize,
		WaitTimeSeconds:     worker.config.WaitTimeSeconds,
		Parallelism:         worker.config.MaxSourceWorkers,
		BodyHandler:         worker.Convert,
	}
	worker.source = awss.NewSQSSource(ctx, worker.client, sqsConfig)
	worker.feedFlow = flow.NewMap("feed", worker.Feed, worker.config.MaxFeedWorkers)
	worker.batchFlow = flow.NewBatch[Message]("batch", uint(worker.config.BatchSize), worker.batchInterval)
	worker.deleteFlow = flow.NewMap("delete", worker.DeleteMessage, worker.config.MaxDeleteWorkers)
	worker.uuid = internal.NewUUID()

	go worker.handleStop(cancel)

	return worker, nil
}

func (w *ConsumerWorker) handleStop(cancel context.CancelCauseFunc) {
	ticker := time.NewTicker(time.Second * 2)
	for range ticker.C {
		if w.stopping.Load() {
			cancel(ErrStop)
			return
		}
	}
}

func (w *ConsumerWorker) shouldStop() bool {
	if w.stopping.Load() {
		return true
	}

	_, err := os.Stat(w.config.StopFilePath)
	if err == nil {
		w.stopping.Store(true)
		w.logger.Printf("STOPファイルが置かれているため、処理を中断します。")
		return true
	}
	return false
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
		w.logger.Printf("[%s] invalid message %v: %s", w.uuid, err, *body)
		// パースに失敗したエラーは無視して処理を続ける
		return nil, nil
	}
	return message, nil
}

func (w *ConsumerWorker) duration(start *time.Time, finish *time.Time) string {
	if start == nil || finish == nil {
		return "-"
	}
	return fmt.Sprintf("%d", finish.Sub(*start).Milliseconds())
}

func (w *ConsumerWorker) Feed(msg Message) (ret Message) {
	ret = msg
	if w.shouldStop() {
		// stopファイルが存在する場合はメッセージを削除せずに処理を終了する
		ret.ReceiptHandle = nil
		return
	}

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

	start := time.Now()
	w.logger.Printf("[%s] starting consumer [%s] [%s] [%s]", w.uuid, w.config.Collection, *msg.MessageID, memberID)
	cmd := exec.Command("/bin/sh", "-c", fmt.Sprintf("%s %s %s %s >>%s 2>>%s", scriptPath, collection, memberID, enqueueAt, stdoutPath, stderrPath))
	if err := cmd.Run(); err != nil {
		w.logger.Printf("[%s] failed consumer [%s] [%s] [%s]", w.uuid, w.config.Collection, *msg.MessageID, memberID)
		return
	}
	finish := time.Now()

	waitUntilEnqueue := w.duration(body.Metadata.UpdatedAt.Time, &body.Metadata.EnqueueAt)
	totalFeedTime := w.duration(body.Metadata.UpdatedAt.Time, &finish)
	waitUntilStart := w.duration(&body.Metadata.EnqueueAt, &start)
	processTime := w.duration(&start, &finish)
	waitUntilFinish := w.duration(&body.Metadata.EnqueueAt, &finish)
	waitAfterReceive := w.duration(&msg.ReceiveTime, &finish)
	w.logger.Printf("[%s] finished consumer [%s] [%s] [%s] %s %s %s %s %s %s", w.uuid, w.config.Collection, *msg.MessageID, memberID, waitUntilEnqueue, waitUntilStart, processTime, waitUntilFinish, waitAfterReceive, totalFeedTime)

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
		w.logger.Errorf("[%s] delete message error: %v", w.uuid, err)
		return msgs
	}

	w.logger.Printf("[%s] delete %d success, %d failed", w.uuid, len(res.Successful), len(res.Failed))
	return msgs
}

func (w *ConsumerWorker) Start() error {
	if w.shouldStop() {
		return nil
	}

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
		Parallelism:         w.config.MaxSourceWorkers,
		BodyHandler:         w.Convert,
	})
	w.feedFlow.SetParallelism(w.config.MaxFeedWorkers)
	w.batchFlow.SetConfig(uint(w.config.BatchSize), w.batchInterval)
	w.deleteFlow.SetParallelism(w.config.MaxDeleteWorkers)
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
