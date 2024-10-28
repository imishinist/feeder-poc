package config

import (
	"errors"
	"io"
	"time"

	"github.com/creasty/defaults"
	"gopkg.in/yaml.v3"
)

var (
	ErrInvalidConfig = errors.New("invalid config")
)

type ConsumerWorker struct {
	Collection string `yaml:"collection"`

	QueueURL        string `yaml:"queue_url"`
	BatchSize       int    `yaml:"batch_size" default:"10"`
	WaitTimeSeconds int    `yaml:"wait_time_seconds" default:"20"`

	MaxWorkers         uint          `yaml:"max_workers" default:"20"`
	PerMessageInterval time.Duration `yaml:"per_message_interval" default:"10ms"`
}

func Parse(in io.Reader) (*ConsumerWorker, error) {
	var conf ConsumerWorker

	defaults.Set(&conf)
	if err := yaml.NewDecoder(in).Decode(&conf); err != nil {
		return nil, errors.Join(ErrInvalidConfig, err)
	}
	if conf.Collection == "" {
		return nil, errors.Join(ErrInvalidConfig, errors.New("collection is required"))
	}
	if conf.QueueURL == "" {
		return nil, errors.Join(ErrInvalidConfig, errors.New("queue_url is required"))
	}
	return &conf, nil
}
