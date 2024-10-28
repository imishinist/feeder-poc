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

	ScriptPath string `yaml:"script_path"`
	StdoutPath string `yaml:"stdout_path"`
	StderrPath string `yaml:"stderr_path"`
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
	if conf.ScriptPath == "" {
		return nil, errors.Join(ErrInvalidConfig, errors.New("script_path is required"))
	}
	if conf.StdoutPath == "" {
		return nil, errors.Join(ErrInvalidConfig, errors.New("stdout_path is required"))
	}
	if conf.StderrPath == "" {
		return nil, errors.Join(ErrInvalidConfig, errors.New("stderr_path is required"))
	}
	return &conf, nil
}
