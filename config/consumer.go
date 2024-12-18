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
	Collection   string `yaml:"collection"`
	StopFilePath string `yaml:"stop_file_path"`

	QueueURL        string `yaml:"queue_url"`
	BatchSize       int    `yaml:"batch_size" default:"10"`
	WaitTimeSeconds int    `yaml:"wait_time_seconds" default:"20"`

	MaxSourceWorkers   uint          `yaml:"max_source_workers" default:"10"`
	MaxFeedWorkers     uint          `yaml:"max_feed_workers" default:"20"`
	MaxDeleteWorkers   uint          `yaml:"max_delete_workers" default:"10"`
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
	if conf.StopFilePath == "" {
		return nil, errors.Join(ErrInvalidConfig, errors.New("stop_file_path is required"))
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
