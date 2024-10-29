package config

import (
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func loadFixtures(t *testing.T, name string) io.Reader {
	f, err := os.Open(filepath.Join("fixtures", name))
	if err != nil {
		t.Fatalf("failed to open fixture %q: %v", name, err)
	}
	return f
}

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		fixture string
		want    *ConsumerWorker
		err     error
	}{
		{
			name:    "valid",
			fixture: "consumer_valid.yaml",
			want: &ConsumerWorker{
				Collection:         "COLLECTION",
				QueueURL:           "QUEUE_URL",
				BatchSize:          5,
				WaitTimeSeconds:    10,
				MaxSourceWorkers:   4,
				MaxFeedWorkers:     10,
				MaxDeleteWorkers:   2,
				PerMessageInterval: 100 * time.Millisecond,
				ScriptPath:         "SCRIPT_PATH",
				StdoutPath:         "STDOUT_PATH",
				StderrPath:         "STDERR_PATH",
			},
			err: nil,
		},
		{
			name:    "fill with default values",
			fixture: "consumer_default.yaml",
			want: &ConsumerWorker{
				Collection:         "COLLECTION",
				QueueURL:           "QUEUE_URL",
				BatchSize:          10,
				WaitTimeSeconds:    20,
				MaxSourceWorkers:   10,
				MaxFeedWorkers:     20,
				MaxDeleteWorkers:   10,
				PerMessageInterval: 10 * time.Millisecond,
				ScriptPath:         "SCRIPT_PATH",
				StdoutPath:         "STDOUT_PATH",
				StderrPath:         "STDERR_PATH",
			},
			err: nil,
		},
		{
			name:    "return invalid-error",
			fixture: "consumer_invalid_collection.yaml",
			want:    nil,
			err:     ErrInvalidConfig,
		},
		{
			name:    "return invalid-error",
			fixture: "consumer_invalid_queue_url.yaml",
			want:    nil,
			err:     ErrInvalidConfig,
		},
		{
			name:    "return invalid-error",
			fixture: "consumer_invalid_integer.yaml",
			want:    nil,
			err:     ErrInvalidConfig,
		},
		{
			name:    "return invalid-error",
			fixture: "consumer_invalid_duration.yaml",
			want:    nil,
			err:     ErrInvalidConfig,
		},
		{
			name:    "return invalid-error",
			fixture: "consumer_invalid_script_path.yaml",
			want:    nil,
			err:     ErrInvalidConfig,
		},
		{
			name:    "return invalid-error",
			fixture: "consumer_invalid_stdout_path.yaml",
			want:    nil,
			err:     ErrInvalidConfig,
		},
		{
			name:    "return invalid-error",
			fixture: "consumer_invalid_stderr_path.yaml",
			want:    nil,
			err:     ErrInvalidConfig,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(loadFixtures(t, tt.fixture))

			if tt.err != nil {
				assert.ErrorContains(t, err, tt.err.Error())
			}
			assert.Equal(t, tt.want, got)
		})
	}
}
