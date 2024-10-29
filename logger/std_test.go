package logger

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStdLogger(t *testing.T) {
	dummy := time.Date(2021, 1, 2, 3, 4, 5, 0, time.UTC)

	logger := NewStdLogger()
	Now = func() time.Time {
		return dummy
	}

	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)

	logger.stdout = stdout
	logger.stderr = stderr

	logger.Printf("test")
	logger.Errorf("error")

	assert.Equal(t, "[2021-01-02 03:04:05] test\n", stdout.String())
	assert.Equal(t, "[2021-01-02 03:04:05] error\n", stderr.String())
}
