package logger

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

var Now = time.Now

type StdLogger struct {
	stdout io.Writer
	stderr io.Writer
}

var _ Logger = (*StdLogger)(nil)

func NewStdLogger() *StdLogger {
	return &StdLogger{
		stdout: os.Stdout,
		stderr: os.Stderr,
	}
}

func (l *StdLogger) Printf(format string, v ...interface{}) (n int, err error) {
	return l.printf(l.stdout, format, v...)
}

func (l *StdLogger) Errorf(format string, v ...interface{}) (n int, err error) {
	return l.printf(l.stderr, format, v...)
}

func (l *StdLogger) printf(w io.Writer, format string, v ...interface{}) (n int, err error) {
	if !strings.HasSuffix(format, "\n") {
		format += "\n"
	}

	now := Now().Format("2006-01-02 15:04:05")
	format = "[" + now + "] " + format
	return fmt.Fprintf(w, format, v...)
}
