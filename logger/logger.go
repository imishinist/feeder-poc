package logger

import (
	"fmt"
	"io"
	"strings"
	"sync"
)

type FileLogger struct {
	stdout *file
	stderr *file

	mu *sync.RWMutex
}

var _ Logger = (*FileLogger)(nil)

func NewFileLogger(stdoutPath, stderrPath string) (*FileLogger, error) {
	stdout, err := newFile(stdoutPath)
	if err != nil {
		return nil, err
	}
	stderr, err := newFile(stderrPath)
	if err != nil {
		return nil, err
	}

	return &FileLogger{
		stdout: stdout,
		stderr: stderr,
		mu:     new(sync.RWMutex),
	}, nil
}

func (l *FileLogger) ReOpen(stdoutPath, stderrPath string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.stdout.ReOpen(stdoutPath); err != nil {
		return err
	}
	if err := l.stderr.ReOpen(stderrPath); err != nil {
		return err
	}
	return nil
}

func (l *FileLogger) Reload() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.stdout.Reload(); err != nil {
		return err
	}

	if err := l.stderr.Reload(); err != nil {
		return err
	}
	return nil
}

func (l *FileLogger) Close() (err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	defer func() {
		err = l.stdout.Close()
	}()
	defer func() {
		err = l.stderr.Close()
	}()

	return nil
}

func (l *FileLogger) Printf(format string, v ...interface{}) (n int, err error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.stdout == nil {
		return 0, nil
	}
	return l.printf(l.stdout, format, v...)
}

func (l *FileLogger) Errorf(format string, v ...interface{}) (n int, err error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.stderr == nil {
		return 0, nil
	}
	return l.printf(l.stderr, format, v...)
}

func (l *FileLogger) printf(w io.Writer, format string, v ...interface{}) (n int, err error) {
	if !strings.HasSuffix(format, "\n") {
		format += "\n"
	}

	now := Now().Format("2006-01-02 15:04:05")
	format = "[" + now + "] " + format
	return fmt.Fprintf(w, format, v...)
}
