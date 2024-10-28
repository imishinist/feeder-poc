package logger

import (
	"fmt"
	"sync"
)

type Logger struct {
	stdout *file
	stderr *file

	mu *sync.RWMutex
}

func NewLogger(stdoutPath, stderrPath string) (*Logger, error) {
	stdout, err := newFile(stdoutPath)
	if err != nil {
		return nil, err
	}
	stderr, err := newFile(stderrPath)
	if err != nil {
		return nil, err
	}

	return &Logger{
		stdout: stdout,
		stderr: stderr,
		mu:     new(sync.RWMutex),
	}, nil
}

func (l *Logger) ReOpen(stdoutPath, stderrPath string) error {
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

func (l *Logger) Reload() error {
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

func (l *Logger) Close() (err error) {
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

func (l *Logger) Printf(format string, v ...interface{}) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.stdout != nil {
		fmt.Fprintf(l.stdout, format, v...)
	}
}

func (l *Logger) Errorf(format string, v ...interface{}) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.stderr != nil {
		fmt.Fprintf(l.stderr, format, v...)
	}
}
