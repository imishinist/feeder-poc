package logger

import (
	"fmt"
	"os"
	"sync"
)

type file struct {
	path  string
	inner *os.File

	mu *sync.RWMutex
}

func newFile(path string) (*file, error) {
	f := &file{
		path: path,
		mu:   new(sync.RWMutex),
	}
	if err := f.Reload(); err != nil {
		return nil, err
	}
	return f, nil
}

func (f *file) Reload() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.reload()
}

func (f *file) ReOpen(path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.path = path
	return f.reload()
}

func (f *file) reload() error {
	// TODO: 失敗した時は上書きしないようにしたい

	if f.inner != nil {
		if err := f.inner.Close(); err != nil {
			return err
		}
		f.inner = nil
	}

	inner, err := os.OpenFile(f.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w ", f.path, err)
	}
	f.inner = inner
	return nil
}

func (f *file) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.inner != nil {
		if err := f.inner.Close(); err != nil {
			return err
		}
		f.inner = nil
	}
	return nil
}

func (f *file) Write(p []byte) (n int, err error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.inner == nil {
		return 0, fmt.Errorf("file is not open")
	}
	return f.inner.Write(p)
}
