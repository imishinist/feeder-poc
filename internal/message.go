package internal

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
)

type Message struct {
	MemberID string `json:"member_id"`
	Force    bool   `json:"force"`

	EnqueueAt string `json:"enqueue_at"`
}

var (
	globalPool = sync.Pool{
		New: func() interface{} {
			return &bytes.Buffer{}
		},
	}
)

func (m *Message) Encode() (string, error) {
	buf := globalPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		globalPool.Put(buf)
	}()

	if err := json.NewEncoder(buf).Encode(m); err != nil {
		return "", err
	}
	return strings.TrimSuffix(buf.String(), "\n"), nil
}
