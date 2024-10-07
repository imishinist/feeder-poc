package internal

import (
	"encoding/json"
	"strings"
	"time"
)

type Message struct {
	Collection string `json:"collection"`
	MemberID   string `json:"member_id"`
	Force      bool   `json:"force"`

	EnqueueAt time.Time `json:"enqueue_at"`
}

func (m *Message) Encode() (string, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(string(data), "\n"), nil
}

func ParseMessage(data string) (*Message, error) {
	var m Message
	if err := json.Unmarshal([]byte(data), &m); err != nil {
		return nil, err
	}
	return &m, nil
}
