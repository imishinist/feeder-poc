package internal

import (
	"encoding/json"
	"strings"
)

type Message struct {
	MemberID string `json:"member_id"`
	Force    bool   `json:"force"`

	EnqueueAt string `json:"enqueue_at"`
}

func (m *Message) Encode() (string, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(string(data), "\n"), nil
}
