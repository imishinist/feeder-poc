package internal

import (
	"encoding/json"
	"strings"
	"time"
)

type MessageMetadata struct {
	Source    string     `json:"source"`
	EnqueueAt time.Time  `json:"enqueue_at"`
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
}

type Message struct {
	MemberID    string `json:"member_id"`
	ProcessType string `json:"process_type"`

	Metadata MessageMetadata `json:"metadata"`
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
