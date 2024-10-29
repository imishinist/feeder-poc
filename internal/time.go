package internal

import (
	"encoding/json"
	"time"
)

type MaybeTime struct {
	*time.Time
}

var _ json.Marshaler = (*MaybeTime)(nil)

func (m *MaybeTime) UnmarshalJSON(data []byte) error {
	x := string(data)
	if x == "null" || x == `""` {
		m.Time = nil
		return nil
	}
	t, err := time.Parse(time.RFC3339, x)
	m.Time = &t
	return err
}

func (m *MaybeTime) MarshalJSON() ([]byte, error) {
	if m.Time == nil {
		return []byte("null"), nil
	}
	return json.Marshal(m.Time)
}

func (m *MaybeTime) String() string {
	if m.Time == nil {
		return "nil"
	}
	return m.Time.String()
}
