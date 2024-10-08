package internal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMessage_Encode(t *testing.T) {
	tests := []struct {
		name    string
		message Message
		want    string
		wantErr bool
	}{
		{
			name: "normal",
			message: Message{
				Collection: "collection",
				MemberID:   "member_id",
				Force:      false,
				EnqueueAt:  time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC),
			},
			want:    "{\"collection\":\"collection\",\"member_id\":\"member_id\",\"force\":false,\"enqueue_at\":\"2021-01-01T00:00:00Z\"}",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.message.Encode()
			if (err != nil) != tt.wantErr {
				t.Errorf("Encode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}
