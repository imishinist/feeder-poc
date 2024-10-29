package internal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Ptr[T any](v T) *T {
	return &v
}

func TestMessage_Encode(t *testing.T) {
	t.Run("encode and decode", func(t *testing.T) {
		tests := []struct {
			name    string
			message Message
			want    string
			wantErr bool
		}{
			{
				name: "updated_at is nil",
				message: Message{
					MemberID:    "member_id",
					ProcessType: "U",
					Metadata: MessageMetadata{
						Source:    "scavenger",
						UpdatedAt: MaybeTime{},
						EnqueueAt: time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC),
					},
				},
				want:    "{\"member_id\":\"member_id\",\"process_type\":\"U\",\"metadata\":{\"source\":\"scavenger\",\"enqueue_at\":\"2021-01-01T00:00:00Z\",\"updated_at\":null}}",
				wantErr: false,
			},
			{
				name: "updated_at has set",
				message: Message{
					MemberID:    "member_id",
					ProcessType: "U",
					Metadata: MessageMetadata{
						Source:    "scavenger",
						UpdatedAt: MaybeTime{Ptr(time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC))},
						EnqueueAt: time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC),
					},
				},
				want:    "{\"member_id\":\"member_id\",\"process_type\":\"U\",\"metadata\":{\"source\":\"scavenger\",\"enqueue_at\":\"2021-01-01T00:00:00Z\",\"updated_at\":\"2021-01-01T00:00:00Z\"}}",
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
	})

	t.Run("decode", func(t *testing.T) {
		tests := []struct {
			name    string
			data    string
			want    Message
			wantErr bool
		}{
			{
				name: "updated_at has empty string",
				data: "{\"member_id\":\"member_id\",\"process_type\":\"U\",\"metadata\":{\"source\":\"scavenger\",\"enqueue_at\":\"2021-01-01T00:00:00Z\",\"updated_at\":\"\"}}",
				want: Message{
					MemberID:    "member_id",
					ProcessType: "U",
					Metadata: MessageMetadata{
						Source:    "scavenger",
						UpdatedAt: MaybeTime{},
						EnqueueAt: time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC),
					},
				},
				wantErr: false,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := ParseMessage(tt.data)
				if (err != nil) != tt.wantErr {
					t.Errorf("ParseMessage() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				assert.Equal(t, tt.want, *got)
			})
		}
	})
}
