package internal

import "testing"

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
				MemberID:  "member_id",
				Force:     false,
				EnqueueAt: "2021-01-01T00:00:00Z",
			},
			want:    "{\"member_id\":\"member_id\",\"force\":false,\"enqueue_at\":\"2021-01-01T00:00:00Z\"}",
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
			if got != tt.want {
				t.Errorf("Encode() got = %v, want %v", got, tt.want)
			}
		})
	}
}
