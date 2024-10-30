package internal

import (
	"github.com/google/uuid"
)

func NewUUID() string {
	u, err := uuid.NewRandom()
	if err != nil {
		return "00000000-0000-0000-0000-000000000000"
	}
	return u.String()
}
