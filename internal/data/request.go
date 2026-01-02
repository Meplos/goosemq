package data

import (
	"time"

	"github.com/google/uuid"
)

type PushRequest struct {
	Topic string  `json:"topic"`
	Body  Content `json:"body"`
}

type AckRequest struct {
	Topic string    `json:"topic"`
	ID    uuid.UUID `json:"id"`
}

type PullRequest struct {
	Topic      string        `json:"topic"`
	MaxMessage int           `json:"max_message"`
	Timeout    time.Duration `json:"timeout"`
}
