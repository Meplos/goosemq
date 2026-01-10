package data

import (
	"time"

	"github.com/Meplos/goosemq/pkg/data"
	"github.com/google/uuid"
)

type TopicState struct {
	Name         string    `json:"name"`
	Pending      int       `json:"pending"`
	Inflight     int       `json:"inflight"`
	IsIdle       bool      `json:"is_idle"`
	TotalBytes   int64     `json:"bytes"`
	TotalMessage int       `json:"messages"`
	LastActivity time.Time `json:"last_activity"`
}

type BrockerState struct {
	Topics  []TopicState `json:"topics"`
	StartAt time.Time    `json:"start_at"`
}

type TopicMessage struct {
	ID            uuid.UUID
	Body          data.Content
	ContentLength int64
	CreatedAt     time.Time
	InflightAt    time.Time
	Retry         int
	Topic         string
}

func (m *TopicMessage) Content() data.Content {
	return m.Body
}
