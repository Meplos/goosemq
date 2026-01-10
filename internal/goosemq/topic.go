package goosemq

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/Meplos/goosemq/internal/config"
	internal "github.com/Meplos/goosemq/internal/data"
	"github.com/Meplos/goosemq/pkg/data"
	"github.com/google/uuid"
)

type Topic interface {
	Enqueue(body data.Content) error
	Pull(max int) []internal.TopicMessage
	Ack(msgID uuid.UUID)
	RetryAndCleanUnacked()
	Notify() <-chan struct{}
	Snapshot() internal.TopicState
	IsIdle() bool
	Name() string
	LastActivity() time.Time
}

type InMemoryTopic struct {
	name     string
	Pending  []internal.TopicMessage
	Inflight map[uuid.UUID]internal.TopicMessage
	mu       sync.RWMutex

	totalBytes    int64
	totalMessages int

	config.TopicConfig
	notifyChan   chan struct{}
	lastActivity time.Time
}

func (t *InMemoryTopic) LastActivity() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lastActivity
}

func (t *InMemoryTopic) Name() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.name
}

func New(name string) Topic {
	return &InMemoryTopic{
		name:     name,
		Pending:  make([]internal.TopicMessage, 0),
		Inflight: make(map[uuid.UUID]internal.TopicMessage, 0),
		mu:       sync.RWMutex{},
		TopicConfig: config.TopicConfig{
			MaxMessage: 10000,
			MaxBytes:   100 * 1024 * 1024, //100KB
			TTL:        1 * time.Hour,
			TimeToAck:  30 * time.Minute,
		},
		totalBytes:   0,
		notifyChan:   make(chan struct{}, 1),
		lastActivity: time.Now(),
	}
}

func (t *InMemoryTopic) Enqueue(body data.Content) error {
	bytes, err := json.Marshal(body)
	if err != nil {
		return err
	}
	msgSize := int64(len(bytes))
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.totalMessages+1 > t.MaxMessage || t.totalBytes+msgSize > t.MaxBytes {
		return ErrTopicOverloaded
	}

	t.Pending = append(t.Pending, internal.TopicMessage{
		ID:            uuid.New(),
		CreatedAt:     time.Now(),
		Body:          body,
		Retry:         0,
		ContentLength: msgSize,
		Topic:         t.name,
	})
	t.totalBytes += msgSize
	t.totalMessages++
	t.notifyAll()
	t.lastActivity = time.Now()
	return nil
}

func (t *InMemoryTopic) Pull(max int) []internal.TopicMessage {
	if max <= 0 {
		return []internal.TopicMessage{}

	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastActivity = time.Now()
	if max >= len(t.Pending) {
		max = len(t.Pending)
	}

	pulled := make([]internal.TopicMessage, max)
	copy(pulled, t.Pending[0:max])
	t.Pending = t.Pending[max:len(t.Pending)]

	for _, p := range pulled {
		p.InflightAt = time.Now()
		t.Inflight[p.ID] = p
	}

	return pulled
}

func (t *InMemoryTopic) Ack(msgID uuid.UUID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastActivity = time.Now()
	log.Printf("ACK. T[%s] msg:%s", t.name, msgID)
	msg, ok := t.Inflight[msgID]
	if !ok {
		return
	}
	delete(t.Inflight, msgID)

	if t.totalMessages > 0 {
		t.totalMessages--
	}
	if t.totalBytes > 0 {
		t.totalBytes -= msg.ContentLength
	}
}

func (t *InMemoryTopic) RetryAndCleanUnacked() {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()

	for key, m := range t.Inflight {
		if now.After(m.CreatedAt.Add(t.TTL)) {
			delete(t.Inflight, key)
			if t.totalMessages > 0 {
				t.totalMessages--
			}
			if t.totalBytes > 0 {
				t.totalBytes -= m.ContentLength
			}
			continue
		}
		if now.Before(m.InflightAt.Add(t.TimeToAck)) {
			continue
		}
		m.Retry++
		t.Pending = append(t.Pending, m)
		delete(t.Inflight, key)
		t.notifyAll()
		t.lastActivity = time.Now()
	}
}

func (t *InMemoryTopic) isIdle() bool {
	return len(t.Pending) <= 0 && len(t.Inflight) <= 0
}

func (t *InMemoryTopic) IsIdle() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.Pending) <= 0 && len(t.Inflight) <= 0
}
func (t *InMemoryTopic) Snapshot() internal.TopicState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return internal.TopicState{
		Name:         t.name,
		Inflight:     len(t.Inflight),
		Pending:      len(t.Pending),
		IsIdle:       t.isIdle(),
		TotalBytes:   t.totalBytes,
		TotalMessage: t.totalMessages,
		LastActivity: t.lastActivity,
	}
}

func (t *InMemoryTopic) Notify() <-chan struct{} {
	return t.notifyChan
}

func (t *InMemoryTopic) notifyAll() {
	select {
	case t.notifyChan <- struct{}{}:
	default:
	}

}
