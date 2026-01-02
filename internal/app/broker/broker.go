package broker

import (
	"log"
	"maps"
	"sync"
	"time"

	"github.com/Meplos/goosemq/internal/data"
	"github.com/Meplos/goosemq/internal/goosemq"
)

type Broker interface {
	Start()
	Close()
	Pull(req data.PullRequest) data.PullResponse
	Push(req data.PushRequest) data.AckResponse
	Ack(req data.AckRequest)
	Snapshot() data.BrockerState
}
type BrokerState string

const (
	CLOSED  BrokerState = "CLOSED"
	STARTED BrokerState = "STARTED"
)

type BrokerRuntime struct {
	state    BrokerState
	Topics   map[string]goosemq.Topic
	mu       sync.RWMutex
	StartAt  time.Time
	TopicTTL time.Duration
	stopChan chan struct{}
}

func (b *BrokerRuntime) Snapshot() data.BrockerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	state := data.BrockerState{
		Topics:  make([]data.TopicState, 0),
		StartAt: b.StartAt,
	}

	for _, t := range b.Topics {
		state.Topics = append(state.Topics, t.Snapshot())
	}

	return state
}

func (b *BrokerRuntime) Ack(req data.AckRequest) {
	log.Printf("[Broker] ack T:%s M:%s", req.Topic, req.ID)
	t := b.getOrCreateTopic(req.Topic)
	t.Ack(req.ID)
}

func (b *BrokerRuntime) getOrCreateTopic(topic string) goosemq.Topic {
	b.mu.RLock()
	t, ok := b.Topics[topic]
	b.mu.RUnlock()

	if ok {
		return t
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if t, ok = b.Topics[topic]; ok {
		return t
	}

	t = goosemq.New(topic)
	b.Topics[topic] = t
	return t
}

func (b *BrokerRuntime) Pull(req data.PullRequest) data.PullResponse {
	log.Printf("[Broker] pull T:%s Wait:%vs MM:%v", req.Topic, req.Timeout.Seconds(), req.MaxMessage)
	t := b.getOrCreateTopic(req.Topic)
	items := b.longPolling(t, req.Timeout, req.MaxMessage)

	return data.PullResponse{
		Items: items,
	}

}

func (b *BrokerRuntime) longPolling(t goosemq.Topic, wait time.Duration, maxMessage int) []data.TopicMessage {

	timer := time.NewTimer(wait)
	defer timer.Stop()

	pulled := make([]data.TopicMessage, 0)
	for {
		select {
		case <-timer.C:
			return pulled
		case <-t.Notify():
			pulled = t.Pull(maxMessage)
			return pulled
		}
	}
}

func (b *BrokerRuntime) Push(req data.PushRequest) data.AckResponse {
	log.Printf("[Broker] push T:%s C:%s", req.Topic, req.Body)
	t := b.getOrCreateTopic(req.Topic)
	err := t.Enqueue(req.Body)
	if err != nil {
		return data.AckResponse{
			Status: data.AckKo,
			Error:  err.Error(),
		}
	}
	return data.AckResponse{
		Status: data.AckOk,
	}
}

func (b *BrokerRuntime) cleanLoop() {
	timer := time.NewTicker(30 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-b.stopChan:
			return
		case <-timer.C:
		}
		b.mu.RLock()
		cpy := make(map[string]goosemq.Topic, len(b.Topics))
		maps.Copy(cpy, b.Topics)
		b.mu.RUnlock()

		for _, t := range cpy {
			t.RetryAndCleanUnacked()
			if t.IsIdle() && time.Since(t.LastActivity()) > b.TopicTTL {
				b.mu.Lock()
				if cur, ok := b.Topics[t.Name()]; ok && cur == t {
					delete(b.Topics, t.Name())
				}
				b.mu.Unlock()
			}
		}

	}

}

func (b *BrokerRuntime) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == CLOSED {
		return
	}
	close(b.stopChan)
	b.state = CLOSED

}
func (b *BrokerRuntime) Start() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == STARTED {
		return
	}
	b.state = STARTED
	go b.cleanLoop()
}

func New() Broker {
	return &BrokerRuntime{
		Topics:   make(map[string]goosemq.Topic),
		mu:       sync.RWMutex{},
		StartAt:  time.Now(),
		TopicTTL: 1 * time.Hour,
		stopChan: make(chan struct{}),
	}
}
