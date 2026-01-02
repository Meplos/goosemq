package producer

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/Meplos/goosemq/internal/client"
	"github.com/Meplos/goosemq/internal/config"
	"github.com/Meplos/goosemq/internal/data"
	"github.com/Meplos/goosemq/internal/goosemq"
	"github.com/Meplos/goosemq/internal/protocol"
)

type Producer interface {
	Start()
	Stop()
	Publish(topic string, body data.Content) error
}

type ProducerState string

const (
	STOPPED ProducerState = "STOPPED"
	STARTED ProducerState = "STARTED"
)

type ProducerRuntime struct {
	state       ProducerState
	ConnManager client.ConnManager
	Requests    map[protocol.ReqId]PublishRequest
	mu          sync.Mutex
	stopChan    chan struct{}
}

type PublishRequest struct {
	Topic    string
	Body     data.Content
	ReqId    protocol.ReqId
	Retry    int
	Retrying bool
}

func New(info config.ConnexionInfo) Producer {

	p := &ProducerRuntime{
		state:       STOPPED,
		ConnManager: client.New(info),
		Requests:    make(map[protocol.ReqId]PublishRequest),
		mu:          sync.Mutex{},
		stopChan:    make(chan struct{}),
	}

	return p
}

func (p *ProducerRuntime) Start() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state == STARTED {
		return
	}

	p.state = STARTED
	p.ConnManager.Start()
	go p.ackLoop()
}

func (p *ProducerRuntime) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state == STOPPED {
		return
	}

	p.state = STOPPED
	p.ConnManager.Stop()
	select {
	case <-p.stopChan:
	default:
		close(p.stopChan)
	}
}

func (p *ProducerRuntime) Publish(topic string, body data.Content) error {
	p.mu.Lock()
	reqId := protocol.NewId()

	req := PublishRequest{Topic: topic, Body: body, ReqId: reqId, Retry: 0, Retrying: false}
	p.Requests[reqId] = req
	p.mu.Unlock()

	err := p.send(req)

	if err != nil {
		return err
	}

	return nil
}

func (p *ProducerRuntime) ackLoop() {
	for {
		select {
		case respChan, ok := <-p.ConnManager.AckChan():
			if !ok {
				return
			}
			p.handleAck(respChan)
		case <-p.stopChan:
			return
		}
	}
}

func (p *ProducerRuntime) handleAck(frame protocol.Frame) {
	if frame.Type != protocol.ACK {
		return
	}

	var payload data.AckResponse
	err := json.Unmarshal(frame.Payload, &payload)
	if err != nil {
		log.Printf("[HandleAck] cant unmarshall payload. error: %s", err)
		return
	}

	switch payload.Status {
	case data.AckOk:
		p.mu.Lock()
		delete(p.Requests, frame.ID)
		p.mu.Unlock()
	case data.AckKo:
		p.mu.Lock()
		pr, ok := p.Requests[frame.ID]
		if !ok {
			p.mu.Unlock()
			return
		}
		pr.Retrying = false
		p.Requests[frame.ID] = pr
		p.mu.Unlock()
		if payload.Error == goosemq.ErrTopicOverloaded.Error() {
			go p.retry(frame.ID)
		}
	}

}

func (p *ProducerRuntime) retry(reqId protocol.ReqId) {
	time.Sleep(10 * time.Second)
	p.mu.Lock()
	req, ok := p.Requests[reqId]

	if !ok {
		p.mu.Unlock()
		return
	}
	if req.Retrying {
		p.mu.Unlock()
		return
	}
	if req.Retry > 5 {
		delete(p.Requests, reqId)
		p.mu.Unlock()
		return
	}
	req.Retry++
	req.Retrying = true

	p.Requests[reqId] = req
	p.mu.Unlock()
	if err := p.send(req); err != nil {
		log.Printf("error retrying message %s: %s", req.ReqId, err)
	}

}

func (p *ProducerRuntime) send(req PublishRequest) error {
	content := data.PushRequest{
		Topic: req.Topic,
		Body:  req.Body,
	}

	rawData, err := json.Marshal(content)
	if err != nil {
		return err
	}

	msg := protocol.Frame{
		Type:    protocol.PUSH,
		ID:      protocol.ReqId(req.ReqId),
		Payload: rawData,
	}

	_, err = p.ConnManager.Send(msg)
	return err
}
