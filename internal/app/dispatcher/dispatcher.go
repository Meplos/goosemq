package dispatcher

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/Meplos/goosemq/internal/app/broker"
	"github.com/Meplos/goosemq/internal/app/network"
	"github.com/Meplos/goosemq/internal/data"
	"github.com/Meplos/goosemq/internal/protocol"
)

type FrameAndSocket struct {
	Frame  protocol.Frame
	Socket network.Socket
}
type Dispatcher interface {
	DispatchChan() chan<- FrameAndSocket
	Start()
	Close()
}

type DispatcherState string

const (
	CLOSED  = "CLOSED"
	STOPPED = "STOPPED"
	STARTED = "STARTED"
)

type DispatcherRuntime struct {
	state        DispatcherState
	dispatchChan chan FrameAndSocket
	closeChan    chan struct{}
	broker       broker.Broker
	mu           sync.RWMutex
}

// Close implements [Dispatcher].
func (d *DispatcherRuntime) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	log.Printf("[Dispatcher] shutdown: started")
	if d.state == CLOSED {
		log.Printf("[Dispatcher] shutdown: already_closed")
		return
	}

	d.state = CLOSED

	close(d.closeChan)
	log.Printf("[Dispatcher] shutdown: success")

}

// Start implements [Dispatcher].
func (d *DispatcherRuntime) Start() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state == STARTED {
		return
	}

	go d.dispatchLoop()
	d.state = STARTED

}

func (d *DispatcherRuntime) DispatchChan() chan<- FrameAndSocket {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.dispatchChan
}

func New(broker broker.Broker) Dispatcher {
	return &DispatcherRuntime{
		dispatchChan: make(chan FrameAndSocket, 10_000),
		closeChan:    make(chan struct{}),
		broker:       broker,
		mu:           sync.RWMutex{},
	}
}

func (d *DispatcherRuntime) dispatchLoop() {
	for {
		select {
		case <-d.closeChan:
			return
		case frameAnSocket, ok := <-d.dispatchChan:
			if !ok {
				return
			}
			frame := frameAnSocket.Frame
			socket := frameAnSocket.Socket

			switch frame.Type {
			case protocol.PULL:
				d.pull(frame, socket)
				continue
			case protocol.ACK:
				d.ack(frame)
				continue
			case protocol.PUSH:
				d.push(frame, socket)
				continue
			}
		}

	}
}

func (d *DispatcherRuntime) pull(frame protocol.Frame, s network.Socket) {
	var data data.PullRequest
	err := json.Unmarshal(frame.Payload, &data)
	if err != nil {
		log.Printf("[Dispatcher] fail to unmarshall request. error: %s", err)
		return
	}
	res := d.broker.Pull(data)

	payload, err := json.Marshal(res)
	if err != nil {
		log.Printf("[Dispatcher] fail to marshall response. error: %s", err)
		return
	}

	s.Send(protocol.Frame{
		Type:    protocol.PULL_RESPONSE,
		ID:      frame.ID,
		Payload: payload,
	})
}

func (d *DispatcherRuntime) push(frame protocol.Frame, s network.Socket) {
	var data data.PushRequest
	err := json.Unmarshal(frame.Payload, &data)
	if err != nil {
		log.Printf("[Dispatcher] fail to unmarshall request. error: %s", err)
		return
	}
	res := d.broker.Push(data)
	payload, err := json.Marshal(res)
	if err != nil {
		log.Printf("[Dispatcher] fail to marshall response. error: %s", err)
		return
	}

	s.Send(protocol.Frame{
		Type:    protocol.ACK,
		ID:      frame.ID,
		Payload: payload,
	})

}

func (d *DispatcherRuntime) ack(frame protocol.Frame) {
	var data data.AckRequest
	err := json.Unmarshal(frame.Payload, &data)
	if err != nil {
		log.Printf("[Dispatcher] fail to unmarshall request. error: %s", err)
		return
	}
	d.broker.Ack(data)

}
