package tcp

import (
	"log"
	"maps"
	"net"
	"sync"

	"github.com/Meplos/goosemq/internal/app/dispatcher"
	"github.com/Meplos/goosemq/internal/app/network"
	"github.com/Meplos/goosemq/internal/protocol"
)

type TCPGateway struct {
	state        network.GatewayState
	active       map[string]network.Socket
	mu           sync.RWMutex
	ln           net.Listener
	dispatchChan chan<- dispatcher.FrameAndSocket
}

func New(dispatchChan chan<- dispatcher.FrameAndSocket) network.Gateway {
	return &TCPGateway{
		state:        network.STOPPED,
		active:       make(map[string]network.Socket, 0),
		mu:           sync.RWMutex{},
		ln:           nil,
		dispatchChan: dispatchChan,
	}
}

// State implements [network.Gateway].
func (t *TCPGateway) State() network.GatewayState {
	t.mu.RLock()
	defer t.mu.Unlock()
	return t.state
}

func (t *TCPGateway) disconnectSocket(s network.Socket) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s.Close()
	delete(t.active, s.ID())
}

func (t *TCPGateway) OnNewConnection(conn net.Conn) {
	socket := NewSocket(conn)
	t.mu.Lock()
	t.active[socket.ID()] = socket
	t.mu.Unlock()

	socket.StartReaderLoop(func(frame protocol.Frame) {
		t.dispatch(frame, socket)
	}, func(err error) {
		t.disconnectSocket(socket)
	})

	socket.StartWriterLoop(func(err error) {
		t.disconnectSocket(socket)
	})
}

func (t *TCPGateway) dispatch(frame protocol.Frame, s network.Socket) {
	select {
	case t.dispatchChan <- dispatcher.FrameAndSocket{Frame: frame, Socket: s}:
	default:
	}

}

// Close implements [network.Gateway].
func (t *TCPGateway) Close() {
	t.mu.Lock()
	log.Printf("[Gateway] shutdown: started")
	if t.state == network.CLOSED {
		t.mu.Unlock()
		log.Printf("[Gateway] shutdown: already_closed")
		return
	}

	t.state = network.CLOSED
	if t.ln != nil {
		t.ln.Close()
	}
	t.mu.Unlock()
	for _, client := range t.Connections() {
		t.disconnectSocket(client)
	}
	log.Printf("[Gateway] shutdown: sucess")
}

// Connections implements [network.Gateway].
func (t *TCPGateway) Connections() map[string]network.Socket {
	t.mu.RLock()
	defer t.mu.RUnlock()
	cpy := make(map[string]network.Socket)
	maps.Copy(cpy, t.active)
	return cpy
}

// Start implements [network.Gateway].
func (t *TCPGateway) Start(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	t.mu.Lock()
	t.ln = ln
	t.state = network.STARTED
	t.mu.Unlock()

	go func() {
		for {
			t.mu.RLock()
			state := t.state
			ln := t.ln
			t.mu.RUnlock()
			if state != network.STARTED {
				return
			}
			conn, err := ln.Accept()
			if err != nil {
				continue
			}
			t.OnNewConnection(conn)
		}

	}()
	return nil
}
