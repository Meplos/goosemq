package broker

import (
	"context"
	"log"
	"net"
	"sync"
)

type Broker struct {
	Addr    string
	TCPAddr *net.TCPAddr

	listenerConfig net.ListenConfig
	listener       *net.TCPListener

	context context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
}

func NewBroker(host string, port string, lc net.ListenConfig) *Broker {

	addr := net.JoinHostPort(host, port)
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Fatalf("fatal: could not resolve tcp addr: %v", addr)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Broker{
		Addr:           addr,
		TCPAddr:        tcpAddr,
		listenerConfig: lc,
		context:        ctx,
		cancel:         cancel,
		mu:             sync.Mutex{},
	}
}

func (b *Broker) Start() {
	log.Printf("Starting server..")
	log.Printf("listning on %s", b.Addr)

}

func (b *Broker) Stop() {

}
