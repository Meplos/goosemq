package network

import (
	"net"

	"github.com/Meplos/goosemq/internal/protocol"
)

type Socket interface {
	Send(frame protocol.Frame) error
	Close()
	ID() string
	StartReaderLoop(handler func(frame protocol.Frame), onError func(err error))
	StartWriterLoop(onError func(err error))
}

type GatewayState string

const (
	STOPPED GatewayState = "STOPPED"
	STARTED GatewayState = "STARTED"
	CLOSED  GatewayState = "CLOSED"
)

type Gateway interface {
	Start(addr string) error
	Close()
	OnNewConnection(conn net.Conn)
	Connections() map[string]Socket
	State() GatewayState
}
