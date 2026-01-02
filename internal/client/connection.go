package client

import (
	"net"

	"github.com/Meplos/goosemq/internal/protocol"
)

type ConnState string

const (
	DISCONNECTED ConnState = "DISCONNECTED"
	CONNECTING   ConnState = "CONNECTING"
	CONNECTED    ConnState = "CONNECTED"
	STOPPED      ConnState = "STOPPED"
)

type ConnManager interface {
	Start()
	Stop()
	GetConn() (net.Conn, error)
	ReportError(err error)
	Send(buff protocol.Frame) (chan protocol.Frame, error)
	AckChan() <-chan protocol.Frame
}
