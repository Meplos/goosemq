package tcp

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"

	"github.com/Meplos/goosemq/internal/app/network"
	"github.com/Meplos/goosemq/internal/protocol"
	"github.com/google/uuid"
)

type TCPSocket struct {
	socketId  string
	Conn      net.Conn
	SendChan  chan protocol.Frame
	CloseChan chan struct{}
}

// StartReaderLoop implements [network.Socket].
func (s *TCPSocket) StartReaderLoop(handler func(frame protocol.Frame), onNetworkError func(err error)) {
	go func() {
		for {
			select {
			case <-s.CloseChan:
				return
			default:
			}

			lenBuff := make([]byte, 4)

			if _, err := io.ReadFull(s.Conn, lenBuff); err != nil {
				log.Printf("error: client %s disconnected %s", s.Conn.RemoteAddr(), err)
				onNetworkError(err)
				return
			}
			size := binary.BigEndian.Uint32(lenBuff)

			payload := make([]byte, size)

			if _, err := io.ReadFull(s.Conn, payload); err != nil {
				onNetworkError(err)
				return
			}
			var frame protocol.Frame
			err := json.Unmarshal(payload, &frame)
			if err != nil {
				log.Printf("[ReadLoop]Cant unmarshal frame. error: %s", err)
				continue
			}

			handler(frame)
		}
	}()
}

// StartWriterLoop implements [network.Socket].
func (s *TCPSocket) StartWriterLoop(onNetworkError func(err error)) {
	go func() {
		for {
			select {
			case <-s.CloseChan:
				return
			case frame, ok := <-s.SendChan:
				if !ok {
					return
				}
				payload, err := protocol.Marshall(frame)
				if err != nil {
					log.Printf("[WriterLoop] marshalling error. error %s", err)
					continue
				}

				_, err = s.Conn.Write(payload)
				if err != nil {
					log.Printf("[WriterLoop] Conn close must disconnet from gateway. error: %s", err)
					onNetworkError(err)
					return
				}
			}
		}

	}()
}

func NewSocket(conn net.Conn) network.Socket {
	s := &TCPSocket{
		SendChan:  make(chan protocol.Frame, 1_000),
		CloseChan: make(chan struct{}),
		Conn:      conn,
		socketId:  uuid.New().String(),
	}

	return s
}

func (s *TCPSocket) Close() {
	log.Printf("[Socket] shutdown %s: started", s.ID())
	close(s.CloseChan)
	if err := s.Conn.Close(); err != nil {
		log.Printf("[Socket] shudown %s: error. %s", s.ID(), err)
		return
	}
	log.Printf("[Socket] shudown %s: success", s.ID())
}

func (s *TCPSocket) Send(frame protocol.Frame) error {
	select {
	case s.SendChan <- frame:
	default:
		return errors.New("write_err")
	}
	return nil
}

func (s *TCPSocket) ID() string {
	return s.socketId
}
