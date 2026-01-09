package client

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/Meplos/goosemq/internal/protocol"
	"github.com/Meplos/goosemq/pkg/config"
)

type Client struct {
	started bool
	Config  config.ConnexionInfo
	conn    net.Conn
	state   ConnState
	mu      sync.RWMutex

	writerChan chan []byte
	stopChan   chan struct{}
	ackChan    chan protocol.Frame

	pending map[protocol.ReqId]chan protocol.Frame
}

func New(info config.ConnexionInfo) ConnManager {
	return &Client{
		Config:     info,
		state:      DISCONNECTED,
		mu:         sync.RWMutex{},
		conn:       nil,
		started:    false,
		writerChan: make(chan []byte, 1000),
		stopChan:   make(chan struct{}),
		pending:    make(map[protocol.ReqId]chan protocol.Frame),
		ackChan:    make(chan protocol.Frame, 10_000),
	}
}

func (c *Client) Stop() {
	c.mu.Lock()
	c.started = false
	c.state = STOPPED
	c.mu.Unlock()
	select {
	case <-c.writerChan:
		// déjà fermé, rien à faire
	default:
		close(c.writerChan)
	}

	// Ferme le channel messageChan pour que les workers se terminent
	select {
	case <-c.stopChan:
		// déjà fermé, rien à faire
	default:
		close(c.stopChan)
	}
}

func (c *Client) connectionLoop() {
	c.mu.Lock()
	c.started = true
	c.mu.Unlock()
	backoff := 10 * time.Millisecond
	for {

		c.mu.Lock()
		if c.state == STOPPED {
			if c.conn != nil {
				c.conn.Close()
			}
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()

		if c.conn == nil || c.state == DISCONNECTED {
			c.mu.Lock()
			c.state = CONNECTING
			conn, err := net.Dial("tcp", c.Config.Addr())
			if err != nil {
				c.state = DISCONNECTED
				c.mu.Unlock()
				time.Sleep(backoff)
				if backoff < 1*time.Second {
					backoff *= 2
				}
				continue
			}
			c.state = CONNECTED
			c.conn = conn
			c.mu.Unlock()
			backoff = 10 * time.Millisecond
		}

		c.mu.RLock()
		state := c.state
		c.mu.RUnlock()

		if state != CONNECTED {
			time.Sleep(200 * time.Millisecond)
		}
	}
}

func (c *Client) Start() {
	c.mu.RLock()
	started := c.started
	c.mu.RUnlock()
	if started {
		return
	}
	go c.connectionLoop()
	go c.writerLoop()
	go c.readerLoop()
}

func (c *Client) GetConn() (net.Conn, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.state != CONNECTED || c.conn == nil {
		return nil, errors.New("INVALID_CONN")
	}
	return c.conn, nil
}

func (c *Client) ReportError(err error) {
	log.Printf("[ConnManager] ReportError: %s", err)
	if !errors.Is(err, io.EOF) && !isNetError(err) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = DISCONNECTED
	if c.conn != nil {
		c.conn.Close()
	}
	c.conn = nil
}

func isNetError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return false
}

func (c *Client) Send(frame protocol.Frame) (chan protocol.Frame, error) {
	payload, err := protocol.Marshall(frame)
	if err != nil {
		return nil, err
	}

	var respChan chan protocol.Frame
	if frame.Type == protocol.PULL {
		respChan = make(chan protocol.Frame, 1)
		c.mu.Lock()
		c.pending[frame.ID] = respChan
		c.mu.Unlock()
	}

	select {
	case c.writerChan <- payload:
		return respChan, nil
	default:
		close(respChan)
		return nil, errors.New("buffer_full")
	}
}

func (c *Client) writerLoop() {
	for {
		select {
		case <-c.stopChan:
			return
		case buff, ok := <-c.writerChan:
			if !ok {
				return
			}
			c.mu.RLock()
			conn := c.conn
			state := c.state
			c.mu.RUnlock()
			if conn == nil || state != CONNECTED {
				continue
			}
			_, err := conn.Write(buff)
			if err != nil {
				c.ReportError(err)
			}

		}

	}
}

func (c *Client) readerLoop() {
	backoff := 10 * time.Millisecond
	for {

		select {
		case <-c.stopChan:
			return
		default:
		}
		lenBuff := make([]byte, 4)

		c.mu.RLock()
		conn := c.conn
		state := c.state
		c.mu.RUnlock()

		if conn == nil || state != CONNECTED {
			time.Sleep(backoff)
			if backoff < 1*time.Second {
				backoff *= 2
			}
			continue
		}

		if _, err := io.ReadFull(conn, lenBuff); err != nil {
			log.Printf("error: client %s disconnected %s", conn.RemoteAddr(), err)
			c.ReportError(err)
			continue
		}
		size := binary.BigEndian.Uint32(lenBuff)

		payload := make([]byte, size)

		if _, err := io.ReadFull(conn, payload); err != nil {
			c.ReportError(err)
			continue
		}
		var frame protocol.Frame
		err := json.Unmarshal(payload, &frame)
		if err != nil {
			log.Printf("[ReadLoop]Cant unmarshal frame. error: %s", err)
			continue
		}

		if frame.Type == protocol.ACK {
			select {
			case c.ackChan <- frame:
			default:
				log.Printf("[ReaderLoop] ack receive but buffer full. dropping %s", frame.ID)
			}
			continue
		}

		c.mu.RLock()
		respChan, ok := c.pending[frame.ID]
		c.mu.RUnlock()
		if !ok {
			continue
		}

		select {
		case respChan <- frame:
		default:
			log.Printf("[ReaderLoop] response channel blocked for %s", frame.ID)
		}
		c.mu.Lock()
		delete(c.pending, frame.ID)
		c.mu.Unlock()
		backoff = 10 * time.Millisecond
	}
}

func (c *Client) AckChan() <-chan protocol.Frame {
	return c.ackChan
}
