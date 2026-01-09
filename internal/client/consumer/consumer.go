package consumer

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/Meplos/goosemq/internal/client"
	"github.com/Meplos/goosemq/internal/data"
	"github.com/Meplos/goosemq/internal/protocol"
	"github.com/Meplos/goosemq/pkg/config"
	"github.com/google/uuid"
)

type HandlerFunc func(content data.Content) error
type ConsumerState string

const (
	RUNNING ConsumerState = "RUNNING"
	STOPPED ConsumerState = "STOPPED"
	CLOSED  ConsumerState = "CLOSED"
)

type ConsumerMessage struct {
	Value data.TopicMessage
	Topic string
}
type AckMessage struct {
	ID    uuid.UUID
	Topic string
}

type ConsumerRuntime struct {
	state       ConsumerState
	ConnManager client.ConnManager

	MaxMessage      int
	Timeout         time.Duration
	NbHandlerWorker int
	NbAckWorker     int
	MessageBuffSize int
	AckBuffSize     int
	mu              sync.RWMutex

	handlers    map[string]HandlerFunc
	messageChan chan *ConsumerMessage
	ackChan     chan *AckMessage
	stopChan    chan struct{}
}

type Options struct {
	Timeout         time.Duration
	MaxMessage      int
	MessageBuffSize int
	AckBuffSize     int
	NbHandlerWorker int
	NbAckWorker     int
}

func New(info config.ConnexionInfo) *ConsumerRuntime {
	opts := Options{
		Timeout:         5 * time.Second,
		MaxMessage:      5,
		MessageBuffSize: 10_000,
		AckBuffSize:     10_000,
		NbHandlerWorker: 10,
		NbAckWorker:     10,
	}

	return NewWithOptions(info, opts)
}

func NewWithOptions(info config.ConnexionInfo, opts Options) *ConsumerRuntime {
	return &ConsumerRuntime{
		state:           STOPPED,
		ConnManager:     client.New(info),
		MaxMessage:      opts.MaxMessage,
		Timeout:         opts.Timeout,
		NbHandlerWorker: opts.NbHandlerWorker,
		NbAckWorker:     opts.NbAckWorker,
		AckBuffSize:     opts.AckBuffSize,
		MessageBuffSize: opts.MessageBuffSize,
		handlers:        make(map[string]HandlerFunc),
		messageChan:     make(chan *ConsumerMessage, opts.MessageBuffSize),
		ackChan:         make(chan *AckMessage, opts.AckBuffSize),
		stopChan:        make(chan struct{}),
	}
}

func (c *ConsumerRuntime) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == CLOSED {
		return
	}
	c.state = RUNNING
	c.stopChan = make(chan struct{})
	c.messageChan = make(chan *ConsumerMessage, c.MessageBuffSize)
	c.ackChan = make(chan *AckMessage, c.AckBuffSize)
	c.ConnManager.Start()
	go c.PullLoop()
	go c.workerPool(c.NbHandlerWorker)
	go c.ackLoop(c.NbAckWorker)
}

func (c *ConsumerRuntime) PullLoop() {
	backoff := 50 * time.Millisecond
	for {
		select {
		case <-c.stopChan:
			return
		default:
		}

		c.mu.RLock()
		state := c.state
		c.mu.RUnlock()
		if state == STOPPED {
			return
		}

		topics := make([]string, 0)
		c.mu.RLock()
		for t, _ := range c.handlers {
			topics = append(topics, t)
		}
		c.mu.RUnlock()

		nbMessage := 0

		for _, topic := range topics {
			respChan := c.sendPullRequest(topic)
			if respChan == nil {
				continue
			}

			res := <-respChan
			var response data.PullResponse
			err := json.Unmarshal(res.Payload, &response)
			if err != nil {
				log.Printf("[PullLoop] cant unmarshall response: error %s", err)
			}
			nbMessage += len(response.Items)

			for _, itm := range response.Items {
				c.messageChan <- &ConsumerMessage{
					Topic: itm.Topic, // todo read topic from response
					Value: itm,
				}
			}
		}

		if nbMessage > 0 {
			backoff = 50 * time.Millisecond
		} else {
			if backoff < 10*time.Second {
				backoff *= 2
			}
		}

		time.Sleep(backoff)

	}
}

func (c *ConsumerRuntime) sendPullRequest(topic string) chan protocol.Frame {
	request := data.PullRequest{
		Topic:      topic,
		Timeout:    c.Timeout,
		MaxMessage: c.MaxMessage,
	}
	data, err := json.Marshal(request)
	if err != nil {
		log.Printf("[PullLoop] fail to json marshall data. error: %s", err)
		return nil
	}

	respChan, err := c.ConnManager.Send(protocol.Frame{
		ID:      protocol.NewId(),
		Type:    protocol.PULL,
		Payload: data,
	})

	if err != nil {
		c.ConnManager.ReportError(err)
		return nil
	}

	return respChan
}

func (c *ConsumerRuntime) workerPool(workerCount int) {
	for range workerCount {
		go func() {
			for {
				select {
				case <-c.stopChan:
					{
						return
					}
				case msg, ok := <-c.messageChan:
					if !ok {
						return
					}
					c.mu.RLock()
					handler, ok := c.handlers[msg.Topic]
					if !ok {
						log.Printf("[Worker] warn: topic %s no handlers found", msg.Topic)
						continue
					}
					c.mu.RUnlock()

					if err := handler(msg.Value.Body); err != nil {
						log.Printf("[worker] warn: task terminate with error %s. MsgId: %s Content:%s", err, msg.Value.ID, msg.Value.Body)
					}
					c.ackChan <- &AckMessage{
						Topic: msg.Topic,
						ID:    msg.Value.ID,
					}

				}
			}
		}()

	}
}

func (c *ConsumerRuntime) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = STOPPED
	c.ConnManager.Stop()
	close(c.stopChan)
}
func (c *ConsumerRuntime) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Met le consumer dans l'état STOPPED pour sécuriser
	c.state = CLOSED

	// Stoppe le ConnManager si ce n'est pas déjà fait
	c.ConnManager.Stop()

	// Ferme le channel stopChan pour signaler aux goroutines de s'arrêter
	// déjà fermé, rien à faire
	close(c.stopChan)

	// Optionnel : libérer le map des handlers pour cleanup
	c.handlers = nil

	// Optionnel : libérer le client ConnManager si tu veux rendre le consumer inutilisable
	c.ConnManager = nil
}

func (c *ConsumerRuntime) Subscribe(topic string, handler HandlerFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handlers[topic] = handler
}

func (c *ConsumerRuntime) ackLoop(nbWorker int) {
	for range nbWorker {
		go func() {
			for {
				select {
				case ack, ok := <-c.ackChan:
					if !ok {
						return
					}
					c.ack(ack.Topic, ack.ID)
				case <-c.stopChan:
					return
				}
			}
		}()

	}
}

func (c *ConsumerRuntime) ack(topic string, id uuid.UUID) {
	request := data.AckRequest{
		Topic: topic,
		ID:    id,
	}
	buff, err := json.Marshal(request)
	if err != nil {
		log.Printf("[ACK] can't build json request. error: %s", err)
		return
	}

	_, err = c.ConnManager.Send(protocol.Frame{
		ID:      protocol.NewId(),
		Type:    protocol.ACK,
		Payload: buff,
	})
	if err != nil {
		c.ConnManager.ReportError(err)
	}

}
