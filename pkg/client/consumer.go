package client

import (
	"github.com/Meplos/goosemq/internal/client/consumer"
	"github.com/Meplos/goosemq/pkg/config"
)

type Consumer interface {
	Start()
	Subscribe(topic string, handler consumer.HandlerFunc)
	Stop()
	Close()
}

func NewConsumer(info config.ConnexionInfo) Consumer {
	return consumer.New(info)
}
