package client

import (
	"github.com/Meplos/goosemq/internal/client/producer"
	"github.com/Meplos/goosemq/internal/data"
	"github.com/Meplos/goosemq/pkg/config"
)

type Producer interface {
	Start()
	Stop()
	Publish(topic string, body data.Content) error
}

func NewProducer(info config.ConnexionInfo) Producer {
	return producer.New(info)
}
