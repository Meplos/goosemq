package app

import (
	"context"
	"log"

	"github.com/Meplos/goosemq/internal/app/broker"
	"github.com/Meplos/goosemq/internal/app/dispatcher"
	"github.com/Meplos/goosemq/internal/app/network"
	"github.com/Meplos/goosemq/internal/app/network/tcp"
	"github.com/Meplos/goosemq/internal/config"
	"github.com/Meplos/goosemq/internal/http"
)

type App struct {
	Gateway    network.Gateway
	Dispatcher dispatcher.Dispatcher
	Broker     broker.Broker
	ConnInfo   config.ConnexionInfo
	ctx        context.Context
}

func New(info config.ConnexionInfo, ctx context.Context) *App {
	b := broker.New()
	d := dispatcher.New(b)
	g := tcp.New(d.DispatchChan())
	return &App{
		ConnInfo:   info,
		Dispatcher: d,
		Broker:     b,
		Gateway:    g,
		ctx:        ctx,
	}
}

func (a *App) Start() {
	go a.Broker.Start()
	go a.Dispatcher.Start()
	go a.Gateway.Start(a.ConnInfo.Addr())
	http.StartHttpServer(":9090", a.Broker, a.ctx)

	<-a.ctx.Done()
	log.Println("[App] shutting down...")
	a.Close()
}

func (a *App) Close() {
	a.Broker.Close()
	a.Dispatcher.Close()
	a.Gateway.Close()
}
