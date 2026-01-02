package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/Meplos/goosemq/internal/app"
	"github.com/Meplos/goosemq/internal/config"
)

func main() {
	// Connexion info
	connInfo := config.ConnexionInfo{
		Host: "127.0.0.1",
		Port: "7456",
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()
	// Crée l'application
	a := app.New(connInfo, ctx)
	a.Start()
}
