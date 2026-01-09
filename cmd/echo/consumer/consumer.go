package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Meplos/goosemq/internal/client/consumer"
	"github.com/Meplos/goosemq/internal/data"
	"github.com/Meplos/goosemq/pkg/config"
)

func main() {
	// Configuration de connexion au broker
	info := config.ConnexionInfo{
		Host: "localhost",
		Port: "7456",
	}

	// Crée un consumer avec les options par défaut
	c := consumer.New(info)

	// Abonne un handler sur le topic "echo"
	c.Subscribe("echo", func(msg data.Content) error {
		fmt.Printf("[echo] received: %s\n", msg["content"])
		return nil
	})

	// Démarre le consumer
	c.Start()
	log.Println("Consumer started, listening to topic 'echo'...")

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// ---- Ticker : 5 messages / seconde
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[Consumer] shutting down...")
			c.Stop()
			return
		}
	}
}
