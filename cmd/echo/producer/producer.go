package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Meplos/goosemq/internal/client/producer"
	"github.com/Meplos/goosemq/internal/config"
	"github.com/Meplos/goosemq/internal/data"
)

func main() {
	// ---- Connexion broker
	info := config.ConnexionInfo{
		Host: "127.0.0.1",
		Port: "7456", // adapte si besoin
	}

	p := producer.New(info)

	// ---- Start producer
	p.Start()
	log.Println("[Producer] started")

	// ---- Context annulé via CTRL+C
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// ---- Ticker : 5 messages / seconde
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	counter := 0

	for {
		select {
		case <-ctx.Done():
			log.Println("[Producer] shutting down...")
			p.Stop()
			return

		case <-ticker.C:
			counter++

			msg := data.Content{
				"content": fmt.Sprintf("hello from producer #%d", counter),
			}

			if err := p.Publish("echo", msg); err != nil {
				log.Printf("[Producer] publish error: %s", err)
			}
		}
	}
}
