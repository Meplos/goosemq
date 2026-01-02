package http

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/Meplos/goosemq/internal/app/broker"
)

func StartHttpServer(httpPort string, b broker.Broker, ctx context.Context) {
	mux := http.NewServeMux()

	mux.HandleFunc("/monitor/state", func(w http.ResponseWriter, r *http.Request) {
		state := b.Snapshot()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(state)
	})

	mux.Handle("/", http.FileServer(http.Dir("./static")))

	srv := &http.Server{
		Addr:    httpPort,
		Handler: mux,
	}

	// goroutine pour servir
	go func() {
		log.Printf("[http] monitor listening on %s", httpPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server ListenAndServe: %v", err)
		}
	}()

	// goroutine pour shutdown quand le context est canceled
	go func() {
		<-ctx.Done()
		log.Println("[http] shutting down http server...")
		srv.Shutdown(context.Background())
	}()
}
