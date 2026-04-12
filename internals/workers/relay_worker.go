package workers

import (
	"context"
	"log"
	"time"

	"golang.org/x/text/cases"
)

type RelayWorker struct {
}

func NewRelayWorker() *RelayWorker {
	return &RelayWorker{}
}

func (w *RelayWorker) Start(ctx context.Context) {

	//polling every 200ms for low latency recovery
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	log.Println("Relay worker started")

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			w.processOutbox(ctx)
		}
	}
}

func (w *RelayWorker) processOutbox(ctx context.Context){

	entries, err := 
}
