package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

type Event struct {
	id       uuid.UUID
	payload  []byte
	sentinel uint16
}

type BatchConfig struct {
	batchSize uint16
	maxAge    uint16 // TODO: Add support for emitting batches based on age
}

type Batch struct {
	events []Event
	count  uint16
	maxx   uint16
}

func (b *Batch) reset() {
	b.count = 0
	b.maxx = 0
	b.events = b.events[:0]
}

func (b *Batch) update(ev Event) {
	if ev.sentinel > b.maxx {
		b.maxx = ev.sentinel
	}
	b.count++
	b.events = append(b.events, ev)
}

func NewBatch(size uint16) Batch {
	return Batch{
		count:  0,
		maxx:   0,
		events: make([]Event, size),
	}
}

func createEvents(num int, delay uint16, events chan Event) {
	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)
	const payloadSize = 16
	randomBytes := make([]byte, payloadSize)

	for i := 0; i < num; i++ {
		for i := 0; i < payloadSize; i++ {
			randomBytes[i] = byte(r.Intn(256))
		}
		events <- Event{
			id:       uuid.New(),
			payload:  randomBytes,
			sentinel: uint16(rand.Intn(1000)),
		}
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}
	close(events)
}

func consume(events chan Event, batches chan Batch, config BatchConfig) {
	batch := NewBatch(config.batchSize)
	for {
		if batch.count >= config.batchSize {
			batches <- batch
			batch.reset()
		}
		e, ok := <-events
		if ok {
			fmt.Println("Successfully consumed") // At large ingress rates this will spam
			batch.update(e)
		} else {
			fmt.Println("Consumption ended")
			break
		}
	}
	// Check if there is a partial batch that needs to be emitted
	if batch.count > 0 {
		batches <- batch
	}
	close(batches)
}

func main() {
	input := make(chan Event)  // TODO: Unbounded channel is okay for now but ideally this would have at most 1 channel's worth of events at max
	output := make(chan Batch) // TODO: Unbounded channel is okay for now but ideally this would never have >1 batch
	cfg := BatchConfig{
		batchSize: 3,
		maxAge:    10,
	}
	go createEvents(10, 2000, input)
	go consume(input, output, cfg)

	for {
		batch, ok := <-output
		if ok {
			fmt.Println(batch.count, " events batched up with a max value of ", batch.maxx, ".")
		} else {
			// TODO: There could be a partial batch that needs to be emitted.
			break
		}
	}
}
