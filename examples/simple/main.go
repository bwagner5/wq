package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/bwagner5/wq/pkg/wq"
)

func main() {
	ctx := context.Background()

	// configure the queue with basic options
	q := wq.NewFromOptions(wq.Options[int, int]{
		Concurrency:      2,
		InputQueueSize:   10,
		ResultsQueueSize: 10,
		ProcessorFunc: func(ctx context.Context, n int) (int, error) {
			// random sleep between 0-99ms
			time.Sleep(time.Duration(rand.IntN(100) * int(time.Millisecond)))
			return n, nil
		},
	})

	// start the queue processors
	if err := q.Start(ctx); err != nil {
		panic(err)
	}

	// consume the results in a separate go routine
	go func() {
		for r := range q.Results() {
			fmt.Printf("Got Echo Result: %d -> %d in %s\n", r.Input, r.Output, r.Metadata.Latency())
		}
	}()

	// add items to the work queue
	for i := range 10 {
		q.MustAdd(i)
	}

	// gracefully drain the work queue in 10 seconds
	err := q.Drain(10 * time.Second)
	if err != nil {
		panic(err)
	}

	// print some stats about the queue processing
	fmt.Printf("Stats: \n")
	stats, _ := json.MarshalIndent(q.Status(), " ", "    ")
	fmt.Printf("%s\n", stats)
}
