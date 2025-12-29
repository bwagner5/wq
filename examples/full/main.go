package main

import (
	"context"
	"encoding/json"
	"errors"
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
	for i := range 15 {
		if err := q.AddWithBackOff(i, time.Second, 2); err != nil {
			fmt.Printf("Unable to add work item %d: %s\n", i, err)
		}
	}

	// gracefully drain the work queue in 10 seconds
	err := q.Drain(10 * time.Second)
	if err != nil {
		// if we can't drain in 10 seconds, then shutdown forcefully
		if errors.Is(err, wq.DrainTimeoutErr) {
			// see how many work items weren't processed due to a forceful shutdown
			unprocessed := q.Stop()
			fmt.Printf("Stopped Queue forcefully without processing %d work items\n", unprocessed)
		} else {
			panic(err)
		}
	} else {
		fmt.Printf("Gracefully drained work queue\n")
	}

	// print some stats about the queue processing
	fmt.Printf("Stats: \n")
	stats, _ := json.MarshalIndent(q.Status(), " ", "    ")
	fmt.Printf("%s\n", stats)
}
