# Work Queue (wq)

wq is a simple, concurrent, generic work queue go library. The work queue implementation is simple but offers useful features that are often left off when implementing a quick work queue within a project. 

## Features:
 - Only standard library dependencies
 - Generic Processor for typed pipelines
 - Retries
   - Configurable policies to define the retry strategy and max retries
 - Metadata per Work Item
   - Each Result returned includes Metadata that has information regarding processing latency, retries, and more.
 - Aggregate Status of the Work Queue
   - Includes aggregated statistics on latency (min/max/avg) as well as items processed, retries, failures, etc.
 - Drain() w/ Timeout or Stop() abruptly
   - Shutdown the work queue gracefully or forcefully purge the queue!


## Examples:

More examples [here](examples/).

```go
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
```