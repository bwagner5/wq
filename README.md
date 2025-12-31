# Queue (q)

q is a simple, concurrent, generic work queue library for go. The work queue implementation is simple but offers useful features that are often left off when implementing a quick work queue within a project. 

## Features:
 - ✅ Only standard library dependencies
 - ✅ Generic Processor for typed pipelines
 - ✅ Retries
   - Configurable policies to define the retry strategy and max retries
 - ✅ Metadata per Work Item [OPTIONAL]
   - Each Result returned includes Metadata that has information regarding processing latency, retries, and more.
 - ✅ Aggregate Status of the Work Queue
   - Includes aggregated statistics on latency (min/max/avg) as well as items processed, retries, failures, etc.
 - ✅ Drain() w/ Timeout or Stop() abruptly
   - Shutdown the work queue gracefully or forcefully purge the queue!

## Queue Types

There are two types of work queues:

1. Simple Queue - Operates on a type without wrapping with Metadata and Results. If you need something simple and fast (no allocations in the queue after Start()), use this one!
2. Queue - If you want additional data per processed item, aggregate stats, and more context in the Results (like the input, any errors on retries, etc.) then use this one!
 
When in doubt, just use the Queue. If you need to squeeze out some more performance, switch to the Simple Queue.

## Examples:

More examples [here](examples/).

# Queue

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/bwagner5/q/pkg/q"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Printf("Error:  %s", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	// configure the queue with basic options
	queue := q.NewFromOptions(q.Options[int, int]{
		Concurrency:      2,
		InputQueueSize:   10,
		ResultsQueueSize: 10,
		ErrorsQueueSize:  10,
		ProcessorFunc: func(ctx context.Context, n int) (int, error) {
			select {
			// random sleep between 0-99ms
			case <-time.After(time.Duration(rand.IntN(100) * int(time.Millisecond))):
			case <-ctx.Done():
				return -1, fmt.Errorf("terminated early for %d", n)
			}
			return n, nil
		},
	})

	// start the queue processors
	if err := queue.Start(ctx); err != nil {
		return err
	}

	// consume the results in a separate go routine
	go func() {
		for r := range queue.Results() {
			fmt.Printf("Got Echo Result: %d -> %d in %s\n", r.Input, r.Output, r.Metadata.Latency())
		}
	}()

	// add items to the work queue
	for i := range 15 {
		if err := queue.AddWithBackOff(i, time.Second, 2); err != nil {
			fmt.Printf("Unable to add work item %d: %s\n", i, err)
		}
	}

	// gracefully drain the work queue in 10 seconds
	err := queue.Drain(10 * time.Second)
	if err != nil {
		// if we can't drain in 10 seconds, then shutdown forcefully
		if errors.Is(err, q.DrainTimeoutErr) {
			// see how many work items weren't processed due to a forceful shutdown
			unprocessed := queue.Stop()
			fmt.Printf("Stopped Queue forcefully without processing %d work items\n", unprocessed)
		} else {
			return err
		}
	} else {
		fmt.Printf("Gracefully drained work queue\n")
	}

	// print some stats about the queue processing
	fmt.Printf("Stats: \n")
	stats, _ := json.MarshalIndent(queue.Status(), " ", "    ")
	fmt.Printf("%s\n", stats)
	return nil
}

```

# Simple Queue

```go
package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/bwagner5/q/pkg/q"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Printf("Error: %s", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	// configure the queue
	queue := q.NewSimpleQueue(2, 0, 10, func(ctx context.Context, n int) (int, error) {
		select {
		// random sleep between 0-99ms
		case <-time.After(time.Duration(rand.IntN(100) * int(time.Millisecond))):
		case <-ctx.Done():
			return -1, fmt.Errorf("terminated early for %d", n)
		}
		return n, nil
	})

	// start the queue processors
	if err := queue.Start(ctx); err != nil {
		return err
	}

	// consume the results in a separate go routine
	go func() {
		for r := range queue.Results() {
			fmt.Printf("Got Echo Result: %d\n", *r)
		}
	}()

	// add items to the work queue
	for i := range 10 {
		queue.MustAdd(i)
	}

	// gracefully drain the work queue in 10 seconds
	err := queue.Drain(10 * time.Second)
	if err != nil {
		return err
	}
	fmt.Println("Done draining!")
	return nil
}
```
