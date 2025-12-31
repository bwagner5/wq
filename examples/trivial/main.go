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
