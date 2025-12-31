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
	results, err := q.Collect(ctx, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, func(ctx context.Context, n int) (int, error) {
		select {
		// random sleep between 0-99ms
		case <-time.After(time.Duration(rand.IntN(100) * int(time.Millisecond))):
		case <-ctx.Done():
			return -1, fmt.Errorf("terminated early for %d", n)
		}
		return n, nil
	})
	if err != nil {
		return err
	}

	fmt.Printf("Done processing and collecting results (%d)!", len(results))
	return nil
}
