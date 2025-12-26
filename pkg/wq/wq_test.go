package wq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bwagner5/wq/pkg/wq"
	"github.com/stretchr/testify/require"
)

var (
	echoProcessor = func(ctx context.Context, input int) (int, error) {
		return input, nil
	}

	errProcessor = func(ctx context.Context, input int) (int, error) {
		return input, fmt.Errorf("error")
	}
)

func TestWQ(t *testing.T) {

	t.Run("Simple Work Queue", func(t *testing.T) {
		ctx := context.Background()
		queue := wq.New(1, 0, 5, 5, echoProcessor)
		queue.Start(ctx)

		for i := 1; i <= 5; i++ {
			queue.MustAdd(i)
		}

		err := queue.Drain(time.Minute * 5)
		require.NoError(t, err)

		for i := 1; i <= 5; i++ {
			result, ok := queue.Result()
			require.Equal(t, true, ok)
			require.Equal(t, i, result.Output)
		}
	})

	t.Run("Retries", func(t *testing.T) {
		ctx := context.Background()
		queue := wq.New(1, 1, 5, 5, errProcessor)
		queue.Start(ctx)

		for i := 1; i <= 5; i++ {
			queue.MustAdd(i)
		}

		err := queue.Drain(time.Minute * 5)
		require.NoError(t, err)

		for i := 1; i <= 5; i++ {
			result, ok := queue.Result()
			require.Equal(t, true, ok)
			require.Equal(t, 2, result.Metadata.Attempt)
			require.Equal(t, 1, result.Metadata.Retries)
			require.Equal(t, fmt.Errorf("error"), result.Error)
		}
	})
}

func Benchmark(b *testing.B) {
	queue := wq.New(5, 1, 100, 100, echoProcessor)
	queue.Start(b.Context())
	b.ResetTimer()
	go func() {
		for range queue.Results() {
		}
	}()

	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			queue.MustAdd(1)
			// require.NoError(b, queue.AddWithBackOff(1, 5*time.Second, 3), fmt.Sprintf("%+v\n", queue.Status()))
		}
	})

	unprocessed := queue.Stop()
	fmt.Printf("Unprocessed Items: %d\n", unprocessed)
	fmt.Printf("%+v\n", queue.Status())
}
