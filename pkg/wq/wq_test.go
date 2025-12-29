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
		queue := wq.New(1, 0, 5, echoProcessor)
		queue.Start(ctx)

		for i := 1; i <= 5; i++ {
			queue.MustAdd(i)
		}

		err := queue.Drain(time.Second * 5)
		require.NoError(t, err)

		for i := 1; i <= 5; i++ {
			result, ok := queue.Result()
			require.Equal(t, true, ok)
			require.Equal(t, i, result.Output)
		}
	})

	t.Run("Retries", func(t *testing.T) {
		ctx := context.Background()
		queue := wq.New(1, 1, 5, errProcessor)
		queue.Start(ctx)

		for i := 1; i <= 5; i++ {
			queue.MustAdd(i)
		}

		err := queue.Drain(time.Second * 5)
		require.NoError(t, err)

		for i := 1; i <= 5; i++ {
			result, ok := queue.Result()
			require.Equal(t, true, ok)
			require.Equal(t, 2, result.Metadata.Attempt())
			require.Equal(t, 1, result.Metadata.Retries())
			require.Equal(t, fmt.Errorf("error"), result.Error)
		}
	})
}

func Benchmark(b *testing.B) {

	type QueueBenchmarks[T, R any] struct {
		name    string
		options wq.Options[T, R]
	}

	queueOpts := []QueueBenchmarks[int, int]{
		{
			name: "Default Queue w/ Results Queueing",
			options: wq.Options[int, int]{
				Concurrency:      4,
				InputQueueSize:   100,
				ResultsQueueSize: 100,
				ResultsPolicy:    &wq.ResultsPolicyQueue,
				RetryPolicy:      &wq.DefaultRetryPolicy,
				ProcessorFunc:    echoProcessor,
			},
		},
		{
			name: "Queue w/ Result Dropping",
			options: wq.Options[int, int]{
				Concurrency:      4,
				InputQueueSize:   100,
				ResultsQueueSize: 100,
				ResultsPolicy:    &wq.ResultsPolicyDrop,
				RetryPolicy:      &wq.DefaultRetryPolicy,
				ProcessorFunc:    echoProcessor,
			},
		},
		{
			name: "Queue w/ No Results",
			options: wq.Options[int, int]{
				Concurrency:      4,
				InputQueueSize:   100,
				ResultsQueueSize: 0,
				RetryPolicy:      &wq.DefaultRetryPolicy,
				ProcessorFunc:    echoProcessor,
			},
		},
	}

	for _, opts := range queueOpts {
		b.Run(opts.name, func(b *testing.B) {
			queue := wq.NewFromOptions(opts.options)
			queue.Start(b.Context())
			b.ResetTimer()

			b.RunParallel(func(p *testing.PB) {
				for p.Next() {
					queue.AddWithBackOff(1, 100*time.Microsecond, 3)
					_, ok := queue.Result()
					if opts.options.ResultsQueueSize > 0 {
						require.True(b, ok)
					}
				}
			})

			require.NoError(b, queue.Drain(time.Second*5))
			status := queue.Status()
			b.ReportMetric(float64(status.MinLatency.Nanoseconds()), "MinLatency(ns)")
			b.ReportMetric(float64(status.MaxLatency.Nanoseconds()), "MaxLatency(ns)")
			b.ReportMetric(float64(status.AvgLatency.Nanoseconds()), "AvgLatency(ns)")
		})
	}
}
