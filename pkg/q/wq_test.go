package q_test

import (
	"testing"
	"time"

	"github.com/bwagner5/q/pkg/q"
	"github.com/stretchr/testify/require"
)

func TestWQ(t *testing.T) {

	t.Run("Simple Work Queue", func(t *testing.T) {
		queue := q.New(2, 0, 5, echoProcessor)
		queue.Start(t.Context())

		for i := 1; i <= 5; i++ {
			queue.MustAdd(i)
		}

		err := queue.Drain(time.Second * 5)
		require.NoError(t, err)

		require.Equal(t, q.StateStopped, queue.Status().StateCode)
		require.Equal(t, 5, queue.Status().QueuedResults)
		require.Equal(t, 5, queue.Status().TotalWorkItemsProcessed)
		require.Equal(t, 5, queue.Status().TotalSuccessfullyProcessed)
		require.Equal(t, 0, queue.Status().TotalProcessingErrors)
		require.Equal(t, 0, queue.Status().QueuedWorkItems)
		require.Equal(t, 0, queue.Status().TotalConsumedResults)
		require.Equal(t, 0, queue.Status().TotalRetries)
		require.Equal(t, 0, queue.Status().InProgressWorkItems)

		for result := range queue.Results() {
			require.Equal(t, result.Input, result.Output)
		}

		require.Equal(t, 5, queue.Status().TotalConsumedResults)
		require.Equal(t, 0, queue.Status().QueuedResults)

	})

	t.Run("Retries", func(t *testing.T) {
		queue := q.New(2, 1, 5, errProcessor)
		queue.Start(t.Context())

		for i := 1; i <= 5; i++ {
			queue.MustAdd(i)
		}

		err := queue.Drain(time.Second * 5)
		require.NoError(t, err)

		require.Equal(t, q.StateStopped, queue.Status().StateCode)
		require.Equal(t, 0, queue.Status().QueuedResults)
		require.Equal(t, 10, queue.Status().TotalWorkItemsProcessed)
		require.Equal(t, 0, queue.Status().TotalSuccessfullyProcessed)
		require.Equal(t, 5, queue.Status().TotalProcessingErrors)
		require.Equal(t, 0, queue.Status().QueuedWorkItems)
		require.Equal(t, 0, queue.Status().TotalConsumedResults)
		require.Equal(t, 5, queue.Status().TotalRetries)
		require.Equal(t, 0, queue.Status().InProgressWorkItems)

		for err := range queue.Errors() {
			require.Equal(t, "error", err.(*q.ResultErr[int, int]).Error())
		}
	})
}

func Benchmark(b *testing.B) {

	type QueueBenchmarks[T, R any] struct {
		name    string
		options q.Options[T, R]
	}

	queueOpts := []QueueBenchmarks[int, int]{
		{
			name: "Default Queue w/ Results Queueing",
			options: q.Options[int, int]{
				Concurrency:      4,
				InputQueueSize:   100,
				ResultsQueueSize: 100,
				ResultsPolicy:    &q.ResultsPolicyQueue,
				ProcessorFunc:    echoProcessor,
			},
		},
		{
			name: "Queue w/ Result Dropping",
			options: q.Options[int, int]{
				Concurrency:      4,
				InputQueueSize:   100,
				ResultsQueueSize: 100,
				ResultsPolicy:    &q.ResultsPolicyDrop,
				ProcessorFunc:    echoProcessor,
			},
		},
		{
			name: "Queue w/ No Results",
			options: q.Options[int, int]{
				Concurrency:      4,
				InputQueueSize:   100,
				ResultsQueueSize: 0,
				ProcessorFunc:    echoProcessor,
			},
		},
	}

	for _, opts := range queueOpts {
		b.Run(opts.name, func(b *testing.B) {
			queue := q.NewFromOptions(opts.options)
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
		})
	}
}
