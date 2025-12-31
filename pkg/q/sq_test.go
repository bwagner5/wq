package q_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/bwagner5/q/pkg/q"
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

func TestSQ(t *testing.T) {

	t.Run("Idempotent Start", func(t *testing.T) {
		queue := q.NewSimpleQueue(2, 0, 5, echoProcessor)
		require.Equal(t, q.StateInitialized, queue.StateCode())
		require.NoError(t, queue.Start(t.Context()))
		require.Equal(t, q.StateActive, queue.StateCode())
		require.NoError(t, queue.Start(t.Context()))
		require.Equal(t, q.StateActive, queue.StateCode())
		require.NoError(t, queue.Start(t.Context()))
		require.Equal(t, q.StateActive, queue.StateCode())
		require.Equal(t, 0, queue.Stop())
		require.Equal(t, q.StateStopped, queue.StateCode())
	})

	t.Run("Adds Not Allowed When Draining", func(t *testing.T) {
		stop := make(chan struct{})
		timeoutProcessor := func(ctx context.Context, input int) (int, error) {
			select {
			case <-ctx.Done():
				require.Fail(t, "processor stopped on context.Done() rather that the expected stop channel")
			case <-stop:
			}
			return 0, nil
		}
		queue := q.NewSimpleQueue(2, 0, 5, timeoutProcessor)
		queue.Start(t.Context())
		queue.MustAdd(1)
		go func() {
			queue.Drain(time.Second)
		}()
		// wait for the queue to start draining
		for queue.StateCode() != q.StateDraining {
			time.Sleep(10 * time.Microsecond)
		}
		require.EqualError(t, q.NotAcceptingWorkErr, queue.Add(2).Error())
		close(stop)
	})

	t.Run("Simple Work Queue", func(t *testing.T) {
		queue := q.NewSimpleQueue(2, 0, 5, echoProcessor)
		queue.Start(t.Context())

		for i := 1; i <= 5; i++ {
			queue.MustAdd(i)
		}

		err := queue.Drain(time.Second * 5)
		require.NoError(t, err)
		require.Equal(t, q.StateStopped, queue.StateCode())

		var results []int
		for r := range queue.Results() {
			results = append(results, *r)
		}
		slices.Sort(results)

		for i, r := range results {
			require.Equal(t, i+1, r)
		}

		for range queue.Errors() {
			require.Fail(t, "should not have received any errors during processing")
		}
	})

	t.Run("Collect", func(t *testing.T) {
		results, err := q.Collect(t.Context(), []int{1, 2, 3, 4, 5}, echoProcessor)
		require.NoError(t, err)
		require.Len(t, results, 5)
		slices.Sort(results)

		for i, r := range results {
			require.Equal(t, i+1, r)
		}
	})

	t.Run("Errors", func(t *testing.T) {
		queue := q.NewSimpleQueue(2, 1, 5, errProcessor)
		queue.Start(t.Context())

		for i := 1; i <= 5; i++ {
			queue.MustAdd(i)
		}

		err := queue.Drain(time.Second * 5)
		require.NoError(t, err)
		require.Equal(t, q.StateStopped, queue.StateCode())

		var errs []error
		for err := range queue.Errors() {
			errs = append(errs, err)
			require.Equal(t, fmt.Errorf("error"), err)
		}
		require.Len(t, errs, 5)
	})

	t.Run("Draining Timeout", func(t *testing.T) {
		stop := make(chan struct{})
		timeoutProcessor := func(ctx context.Context, input int) (int, error) {
			select {
			case <-ctx.Done():
				require.Fail(t, "processor stopped on context.Done() rather that the expected stop channel")
			case <-stop:
			}
			return 0, nil
		}
		queue := q.NewSimpleQueue(2, 1, 5, timeoutProcessor)
		queue.Start(t.Context())

		for i := 1; i <= 5; i++ {
			queue.MustAdd(i)
		}

		require.Equal(t, q.StateActive, queue.StateCode())

		err := queue.Drain(time.Microsecond * 100)
		require.EqualError(t, q.DrainTimeoutErr, err.Error())
		require.Equal(t, q.StateDraining, queue.StateCode())
		close(stop)
		err = queue.Drain(time.Microsecond * 100)
		require.NoError(t, err)
		require.Equal(t, q.StateStopped, queue.StateCode())

		for r := range queue.Results() {
			require.Equal(t, 0, *r)
		}
	})
}

func BenchmarkSimpleQueue(b *testing.B) {

	type QueueBenchmarks[T, R any] struct {
		name    string
		options q.SimpleOptions[T, R]
	}

	queueOpts := []QueueBenchmarks[int, int]{
		{
			name: "Default Queue w/ Results Queueing",
			options: q.SimpleOptions[int, int]{
				Concurrency:      4,
				InputQueueSize:   100,
				ResultsQueueSize: 100,
				ResultsPolicy:    &q.ResultsPolicyQueue,
				ProcessorFunc:    echoProcessor,
			},
		},
		{
			name: "Queue w/ Result Dropping",
			options: q.SimpleOptions[int, int]{
				Concurrency:      4,
				InputQueueSize:   100,
				ResultsQueueSize: 100,
				ResultsPolicy:    &q.ResultsPolicyDrop,
				ProcessorFunc:    echoProcessor,
			},
		},
		{
			name: "Queue w/ No Results",
			options: q.SimpleOptions[int, int]{
				Concurrency:      4,
				InputQueueSize:   100,
				ResultsQueueSize: 0,
				ProcessorFunc:    echoProcessor,
			},
		},
	}

	for _, opts := range queueOpts {
		b.Run(opts.name, func(b *testing.B) {
			queue := q.NewFromSimpleOptions(opts.options)
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
