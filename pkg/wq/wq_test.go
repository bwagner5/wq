package wq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bwagner5/wq/pkg/wq"
	"github.com/stretchr/testify/require"
)

func TestWQ(t *testing.T) {

	echoProcessor := func(ctx context.Context, input int) (int, error) {
		return input, nil
	}

	errProcessor := func(ctx context.Context, input int) (int, error) {
		return input, fmt.Errorf("error")
	}

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
