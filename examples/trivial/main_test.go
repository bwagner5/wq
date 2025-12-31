package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRun(t *testing.T) {
	// simple execution test just to make sure everything runs without errors
	require.NoError(t, run(context.Background()))
}
