package wq

import (
	"context"
	"time"
)

// Queue is a generic and parallel work queue that buffers inputs and outputs of a provided Processor function.
// Use wq.New() to instantiate the Queue.
type Queue[T, R any] struct {
	processor      Processor[T, R]
	concurrency    int
	allowedRetries int
	inputSize      int
	input          chan WorkItem[T]
	processing     chan T
	outputSize     int
	output         chan *Result[T, R]
}

type Result[T, R any] struct {
	Input    T
	Output   R
	Error    error
	Metadata *Metadata
}

type WorkItem[T any] struct {
	Input    T
	Metadata *Metadata
}

// Metadata shows processing information
type Metadata struct {
	StartTime time.Time
	EndTime   time.Time
	Latency   time.Duration
	Attempt   int
	Retries   int
	// Errors shows all attempt errors
	Errors []error
	// worker ID?
}

// Processor is a generic, user-provided processing func for the input items added to the queue
type Processor[T, R any] func(T) (R, error)

// New creates a concurrent work queue
func New[T, R any](concurrency, allowedRetries, inputSize, outputSize int, processor Processor[T, R]) *Queue[T, R] {
	var output chan *Result[T, R]
	if outputSize > 0 {
		output = make(chan *Result[T, R], outputSize)
	}
	return &Queue[T, R]{
		input:  make(chan WorkItem[T], inputSize),
		output: output,
	}
}

// Start executes processors based on the concurrency configured from New()
// Alternatively, you can just call Add() to auto-start the work queue
func (wq Queue[T, R]) Start(ctx context.Context) {
	for i := 0; i < wq.concurrency; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case workItem := <-wq.input:
					workItem.Metadata.Attempt++
					workItem.Metadata.StartTime = time.Now().UTC()
					output, err := wq.processor(workItem.Input)
					workItem.Metadata.EndTime = time.Now().UTC()
					workItem.Metadata.Latency = workItem.Metadata.EndTime.Sub(workItem.Metadata.StartTime)

					// if we get an error from the user-provided processor, we may be able to:
					// Retry
					// OR
					// If all retries have been exhausted, we just return the error in the Result back to the user
					if err != nil {
						workItem.Metadata.Errors = append(workItem.Metadata.Errors, err)
						if wq.allowedRetries < workItem.Metadata.Retries {
							wq.retry(workItem)
							continue
						} else {
							wq.output <- &Result[T, R]{
								Input:    workItem.Input,
								Output:   output,
								Metadata: workItem.Metadata,
								Error:    err,
							}
							continue
						}
					}
					// If we get a success, return the Result with nil error
					wq.output <- &Result[T, R]{
						Input:    workItem.Input,
						Output:   output,
						Metadata: workItem.Metadata,
						Error:    nil,
					}
				}
			}
		}()
	}
}

// Stop drains and stops executing processors
func (wq Queue[T, R]) Stop(timeout time.Duration) {

}

// Clear deletes all items from the input queue.
// Items that are currently being processed continue to be processed.
func (wq Queue[T, R]) Clear() {

}

// Add enqueues an item to the work queue for processing
func (wq Queue[T, R]) Add(item T) {
	wq.input <- WorkItem[T]{
		Input:    item,
		Metadata: &Metadata{},
	}
}

func (wq Queue[T, R]) retry(workItem WorkItem[T]) {
	workItem.Metadata.Retries++
	wq.input <- workItem
}

func (wq Queue[T, R]) Result() *Result[T, R] {
	return <-wq.output
}
