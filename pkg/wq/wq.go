package wq

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const (
	StatusInitialized = 0
	StatusActive      = 1
	StatusDraining    = 2
	StatusStopping    = 3
	StatusStopped     = 4
)

var (
	DrainTimeoutErr     = fmt.Errorf("timeout waiting for queue to drain")
	NotAcceptingWorkErr = fmt.Errorf("work queue is not Active so no work is being accepted")
	WorkQueueFullErr    = fmt.Errorf("work queue is full, try again soon or increase buffer sizes")
	UnableToStartErr    = fmt.Errorf("work queue cannot be started")
)

// Queue is a generic and parallel work queue that buffers inputs and outputs of a provided Processor function.
// Use wq.New() to instantiate the Queue.
type Queue[T, R any] struct {
	// status indicates the work queue's current mode
	status uint8
	// processor is a user-defined processing func
	processor ProcessorFunc[T, R]
	// concurrency is the number of processor loops running that consume from the input channel/queue
	concurrency int
	// allowedRetries is how many times a work item input can be requeued for a retry.
	// The Processor func must return an error for the work item to be considered for retry
	allowedRetries int
	// inputSize is the buffer size for the input channel/queue
	inputSize int
	// input is the channel/queue that stores pending work items for processing
	inputQueue chan *WorkItem[T]
	// outputSize is the buffer size for the ouput channel/queue
	outputSize int
	// output is the channel that stores processing results
	outputQueue chan *Result[T, R]
	// wqCtx is the work queue's context used for shutting down the queue.
	wqCtx context.Context
	// cancel is a context cancel func used for shutting down the work queue and processors
	cancel context.CancelFunc
	// processorWaitGroup tracks the concurrent processors and is used to make sure all processors exit gracefully on Stop()
	processorWaitGroup sync.WaitGroup
	// workItemWaitGroup tracks individual work items being processed
	// and is used for draining work items
	workItemWaitGroup sync.WaitGroup
	// mu is a lock for initialization and status changes
	mu sync.RWMutex
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

// ProcessorFunc is a generic, user-provided processing func for the input items added to the queue
type ProcessorFunc[T, R any] func(context.Context, T) (R, error)

// New creates a concurrent work queue
func New[T, R any](concurrency, allowedRetries, inputSize, outputSize int, processor ProcessorFunc[T, R]) *Queue[T, R] {
	return &Queue[T, R]{
		status:         StatusInitialized,
		processor:      processor,
		concurrency:    concurrency,
		allowedRetries: allowedRetries,
		inputSize:      inputSize,
		outputSize:     outputSize,
	}
}

// Start executes processors based on the concurrency configured from New()
// Alternatively, you can just call Add() to auto-start the work queue
func (wq *Queue[T, R]) Start(ctx context.Context) error {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	// if we're already active or draining, do nothing
	if wq.status == StatusActive || wq.status == StatusDraining {
		return nil
	}

	if wq.status == StatusDraining || wq.status == StatusStopping {
		return fmt.Errorf("work queue has an invalid status to start, %s: %w", wq.statusDescription(wq.status), UnableToStartErr)
	}

	wqCtx, cancel := context.WithCancel(ctx)
	wq.cancel = cancel
	wq.wqCtx = wqCtx

	if wq.outputSize > 0 {
		wq.outputQueue = make(chan *Result[T, R], wq.outputSize)
	}
	wq.inputQueue = make(chan *WorkItem[T], wq.inputSize)

	for i := 0; i < wq.concurrency; i++ {
		wq.processorWaitGroup.Add(1)
		go func(wqCtx context.Context) {
			defer wq.processorWaitGroup.Done()
			for {
				select {
				case <-wqCtx.Done():
					return
				case <-ctx.Done():
					wq.cancel()
					return
				case workItem, ok := <-wq.inputQueue:
					// input channel is fully drained and closed, so exit
					if !ok {
						return
					}
					wq.executer(wqCtx, workItem)
				}
			}
		}(wqCtx)
	}
	wq.status = StatusActive
	return nil
}

func (wq *Queue[T, R]) executer(ctx context.Context, workItem *WorkItem[T]) {
	defer wq.workItemWaitGroup.Done()
	workItem.Metadata.Attempt++
	workItem.Metadata.StartTime = time.Now().UTC()
	output, err := wq.processor(ctx, workItem.Input)
	workItem.Metadata.EndTime = time.Now().UTC()
	workItem.Metadata.Latency = workItem.Metadata.EndTime.Sub(workItem.Metadata.StartTime)

	// if we get an error from the user-provided processor, we may be able to:
	// Retry
	// OR
	// If all retries have been exhausted, we just return the error in the Result back to the user
	if err != nil {
		workItem.Metadata.Errors = append(workItem.Metadata.Errors, err)
		if workItem.Metadata.Retries < wq.allowedRetries {
			wq.retry(workItem)
			return
		} else {
			wq.outputQueue <- &Result[T, R]{
				Input:    workItem.Input,
				Output:   output,
				Metadata: workItem.Metadata,
				Error:    err,
			}
			return
		}
	}
	// If we get a success, return the Result with nil error
	wq.outputQueue <- &Result[T, R]{
		Input:    workItem.Input,
		Output:   output,
		Metadata: workItem.Metadata,
		Error:    nil,
	}
}

// Add enqueues an item to the work queue for processing
func (wq *Queue[T, R]) Add(item T) error {
	wq.mu.RLock()
	defer wq.mu.RUnlock()
	if wq.status != StatusActive {
		return NotAcceptingWorkErr
	}

	select {
	case <-wq.wqCtx.Done():
		return NotAcceptingWorkErr
	default:
		wq.workItemWaitGroup.Add(1)
		wq.inputQueue <- &WorkItem[T]{
			Input:    item,
			Metadata: &Metadata{},
		}
	}
	return nil
}

// MustAdd tries to Add an item to the work queue for processing, but if an error occurs, panics.
func (wq *Queue[T, R]) MustAdd(item T) {
	if err := wq.Add(item); err != nil {
		panic(err)
	}
}

func (wq *Queue[T, R]) retry(workItem *WorkItem[T]) {
	wq.mu.RLock()
	defer wq.mu.RUnlock()
	if wq.status != StatusDraining && wq.status != StatusActive {
		return
	}
	wq.workItemWaitGroup.Add(1)
	workItem.Metadata.Retries++
	wq.inputQueue <- workItem
}

// Result returns a queued result from processed work items
// If the work queue is active but empty, Result() can block
// until new items are added and processed OR the work queue is Stopped.
func (wq *Queue[T, R]) Result() (*Result[T, R], bool) {
	output, ok := <-wq.outputQueue
	if !ok {
		return nil, false
	}
	return output, true
}

// Drain closes the work queue for new work, but the remaining work is processed, including any necessary retries.
// A successful Drain will transition from Active -> Draining -> Stopping -> Stopped
func (wq *Queue[T, R]) Drain(timeout time.Duration) error {
	wq.setStatus(StatusDraining)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-wq.workItemsDrained():
	case <-timer.C:
		return DrainTimeoutErr
	}

	wq.setStatus(StatusStopping)
	close(wq.inputQueue)

	select {
	case <-wq.processorsExited():
	case <-timer.C:
		return DrainTimeoutErr
	}

	close(wq.outputQueue)
	wq.setStatus(StatusStopped)
	return nil
}

// Stop will allow processors to finish their current work item, but will not allow additional work items to be processed.
// Retries are skipped if current processing fails.
// Work Items in the queue are also cleared, but Results are retained.
// The integer return is the number of work items not processed from the input queue.
// Make sure that the Processor func provided handles the received context.Context or Stop() could hang forever.
func (wq *Queue[T, R]) Stop() int {
	wq.setStatus(StatusStopping)

	close(wq.inputQueue)
	wq.cancel()
	wq.processorWaitGroup.Wait()
	close(wq.outputQueue)
	unprocessed := 0
	for range wq.inputQueue {
		unprocessed++
	}
	wq.setStatus(StatusStopped)
	return unprocessed
}

func (wq *Queue[T, R]) workItemsDrained() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		wq.workItemWaitGroup.Wait()
		close(done)
	}()
	return done
}

func (wq *Queue[T, R]) processorsExited() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		wq.processorWaitGroup.Wait()
		close(done)
	}()
	return done
}

// Status returns the status code of the work queue
func (wq *Queue[T, R]) Status() uint8 {
	wq.mu.RLock()
	defer wq.mu.RUnlock()
	return wq.status
}

// StatusDescription returns a string representation of the work queue status
func (wq *Queue[T, R]) StatusDescription() string {
	return wq.statusDescription(wq.Status())
}

func (wq *Queue[T, R]) setStatus(status uint8) {
	wq.mu.Lock()
	defer wq.mu.Unlock()
	wq.status = status
}

func (wq *Queue[T, R]) statusDescription(status uint8) string {
	switch status {
	case StatusInitialized:
		return fmt.Sprintf("%d: Initialized", StatusInitialized)
	case StatusActive:
		return fmt.Sprintf("%d: Active", StatusInitialized)
	case StatusDraining:
		return fmt.Sprintf("%d: Draining", StatusDraining)
	case StatusStopping:
		return fmt.Sprintf("%d: Stopping", StatusStopping)
	case StatusStopped:
		return fmt.Sprintf("%d: Stopped", StatusStopped)
	}
	panic("UNKNOWN STATUS")
}
