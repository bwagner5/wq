package wq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	StateInitialized = 0
	StateActive      = 1
	StateDraining    = 2
	StateStopping    = 3
	StateStopped     = 4
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
	// stateCode indicates the work queue's current mode
	stateCode uint8
	// currentState is the full State of the queue with all summary metrics
	currentState *Status
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
	// mu is a lock for initialization and State changes
	mu              sync.RWMutex
	inProgressItems atomic.Int64

	totalRetriesProcessed      atomic.Int64
	totalProcessingErrors      atomic.Int64
	totalSuccessfullyProcessed atomic.Int64
	totalConsumedResults       atomic.Int64
	totalItemsProcessed        int
	durationNanoSum            int
	maxLatency                 time.Duration
	minLatency                 time.Duration
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

type Status struct {
	State                      string
	StateCode                  int
	QueuedResults              int
	ResultsQueueSize           int
	QueuedWorkItems            int
	WorkItemQueueSize          int
	InProgressWorkItems        int
	TotalWorkItemsProcessed    int
	TotalProcessingErrors      int
	TotalRetries               int
	TotalSuccessfullyProcessed int
	TotalConsumedResults       int
	MaxLatency                 time.Duration
	MinLatency                 time.Duration
	AvgLatency                 time.Duration
}

// ProcessorFunc is a generic, user-provided processing func for the input items added to the queue
type ProcessorFunc[T, R any] func(context.Context, T) (R, error)

// New creates a concurrent work queue
func New[T, R any](concurrency, allowedRetries, inputSize, outputSize int, processor ProcessorFunc[T, R]) *Queue[T, R] {
	return &Queue[T, R]{
		stateCode:      StateInitialized,
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
	if wq.stateCode == StateActive || wq.stateCode == StateDraining {
		return nil
	}

	if wq.stateCode == StateDraining || wq.stateCode == StateStopping {
		return fmt.Errorf("work queue has an invalid State to start, %s: %w", wq.stateCodeDescription(wq.stateCode), UnableToStartErr)
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
	wq.stateCode = StateActive
	return nil
}

// executer wraps the user-provided processor func to handle work queue accounting like metadata, retries, and result packaging.
func (wq *Queue[T, R]) executer(ctx context.Context, workItem *WorkItem[T]) {
	wq.inProgressItems.Add(1)
	defer wq.inProgressItems.Add(-1)
	defer wq.workItemWaitGroup.Done()

	workItem.Metadata.Attempt++
	workItem.Metadata.StartTime = time.Now().UTC()
	output, err := wq.processor(ctx, workItem.Input)
	workItem.Metadata.EndTime = time.Now().UTC()
	workItem.Metadata.Latency = workItem.Metadata.EndTime.Sub(workItem.Metadata.StartTime)

	// lock to update latency stats
	wq.mu.Lock()
	if workItem.Metadata.Latency > wq.maxLatency {
		wq.maxLatency = workItem.Metadata.Latency
	}

	if workItem.Metadata.Latency < wq.minLatency || workItem.Metadata.Latency == 0 {
		wq.minLatency = workItem.Metadata.Latency
	}
	// we need to do this while locked so that when we compute avg latency, duration sum and total items are in-sync
	// since we're locking anyways, we're using regular ints rather than atomics.
	wq.durationNanoSum += int(workItem.Metadata.Latency)
	wq.totalItemsProcessed++
	wq.mu.Unlock()

	if workItem.Metadata.Attempt > 1 {
		wq.totalRetriesProcessed.Add(1)
	}

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
			wq.totalProcessingErrors.Add(1)
			select {
			case <-ctx.Done():
			case wq.outputQueue <- &Result[T, R]{
				Input:    workItem.Input,
				Output:   output,
				Metadata: workItem.Metadata,
				Error:    err,
			}:
			}

			return
		}
	}
	wq.totalSuccessfullyProcessed.Add(1)
	// If we get a success, return the Result with nil error
	select {
	case <-ctx.Done():
	case wq.outputQueue <- &Result[T, R]{
		Input:    workItem.Input,
		Output:   output,
		Metadata: workItem.Metadata,
		Error:    nil,
	}:
	}

}

// Add enqueues an item to the work queue for processing
func (wq *Queue[T, R]) Add(item T) error {
	wq.mu.RLock()
	defer wq.mu.RUnlock()
	if wq.stateCode != StateActive {
		return NotAcceptingWorkErr
	}

	select {
	case <-wq.wqCtx.Done():
		return NotAcceptingWorkErr
	default:
		wq.workItemWaitGroup.Add(1)
		select {
		case wq.inputQueue <- &WorkItem[T]{
			Input:    item,
			Metadata: &Metadata{},
		}:
		default:
			wq.workItemWaitGroup.Done()
			return WorkQueueFullErr
		}
	}
	return nil
}

// AddWithBackoff enqueues an item to the work queue for processing, but if the queue is full, wait and try again.
// A total of 3 attempts will be made:
//
//	1: right away like a normal Add()
//	2: after the initialDelay duration
//	3: 2x the initialDelay duration
//
// If the queue is still full, a WorkQueueFullErr is returned
func (wq *Queue[T, R]) AddWithBackOff(item T, initialDelay time.Duration, maxAttempts int) error {
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := wq.Add(item); err != nil {
			if err == WorkQueueFullErr {
				if attempt == maxAttempts {
					return WorkQueueFullErr
				}
				time.Sleep(time.Duration(attempt) * initialDelay)
				continue
			} else {
				return err
			}
		}
		return nil
	}
	return WorkQueueFullErr
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
	if wq.stateCode != StateDraining && wq.stateCode != StateActive {
		return
	}
	wq.workItemWaitGroup.Add(1)
	workItem.Metadata.Retries++
	wq.inputQueue <- workItem
}

// Result returns a queued result from processed work items.
// If the work queue is active but empty, Result() can block
// until new items are added and processed OR the work queue is Stopped.
// The second argument follows the "comma ok" pattern and signifies the results queue has been full drained and stopped.
func (wq *Queue[T, R]) Result() (*Result[T, R], bool) {
	output, ok := <-wq.outputQueue
	if !ok {
		return nil, false
	}
	wq.totalConsumedResults.Add(1)
	return output, true
}

// Results is a range-able iterator that returns processed work item results
func (wq *Queue[T, R]) Results() func(func(*Result[T, R]) bool) {
	return func(yield func(r *Result[T, R]) bool) {
		for {
			r, ok := wq.Result()
			if !ok || !yield(r) {
				return
			}
		}
	}
}

// Drain closes the work queue for new work, but the remaining work is processed, including any necessary retries.
// A successful Drain will transition from Active -> Draining -> Stopping -> Stopped
func (wq *Queue[T, R]) Drain(timeout time.Duration) error {
	wq.setStateCode(StateDraining)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-wq.workItemsDrained():
	case <-timer.C:
		return DrainTimeoutErr
	}

	wq.setStateCode(StateStopping)
	close(wq.inputQueue)

	select {
	case <-wq.processorsExited():
	case <-timer.C:
		return DrainTimeoutErr
	}

	close(wq.outputQueue)
	wq.setStateCode(StateStopped)
	return nil
}

// Stop will allow processors to finish their current work item, but will not allow additional work items to be processed.
// Retries are skipped if current processing fails.
// Work Items in the queue are also cleared, but Results are retained.
// The integer return is the number of work items not processed from the input queue.
// Make sure that the Processor func provided handles the received context.Context or Stop() could hang forever.
func (wq *Queue[T, R]) Stop() int {
	wq.setStateCode(StateStopping)

	close(wq.inputQueue)
	wq.cancel()
	wq.processorWaitGroup.Wait()
	close(wq.outputQueue)
	unprocessed := 0
	for range wq.inputQueue {
		unprocessed++
	}
	wq.setStateCode(StateStopped)
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

func (wq *Queue[T, R]) Status() Status {
	wq.mu.RLock()
	defer wq.mu.RUnlock()

	avgLatency := 0
	if wq.totalItemsProcessed > 0 {
		avgLatency = wq.durationNanoSum / wq.totalItemsProcessed
	}

	return Status{
		State:                      wq.stateCodeDescription(wq.stateCode),
		StateCode:                  int(wq.stateCode),
		QueuedResults:              len(wq.outputQueue),
		ResultsQueueSize:           wq.outputSize,
		QueuedWorkItems:            len(wq.inputQueue),
		WorkItemQueueSize:          wq.inputSize,
		InProgressWorkItems:        int(wq.inProgressItems.Load()),
		TotalWorkItemsProcessed:    int(wq.totalItemsProcessed),
		TotalProcessingErrors:      int(wq.totalProcessingErrors.Load()),
		TotalRetries:               int(wq.totalProcessingErrors.Load()),
		TotalSuccessfullyProcessed: int(wq.totalSuccessfullyProcessed.Load()),
		TotalConsumedResults:       int(wq.totalConsumedResults.Load()),
		MaxLatency:                 wq.maxLatency,
		MinLatency:                 wq.minLatency,
		AvgLatency:                 time.Duration(avgLatency),
	}
}

// StateCode returns the state code of the work queue
func (wq *Queue[T, R]) StateCode() uint8 {
	wq.mu.RLock()
	defer wq.mu.RUnlock()
	return wq.stateCode
}

// StateCodeDescription returns a string representation of the work queue State code
func (wq *Queue[T, R]) StateCodeDescription() string {
	return wq.stateCodeDescription(wq.StateCode())
}

func (wq *Queue[T, R]) setStateCode(State uint8) {
	wq.mu.Lock()
	defer wq.mu.Unlock()
	wq.stateCode = State
}

func (wq *Queue[T, R]) stateCodeDescription(state uint8) string {
	switch state {
	case StateInitialized:
		return fmt.Sprintf("%d: Initialized", StateInitialized)
	case StateActive:
		return fmt.Sprintf("%d: Active", StateActive)
	case StateDraining:
		return fmt.Sprintf("%d: Draining", StateDraining)
	case StateStopping:
		return fmt.Sprintf("%d: Stopping", StateStopping)
	case StateStopped:
		return fmt.Sprintf("%d: Stopped", StateStopped)
	}
	panic("UNKNOWN State")
}
