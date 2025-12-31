package q

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Queue is a generic and parallel work queue that buffers inputs and outputs of a provided Processor function.
// Use q.New() to instantiate the Queue.
type Queue[T, R any] struct {
	// squeue is the backing simple queue
	squeue *SimpleQueue[WorkItem[T], Result[T, R]]

	// mu is a lock for initialization and State changes
	mu sync.RWMutex

	processorFunc ProcessorFunc[WorkItem[T], Result[T, R]]

	options Options[T, R]

	// status metrics:

	inProgressItems            atomic.Int64
	totalRetriesProcessed      atomic.Int64
	totalProcessingErrors      atomic.Int64
	totalSuccessfullyProcessed atomic.Int64
	totalConsumedResults       atomic.Int64
	totalConsumedErrors        atomic.Int64
	totalItemsProcessed        int
	durationNanoSum            int
	maxLatency                 time.Duration
	minLatency                 time.Duration
}

type Options[T, R any] struct {
	// Concurrency is the number of processor loops running that consume from the input channel/queue
	Concurrency int
	// InputQueueSize is the buffer size for the input channel/queue
	InputQueueSize int
	// ResultsQueueSize is the buffer size for the ouput channel/queue
	ResultsQueueSize int
	// ErrorsQueueSize is the buffer size for the errors channel/queue
	ErrorsQueueSize int
	// ResultsPolicy determines how to handle results from the processor func
	ResultsPolicy *ResultsPolicy

	RetryPolicy *RetryPolicy
	// ProcessorFunc is a user-defined processing func
	ProcessorFunc ProcessorFunc[T, R]
}

type RetryStrategy string

var (
	RetryStrategyExponentialBackoff = RetryStrategy("EXPONENTIAL BACKOFF")
	RetryStrategyImmediateRequeue   = RetryStrategy("IMMEDIATE REQUEUE")
)

type RetryPolicy struct {
	// MaxRetries is how many times a work item input can be requeued for a retry.
	// The Processor func must return an error for the work item to be considered for retry
	MaxRetries int
	// Strategy determines how retries are requeued for reprocessing
	Strategy RetryStrategy
	// InitialRetryDelay is the time to wait before retrying on the first attempt.
	// The Strategy will affect they delay after the first retry, but still based on the initial delay.
	InitialRetryDelay time.Duration
}

type Result[T, R any] struct {
	Input    T
	Output   R
	Err      error
	Metadata *Metadata
}

type ResultErr[T, R any] struct {
	result Result[T, R]
}

func (r ResultErr[T, R]) Error() string {
	return r.result.Err.Error()
}

type WorkItem[T any] struct {
	Input    T
	Metadata *Metadata
}

// Metadata shows processing information
type Metadata struct {
	startTime time.Time
	endTime   time.Time
	latency   time.Duration
	attempt   int
	retries   int
	// Errors shows all attempt errors
	errors []error
	// worker ID?
}

func (m Metadata) StartTime() time.Time {
	return m.startTime
}

func (m Metadata) EndTime() time.Time {
	return m.endTime
}

func (m Metadata) Latency() time.Duration {
	return m.latency
}

func (m Metadata) Attempt() int {
	return m.attempt
}

func (m Metadata) Retries() int {
	return m.retries
}

func (m Metadata) Errors() []error {
	return m.errors
}

type Status struct {
	State                      string
	StateCode                  uint8
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

// New creates a concurrent work queue with sane defaults
func New[T, R any](concurrency, maxRetries, queueSize int, processor ProcessorFunc[T, R]) *Queue[T, R] {
	options := Options[T, R]{
		Concurrency:   concurrency,
		ProcessorFunc: processor,
		RetryPolicy: &RetryPolicy{
			MaxRetries: maxRetries,
			Strategy:   RetryStrategyExponentialBackoff,
		},
		ResultsPolicy:    &ResultsPolicyQueue,
		InputQueueSize:   queueSize,
		ResultsQueueSize: queueSize,
		ErrorsQueueSize:  queueSize,
	}
	return NewFromOptions(options)
}

// NewFromOptions creates a concurrent work queue with a custom configuration
func NewFromOptions[T, R any](options Options[T, R]) *Queue[T, R] {
	queue := &Queue[T, R]{
		options: setDefaultOptions(options),
	}
	queue.squeue = NewFromSimpleOptions(SimpleOptions[WorkItem[T], Result[T, R]]{
		Concurrency:      options.Concurrency,
		ProcessorFunc:    ProcessorFunc[WorkItem[T], Result[T, R]](queue.executer(ProcessorFunc[T, R](options.ProcessorFunc))),
		ResultsPolicy:    (*ResultsPolicy)(options.ResultsPolicy),
		InputQueueSize:   options.InputQueueSize,
		ResultsQueueSize: options.ResultsQueueSize,
		ErrorsQueueSize:  options.ErrorsQueueSize,
	})
	return queue
}

func setDefaultOptions[T, R any](options Options[T, R]) Options[T, R] {
	if options.ResultsPolicy == nil {
		options.ResultsPolicy = &DefaultResultsPolicy
	}
	if options.RetryPolicy == nil {
		options.RetryPolicy = &DefaultRetryPolicy
	}
	return options
}

// Start executes processors based on the queue options
func (q *Queue[T, R]) Start(ctx context.Context) error {
	return q.squeue.Start(ctx)
}

// executer wraps the user-provided processor func to handle work queue accounting like metadata, retries, and result packaging.
func (q *Queue[T, R]) executer(processorFunc ProcessorFunc[T, R]) ProcessorFunc[WorkItem[T], Result[T, R]] {
	return func(ctx context.Context, workItem WorkItem[T]) (Result[T, R], error) {
		q.inProgressItems.Add(1)
		defer q.inProgressItems.Add(-1)

		workItem.Metadata.attempt++
		workItem.Metadata.startTime = time.Now().UTC()
		output, err := processorFunc(ctx, workItem.Input)
		workItem.Metadata.retries = workItem.Metadata.attempt - 1
		workItem.Metadata.endTime = time.Now().UTC()
		workItem.Metadata.latency = workItem.Metadata.endTime.Sub(workItem.Metadata.startTime)
		if err != nil {
			workItem.Metadata.errors = append(workItem.Metadata.errors, err)
		}

		// lock to update latency stats
		q.mu.Lock()
		if workItem.Metadata.latency > q.maxLatency {
			q.maxLatency = workItem.Metadata.latency
		}

		if workItem.Metadata.latency < q.minLatency || q.minLatency == 0 {
			q.minLatency = workItem.Metadata.latency
		}
		// we need to do this while locked so that when we compute avg latency, duration sum and total items are in-sync
		// since we're locking anyways, we're using regular ints rather than atomics.
		q.durationNanoSum += int(workItem.Metadata.latency)
		q.totalItemsProcessed++
		q.mu.Unlock()

		if workItem.Metadata.attempt > 1 {
			q.totalRetriesProcessed.Add(1)
		}

		// if we get an error from the user-provided processor, we may be able to:
		// Retry
		// OR
		// If all retries have been exhausted, we just return the error in the Result back to the user
		if err != nil {
			if workItem.Metadata.retries < q.options.RetryPolicy.MaxRetries {
				switch q.options.RetryPolicy.Strategy {
				case RetryStrategyImmediateRequeue:
					q.retry(&workItem, 0)
				case RetryStrategyExponentialBackoff:
					q.retry(&workItem, q.options.RetryPolicy.InitialRetryDelay<<time.Duration(workItem.Metadata.retries+1))
				}
				return Result[T, R]{}, IgnoreResultErr
			} else {
				q.totalProcessingErrors.Add(1)
				return Result[T, R]{}, &ResultErr[T, R]{
					result: Result[T, R]{
						Input:    workItem.Input,
						Err:      err,
						Metadata: workItem.Metadata,
					},
				}
			}
		}

		q.totalSuccessfullyProcessed.Add(1)
		return Result[T, R]{
			Input:    workItem.Input,
			Output:   output,
			Err:      nil,
			Metadata: workItem.Metadata,
		}, nil
	}
}

// retry is a little awkward since it reaches into the internals of the SimpleQueue to be able to add items during Draining.
// A ForceAdd() could be exposed in SimpleQueue that allows adding during Draining...
func (q *Queue[T, R]) retry(workItem *WorkItem[T], delay time.Duration) {
	q.squeue.workItemWaitGroup.Add(1)
	go func() {
		timer := time.NewTimer(delay)
		defer q.squeue.workItemWaitGroup.Done()
		defer timer.Stop()
		select {
		case <-q.squeue.qCtx.Done():
			return
		case <-timer.C:
			if err := q.squeue.ForceAdd(*workItem); err != nil {
				// if the input queue is full, try again after waiting
				if err == WorkSimpleQueueFullErr {
					q.retry(workItem, q.options.RetryPolicy.InitialRetryDelay)
				}
			}
		}
	}()
}

// Add enqueues an item to the work queue for processing
func (q *Queue[T, R]) Add(item T) error {
	return q.squeue.Add(WorkItem[T]{
		Input:    item,
		Metadata: &Metadata{},
	})
}

// AddWithBackoff enqueues an item to the work queue for processing, but if the queue is full, wait and try again.
// A total of 3 attempts will be made:
//
//	1: right away like a normal Add()
//	2: after the initialDelay duration
//	3: 2x the initialDelay duration
//
// If the queue is still full, a WorkQueueFullErr is returned
func (q *Queue[T, R]) AddWithBackOff(item T, initialDelay time.Duration, maxAttempts int) error {
	return q.squeue.AddWithBackOff(WorkItem[T]{
		Input:    item,
		Metadata: &Metadata{},
	}, initialDelay, maxAttempts)
}

// MustAdd tries to Add an item to the work queue for processing, but if an error occurs, panics.
func (q *Queue[T, R]) MustAdd(item T) {
	if err := q.Add(item); err != nil {
		panic(err)
	}
}

// Result returns a queued result from processed work items.
// If the work queue is active but empty, Result() can block
// until new items are added and processed OR the work queue is Stopped.
// The second argument follows the "comma ok" pattern and signifies the results queue has been full drained and stopped.
func (q *Queue[T, R]) Result() (*Result[T, R], bool) {
	r, ok := q.squeue.Result()
	if !ok {
		return nil, false
	}
	q.totalConsumedResults.Add(1)
	return r, true
}

// Results is a range iterator that returns processed work item results
func (q *Queue[T, R]) Results() func(func(*Result[T, R]) bool) {
	return func(yield func(r *Result[T, R]) bool) {
		for {
			r, ok := q.Result()
			if !ok || !yield(r) {
				return
			}
		}
	}
}

// Error returns a queued error from processed work items.
// If the work queue is active but empty, Error() can block
// until new items are added and processed OR the work queue is Stopped.
// The second argument follows the "comma ok" pattern and signifies the error queue has been full drained and stopped.
func (q *Queue[T, R]) Error() (error, bool) {
	r, ok := q.squeue.Error()
	if !ok {
		return nil, false
	}
	q.totalConsumedErrors.Add(1)
	return r, true
}

// Errors is a range iterator that returns processed work item errors
func (q *Queue[T, R]) Errors() func(func(error) bool) {
	return func(yield func(err error) bool) {
		for {
			err, ok := q.Error()
			if !ok || !yield(err) {
				return
			}
		}
	}
}

// Drain closes the work queue for new work, but the remaining work is processed, including any necessary retries.
// A successful Drain will transition from Active -> Draining -> Stopping -> Stopped.
// Drain is blocking until the work queue is empty or the timeout elapses.
func (q *Queue[T, R]) Drain(timeout time.Duration) error {
	return q.squeue.Drain(timeout)
}

// Stop will allow processors to finish their current work item, but will not allow additional work items to be processed.
// Retries are skipped if current processing fails.
// Work Items in the queue are also cleared, but Results are retained.
// The integer return is the number of work items not processed from the input queue.
// Make sure that the Processor func provided handles the received context.Context or Stop() could hang forever.
func (q *Queue[T, R]) Stop() int {
	return q.squeue.Stop()
}

func (q *Queue[T, R]) Status() Status {
	q.mu.RLock()
	defer q.mu.RUnlock()

	avgLatency := 0
	if q.totalItemsProcessed > 0 {
		avgLatency = q.durationNanoSum / q.totalItemsProcessed
	}

	return Status{
		State:                      q.squeue.stateCodeDescription(q.squeue.stateCode),
		StateCode:                  q.squeue.stateCode,
		QueuedResults:              len(q.squeue.resultsQueue),
		ResultsQueueSize:           q.options.ResultsQueueSize,
		QueuedWorkItems:            len(q.squeue.inputQueue),
		WorkItemQueueSize:          q.options.InputQueueSize,
		InProgressWorkItems:        int(q.inProgressItems.Load()),
		TotalWorkItemsProcessed:    int(q.totalItemsProcessed),
		TotalProcessingErrors:      int(q.totalProcessingErrors.Load()),
		TotalRetries:               int(q.totalProcessingErrors.Load()),
		TotalSuccessfullyProcessed: int(q.totalSuccessfullyProcessed.Load()),
		TotalConsumedResults:       int(q.totalConsumedResults.Load()),
		MaxLatency:                 q.maxLatency,
		MinLatency:                 q.minLatency,
		AvgLatency:                 time.Duration(avgLatency),
	}
}

// StateCode returns the state code of the work queue
func (q *Queue[T, R]) StateCode() uint8 {
	return q.squeue.StateCode()
}

// StateCodeDescription returns a string representation of the work queue State code
func (q *Queue[T, R]) StateCodeDescription() string {
	return q.squeue.StateCodeDescription()
}
