package q

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"
)

const (
	StateInitialized = uint8(0)
	StateActive      = uint8(1)
	StateDraining    = uint8(2)
	StateStopping    = uint8(3)
	StateStopped     = uint8(4)
)

var (
	DrainTimeoutErr        = fmt.Errorf("timeout waiting for queue to drain")
	NotAcceptingWorkErr    = fmt.Errorf("work queue is not Active so no work is being accepted")
	WorkSimpleQueueFullErr = fmt.Errorf("work queue is full, try again soon or increase buffer sizes")
	UnableToStartErr       = fmt.Errorf("work queue cannot be started")
	// IgnoreResultErr can be used by a user provided processor func to instruct the queue to not place the result or error on a specific queue.
	// This allows some results to be ignored without configuring all results to be ignored at the queue level.
	IgnoreResultErr      = fmt.Errorf("ignore result")
	defaultResultsPolicy = ResultsPolicyQueue
	defaultRetryPolicy   = RetryPolicy{
		MaxRetries:        2,
		Strategy:          RetryStrategyExponentialBackoff,
		InitialRetryDelay: 1 * time.Second,
	}
)

// SimpleQueue is a generic and parallel work queue that buffers inputs and outputs of a provided Processor function.
// Use sq.New() to instantiate the SimpleQueue.
type SimpleQueue[T, R any] struct {
	// stateCode indicates the work queue's current mode
	stateCode uint8
	// stateEvents provides a mechanism for a consumer to listen for state changes in the queue.
	stateEvents chan uint8
	// inputQueue is the channel/queue that stores pending work items for processing
	inputQueue chan T
	// inputCloserOnce is used to make the channel closing idempotent between Drain() and Stop()
	inputCloserOnce sync.Once
	// resultsQueueSize is the channel that stores processing results
	resultsQueue chan R
	// resultsCloserOnce is used to make the channel closing idempotent between Drain() and Stop()
	resultsCloserOnce sync.Once
	// errorsQueue is the channel that stores processing errors
	errorsQueue chan error
	// errorsCloserOnce is used to make the channel closing idempotent between Drain() and Stop()
	errorsCloserOnce sync.Once
	// qCtx is the work queue's context used for shutting down the queue.
	qCtx context.Context
	// cancel is a context cancel func used for shutting down the work queue and processors
	cancel context.CancelFunc
	// processorWaitGroup tracks the concurrent processors and is used to make sure all processors exit gracefully on Stop()
	processorWaitGroup sync.WaitGroup
	// workItemWaitGroup tracks individual work items being processed
	// and is used for draining work items
	workItemWaitGroup sync.WaitGroup
	// mu is a lock for initialization and State changes
	mu sync.RWMutex

	options SimpleOptions[T, R]
}

type SimpleOptions[T, R any] struct {
	// Concurrency is the number of processor loops running that consume from the input channel/queue
	Concurrency int
	// InputQueueSize is the buffer size for the input channel/queue
	InputQueueSize int
	// ResultsQueueSize is the buffer size for the ouput channel/queue
	ResultsQueueSize int
	// ErrorsQueueSize is the buffer size for the errors channel/queue
	ErrorsQueueSize int
	// ResultsPolicy determines how to handle results from the processor func. This includes errors
	ResultsPolicy *ResultsPolicy
	// ProcessorFunc is a user-defined processing func
	ProcessorFunc ProcessorFunc[T, R]
}

type ResultsPolicy string

var (
	ResultsPolicyQueue = ResultsPolicy("QUEUE")
	ResultsPolicyDrop  = ResultsPolicy("DROP")
)

// ProcessorFunc is a generic, user-provided processing func for the input items added to the queue
type ProcessorFunc[T, R any] func(context.Context, T) (R, error)

// New creates a concurrent work queue with sane defaults
func NewSimpleQueue[T, R any](concurrency, maxRetries, queueSize int, processor ProcessorFunc[T, R]) *SimpleQueue[T, R] {
	options := SimpleOptions[T, R]{
		Concurrency:      concurrency,
		ProcessorFunc:    processor,
		ResultsPolicy:    &ResultsPolicyQueue,
		InputQueueSize:   queueSize,
		ResultsQueueSize: queueSize,
		ErrorsQueueSize:  queueSize,
	}
	return NewFromSimpleOptions(options)
}

// NewFromSimpleOptions creates a concurrent simple work queue with a custom configuration
func NewFromSimpleOptions[T, R any](options SimpleOptions[T, R]) *SimpleQueue[T, R] {
	return &SimpleQueue[T, R]{
		stateCode: StateInitialized,
		options:   setDefaultSimpleOptions(options),
	}
}

func setDefaultSimpleOptions[T, R any](options SimpleOptions[T, R]) SimpleOptions[T, R] {
	if options.ResultsPolicy == nil {
		options.ResultsPolicy = &defaultResultsPolicy
	}
	return options
}

// Collect can be used for casual concurrent processing use-cases where the data is in a slice and you want a simple and terse
// call to process the data and get results.
// There is no need to call Start(), Stop(), or Drain() when using Collect().
// If the user-provided processor func returns an error, the error is dropped and no result is added to the results slice.
// If you need to process item errors, use the normal queue setup with individual error and results streams.
// The concurrency is set using runtime.NumCPU().
func Collect[T, R any](ctx context.Context, data []T, processor ProcessorFunc[T, R]) ([]R, error) {
	queue := NewFromSimpleOptions(SimpleOptions[T, R]{
		Concurrency:      runtime.NumCPU(),
		ProcessorFunc:    processor,
		ResultsPolicy:    &ResultsPolicyQueue,
		InputQueueSize:   len(data),
		ResultsQueueSize: len(data),
		ErrorsQueueSize:  len(data),
	})
	if err := queue.Start(ctx); err != nil {
		return nil, err
	}
	defer queue.Stop()

	results := make([]R, 0, len(data))

	for _, d := range data {
		if err := queue.Add(d); err != nil {
			return nil, err
		}
	}
	if err := queue.Drain(time.Nanosecond * math.MaxInt64); err != nil {
		return nil, err
	}

	for r := range queue.Results() {
		results = append(results, *r)
	}
	return results, nil
}

// Start executes processors based on the queue options
func (wq *SimpleQueue[T, R]) Start(ctx context.Context) error {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	// if we're already active or draining, do nothing
	if wq.stateCode == StateActive || wq.stateCode == StateDraining {
		return nil
	}

	if wq.stateCode == StateDraining || wq.stateCode == StateStopping {
		return fmt.Errorf("simple work queue has an invalid State to start, %s: %w", wq.stateCodeDescription(wq.stateCode), UnableToStartErr)
	}

	wqCtx, cancel := context.WithCancel(ctx)
	wq.cancel = cancel
	wq.qCtx = wqCtx

	if wq.options.ResultsQueueSize > 0 {
		wq.resultsQueue = make(chan R, wq.options.ResultsQueueSize)
	} else {
		wq.resultsQueue = make(chan R)
	}
	wq.inputQueue = make(chan T, wq.options.InputQueueSize)
	wq.errorsQueue = make(chan error, wq.options.ErrorsQueueSize)

	for i := 0; i < wq.options.Concurrency; i++ {
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
func (wq *SimpleQueue[T, R]) executer(ctx context.Context, workItem T) {
	defer wq.workItemWaitGroup.Done()

	output, err := wq.options.ProcessorFunc(ctx, workItem)
	if err == IgnoreResultErr {
		return
	}
	wq.sendResult(ctx, output, err)
}

func (wq *SimpleQueue[T, R]) sendResult(ctx context.Context, output R, err error) {
	if wq.options.ResultsQueueSize <= 0 {
		return
	}

	// send errors
	if err != nil {
		switch string(*wq.options.ResultsPolicy) {
		case string(ResultsPolicyQueue):
			select {
			case <-ctx.Done():
			case wq.errorsQueue <- err:
			}
		case string(ResultsPolicyDrop):
			select {
			case wq.errorsQueue <- err:
			default:
			}
		default:
			panic("invalid ResultsPolicy")
		}
		return
	}

	// send results
	switch string(*wq.options.ResultsPolicy) {
	case string(ResultsPolicyQueue):
		select {
		case <-ctx.Done():
		case wq.resultsQueue <- output:
		}
	case string(ResultsPolicyDrop):
		select {
		case wq.resultsQueue <- output:
		default:
		}
	default:
		panic("invalid ResultsPolicy")
	}
}

func (wq *SimpleQueue[T, R]) add(workItem T) error {
	select {
	case <-wq.qCtx.Done():
		return NotAcceptingWorkErr
	default:
		wq.workItemWaitGroup.Add(1)
		select {
		case wq.inputQueue <- workItem:
		default:
			wq.workItemWaitGroup.Done()
			return WorkSimpleQueueFullErr
		}
	}
	return nil
}

// Add enqueues an item to the work queue for processing if the queue is in an Active state.
func (wq *SimpleQueue[T, R]) Add(item T) error {
	wq.mu.RLock()
	defer wq.mu.RUnlock()
	if wq.stateCode != StateActive {
		return NotAcceptingWorkErr
	}

	return wq.add(item)
}

// ForceAdd enqueues an item to the work queue for processing if the queue is Active OR Draining.
func (wq *SimpleQueue[T, R]) ForceAdd(item T) error {
	wq.mu.RLock()
	defer wq.mu.RUnlock()
	if wq.stateCode != StateActive && wq.stateCode != StateDraining {
		return NotAcceptingWorkErr
	}

	return wq.add(item)
}

// AddWithBackoff enqueues an item to the work queue for processing, but if the queue is full, wait and try again.
// A total of maxAttempts will be made:
//
//		1: right away like a normal Add()
//		2: after the initialDelay duration
//		3: 2x the initialDelay duration
//	    ...
//
// If the queue is still full, a WorkSimpleQueueFullErr is returned
func (wq *SimpleQueue[T, R]) AddWithBackOff(item T, initialDelay time.Duration, maxAttempts int) error {
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := wq.Add(item); err != nil {
			if err == WorkSimpleQueueFullErr {
				if attempt == maxAttempts {
					return WorkSimpleQueueFullErr
				}
				time.Sleep(time.Duration(attempt) * initialDelay)
				continue
			} else {
				return err
			}
		}
		return nil
	}
	return WorkSimpleQueueFullErr
}

// MustAdd tries to Add an item to the work queue for processing, but if an error occurs, panics.
func (wq *SimpleQueue[T, R]) MustAdd(item T) {
	if err := wq.Add(item); err != nil {
		panic(err)
	}
}

// Result returns a queued result from processed work items.
// If the work queue is active but empty, Result() can block
// until new items are added and processed OR the work queue is Stopped.
// The second argument follows the "comma ok" pattern and signifies the results queue has been full drained and stopped.
func (wq *SimpleQueue[T, R]) Result() (*R, bool) {
	if wq.options.ResultsQueueSize <= 0 {
		return nil, false
	}
	output, ok := <-wq.resultsQueue
	if !ok {
		return nil, false
	}
	return &output, true
}

// Results is a range iterator that returns processed work item results
func (wq *SimpleQueue[T, R]) Results() func(func(*R) bool) {
	return func(yield func(r *R) bool) {
		for {
			r, ok := wq.Result()
			if !ok || !yield(r) {
				return
			}
		}
	}
}

// Error returns a queued error from processed work items.
// If the work queue is active but empty, Error() can block
// until new items are added and processed OR the work queue is Stopped.
// The second argument follows the "comma ok" pattern and signifies the errors queue has been full drained and stopped.
func (wq *SimpleQueue[T, R]) Error() (error, bool) {
	if wq.options.ErrorsQueueSize <= 0 {
		return nil, false
	}
	err, ok := <-wq.errorsQueue
	if !ok {
		return nil, false
	}
	return err, true
}

// Errors is a range iterator that returns processed work item errors
func (wq *SimpleQueue[T, R]) Errors() func(func(error) bool) {
	return func(yield func(err error) bool) {
		for {
			err, ok := wq.Error()
			if !ok || !yield(err) {
				return
			}
		}
	}
}

// Drain closes the work queue for new work, but the remaining work is processed, including any necessary retries.
// A successful Drain will transition from Active -> Draining -> Stopping -> Stopped.
// Drain is blocking until the work queue is empty or the timeout elapses.
func (wq *SimpleQueue[T, R]) Drain(timeout time.Duration) error {
	wq.setStateCode(StateDraining)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-wq.workItemsDrained():
	case <-timer.C:
		return DrainTimeoutErr
	}

	wq.setStateCode(StateStopping)
	wq.closeInputQueue()

	select {
	case <-wq.processorsExited():
	case <-timer.C:
		return DrainTimeoutErr
	}

	wq.closeResultsQueue()
	wq.closeErrorsQueue()
	wq.setStateCode(StateStopped)
	return nil
}

// Stop will allow processors to finish their current work item, but will not allow additional work items to be processed.
// Retries are skipped if current processing fails.
// Work Items in the queue are also cleared, but Results are retained.
// The integer return is the number of work items not processed from the input queue.
// Make sure that the Processor func provided handles the received context.Context or Stop() could hang forever.
func (wq *SimpleQueue[T, R]) Stop() int {
	wq.setStateCode(StateStopping)

	wq.closeInputQueue()
	wq.cancel()
	wq.processorWaitGroup.Wait()
	wq.closeResultsQueue()
	wq.closeErrorsQueue()
	unprocessed := 0
	for range wq.inputQueue {
		unprocessed++
	}
	wq.setStateCode(StateStopped)
	return unprocessed
}

func (wq *SimpleQueue[T, R]) closeInputQueue() {
	wq.inputCloserOnce.Do(func() {
		close(wq.inputQueue)
	})
}

func (wq *SimpleQueue[T, R]) closeResultsQueue() {
	wq.resultsCloserOnce.Do(func() {
		close(wq.resultsQueue)
	})
}

func (wq *SimpleQueue[T, R]) closeErrorsQueue() {
	wq.errorsCloserOnce.Do(func() {
		close(wq.errorsQueue)
	})
}

func (wq *SimpleQueue[T, R]) workItemsDrained() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		wq.workItemWaitGroup.Wait()
		close(done)
	}()
	return done
}

func (wq *SimpleQueue[T, R]) processorsExited() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		wq.processorWaitGroup.Wait()
		close(done)
	}()
	return done
}

// StateCode returns the state code of the work queue
func (wq *SimpleQueue[T, R]) StateCode() uint8 {
	wq.mu.RLock()
	defer wq.mu.RUnlock()
	return wq.stateCode
}

// StateCodeDescription returns a string representation of the work queue State code
func (wq *SimpleQueue[T, R]) StateCodeDescription() string {
	return wq.stateCodeDescription(wq.StateCode())
}

func (wq *SimpleQueue[T, R]) setStateCode(State uint8) {
	wq.mu.Lock()
	defer wq.mu.Unlock()
	wq.stateCode = State
}

func (wq *SimpleQueue[T, R]) stateCodeDescription(state uint8) string {
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
