package event

import (
	"fmt"
	"sync"
	"time"

	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/The-Pocket/PocketFlow/go/event/flow"
	"github.com/The-Pocket/PocketFlow/go/logger"
	"github.com/The-Pocket/PocketFlow/go/semantic"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// Runner encapsulates all the components needed to run PocketFlow
// applications, providing a simplified API for setting up and executing flows.
type Runner struct {
	stateStore   semantic.StateStore
	flowRegistry core.FlowRegistry
	publisher    core.EventPublisher
	subscriber   message.Subscriber
	// Separate router for each flow execution
	flowRouters   map[string]*FlowEventRouter
	nodeWorkers   map[string]core.NodeWorker
	options       RunnerOptions
	completeChans map[string]chan struct{}
	muCompChans   sync.Mutex
	muFlowRouters sync.RWMutex
	useRedis      bool
	redisAddr     string
}

// RunnerOptions configures the behavior of a Runner
type RunnerOptions struct {
	// Database URL for state store (default: ":memory:" for SQLite)
	DatabaseURL string

	// Event handlers
	OnFlowCompleted func(r *Runner, msg core.FlowCompletedMessage) error
	OnFlowFailed    func(r *Runner, msg core.FlowFailedMessage) error
	OnProgress      func(core.ProgressUpdateMessage) error

	// Debug mode enables more verbose logging
	DebugMode bool
}

// DefaultOptions returns sensible default options for a Runner
func DefaultOptions() RunnerOptions {
	return RunnerOptions{
		DatabaseURL: ":memory:",
		DebugMode:   false,
		// Default handlers
		OnFlowCompleted: func(r *Runner, msg core.FlowCompletedMessage) error {
			log.Info().Str("flowExecutionID", msg.FlowExecutionID).Msg("🎉 Flow completed successfully!")
			return nil
		},
		OnFlowFailed: func(r *Runner, msg core.FlowFailedMessage) error {
			log.Error().Str("flowExecutionID", msg.FlowExecutionID).Str("errorMessage", msg.ErrorMessage).Msg("❌ Flow failed!")
			return nil
		},
		OnProgress: func(msg core.ProgressUpdateMessage) error {
			log.Info().
				Str("status", msg.Status).
				Float64("progress", msg.Progress).
				Str("message", msg.Message).
				Msg("📊 Progress update")
			return nil
		},
	}
}

// Option is a function that configures RunnerOptions
type Option func(*RunnerOptions)

// WithDatabaseURL configures the database URL
func WithDatabaseURL(url string) Option {
	return func(o *RunnerOptions) {
		o.DatabaseURL = url
	}
}

// WithFlowCompletedHandler configures the flow completion handler
func WithFlowCompletedHandler(handler func(r *Runner, msg core.FlowCompletedMessage) error) Option {
	return func(o *RunnerOptions) {
		o.OnFlowCompleted = handler
	}
}

// WithFlowFailedHandler configures the flow failure handler
func WithFlowFailedHandler(handler func(r *Runner, msg core.FlowFailedMessage) error) Option {
	return func(o *RunnerOptions) {
		o.OnFlowFailed = handler
	}
}

// WithProgressHandler configures the progress handler
func WithProgressHandler(handler func(core.ProgressUpdateMessage) error) Option {
	return func(o *RunnerOptions) {
		o.OnProgress = handler
	}
}

// WithDebugMode enables or disables debug mode
func WithDebugMode(enabled bool) Option {
	return func(o *RunnerOptions) {
		o.DebugMode = enabled
	}
}

// NewRunner creates a new PocketFlow runner with the given options
func NewRunner(opts ...Option) *Runner {
	options := DefaultOptions()

	// Apply provided options
	for _, opt := range opts {
		opt(&options)
	}

	return &Runner{
		options:       options,
		completeChans: make(map[string]chan struct{}),
		flowRouters:   make(map[string]*FlowEventRouter),
		useRedis:      false,
	}
}

// NewRunnerWithRedis creates a new runner with Redis Streams messaging
func NewRunnerWithRedis(redisAddr string, opts ...Option) *Runner {
	options := DefaultOptions()

	// Apply provided options
	for _, opt := range opts {
		opt(&options)
	}

	return &Runner{
		options:       options,
		completeChans: make(map[string]chan struct{}),
		flowRouters:   make(map[string]*FlowEventRouter),
		useRedis:      true,
		redisAddr:     redisAddr,
	}
}

// Init initializes the runner with all necessary components
func (r *Runner) Init() error {
	// Initialize logger
	logger.Get() // Ensure logger is initialized
	log.Info().Msg("Initializing PocketFlow runner")

	// Initialize state store
	log.Debug().Str("databaseURL", r.options.DatabaseURL).Msg("Creating state store")
	r.stateStore = semantic.NewLayeredStateStore()
	log.Info().Msg("State store initialized")

	// Create flow registry
	r.flowRegistry = flow.NewInMemoryFlowRegistry()
	log.Info().Msg("Flow registry created")

	// Set up publisher based on messaging type
	var subscriber message.Subscriber
	if r.useRedis {
		log.Info().Str("redisAddr", r.redisAddr).Msg("Setting up Redis Streams publisher")
		redisRouter := NewWatermillEventRouterWithRedis(r.redisAddr, nil)
		r.publisher = NewWatermillPublisher(redisRouter.Publisher)
		subscriber = redisRouter.Subscriber
	} else {
		log.Info().Msg("Setting up in-memory publisher")
		router := NewWatermillEventRouter(nil)
		r.publisher = NewWatermillPublisher(router.Publisher)
		subscriber = router.Subscriber
	}
	log.Info().Msg("Event publisher initialized")

	// Store subscriber for creating flow routers
	r.subscriber = subscriber

	return nil
}

// wrapFlowCompletionHandler wraps the user-provided completion handler
// to also signal any waiting goroutines
func (r *Runner) wrapFlowCompletionHandler() func(core.FlowCompletedMessage) error {
	return func(msg core.FlowCompletedMessage) error {
		// Call user handler
		if r.options.OnFlowCompleted != nil {
			if err := r.options.OnFlowCompleted(r, msg); err != nil {
				log.Error().Err(err).Msg("Error in flow completion handler")
			}
		}

		// Signal completion
		r.muCompChans.Lock()
		if ch, ok := r.completeChans[msg.FlowExecutionID]; ok {
			ch <- struct{}{}
			delete(r.completeChans, msg.FlowExecutionID)
		}
		r.muCompChans.Unlock()

		return nil
	}
}

// wrapFlowFailureHandler wraps the user-provided failure handler
// to also signal any waiting goroutines
func (r *Runner) wrapFlowFailureHandler() func(core.FlowFailedMessage) error {
	return func(msg core.FlowFailedMessage) error {
		// Call user handler
		if r.options.OnFlowFailed != nil {
			if err := r.options.OnFlowFailed(r, msg); err != nil {
				log.Error().Err(err).Msg("Error in flow failure handler")
			}
		}

		// Signal completion (with failure)
		r.muCompChans.Lock()
		if ch, ok := r.completeChans[msg.FlowExecutionID]; ok {
			ch <- struct{}{}
			delete(r.completeChans, msg.FlowExecutionID)
		}
		r.muCompChans.Unlock()

		return nil
	}
}

// RegisterNodeWorker registers a node worker with the router
func (r *Runner) RegisterNodeWorker(worker core.NodeWorker) *Runner {
	r.nodeWorkers[worker.NodeType()] = worker
	return r
}

// RegisterNodeWorkers registers multiple node workers with the router
func (r *Runner) RegisterNodeWorkers(workers ...core.NodeWorker) *Runner {
	for _, worker := range workers {
		r.nodeWorkers[worker.NodeType()] = worker
	}
	return r
}

// RegisterFlow registers a flow with the registry and creates a worker for it
func (r *Runner) RegisterFlow(flow core.Flow) *Runner {
	// Register flow with registry
	r.flowRegistry.RegisterFlow(flow.ID(), flow)

	// Create and register a flow worker
	flowRouter, err := NewFlowEventRouter(flow, r.stateStore, r.publisher, r.subscriber, r.nodeWorkers, nil)
	if err != nil {
		log.Fatal().Err(err).Msg("Error creating flow router")
	}
	r.flowRouters[flow.Type()] = flowRouter

	return r
}

// RunFlow executes a flow and returns its execution ID
func (r *Runner) RunFlow(flow core.Flow, initialData map[string]interface{}) string {
	// Ensure we have some initial data
	if initialData == nil {
		initialData = map[string]interface{}{}
	}

	// Add start timestamp if not present
	if _, ok := initialData["started_at"]; !ok {
		initialData["started_at"] = time.Now().Format(time.RFC3339)
	}

	// Generate execution ID
	flowExecutionID := uuid.New().String()

	// Publish flow start request
	r.publisher.Publish(
		fmt.Sprintf("flow.%s", flow.Type()),
		core.FlowStartRequestedMessage{
			BaseMessage: core.BaseMessage{
				MessageType:     core.MessageTypeFlowStartRequested,
				FlowExecutionID: flowExecutionID,
				Timestamp:       time.Now(),
			},
			FlowType:          flow.Type(),
			FlowDefinitionID:  flow.ID(),
			InitialSharedData: initialData,
		},
	)

	log.Info().Str("flowExecutionID", flowExecutionID).Msg("Flow execution started")
	return flowExecutionID
}

// RunFlowAndWait executes a flow and waits for it to complete
func (r *Runner) RunFlowAndWait(flow core.Flow, initialData map[string]interface{}) (string, error) {
	flowExecutionID := r.RunFlow(flow, initialData)
	err := r.WaitForFlow(flowExecutionID)
	return flowExecutionID, err
}

// WaitForFlow waits for a flow to complete
func (r *Runner) WaitForFlow(flowExecutionID string) error {
	// Create a channel to wait for completion
	r.muCompChans.Lock()
	ch := make(chan struct{}, 1)
	r.completeChans[flowExecutionID] = ch
	r.muCompChans.Unlock()

	// Wait for completion signal
	<-ch

	// Check if the flow failed
	// We could store error information in a map when the flow fails
	// For now, just report success
	return nil
}

// Stop gracefully stops the runner
func (r *Runner) Stop() error {
	log.Info().Msg("Stopping runner")
	// XXX needs to wait on all flow event routers
	return nil
}

// GetSharedData gets the shared data for a flow execution
func (r *Runner) GetSharedData(flowExecutionID string) (map[string]interface{}, error) {
	return r.stateStore.GetSharedData(flowExecutionID)
}

// StateStore returns the state store
func (r *Runner) StateStore() semantic.StateStore {
	return r.stateStore
}

// Publisher returns the event publisher
func (r *Runner) Publisher() core.EventPublisher {
	return r.publisher
}

// Subscriber returns the event subscriber (uses the same Subscriber as router)
func (r *Runner) Subscriber() message.Subscriber {
	return r.subscriber
}

// FlowRegistry returns the flow registry
func (r *Runner) FlowRegistry() core.FlowRegistry {
	return r.flowRegistry
}
