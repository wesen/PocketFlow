package event

import (
	"context"
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
	// Separate router for each flow type, each with its own node workers
	flowRouters map[string]*FlowEventRouter

	// Global router for system-wide handlers (completion, failure, etc.)
	globalRouter *WatermillEventRouter

	// Node workers registered for flows - keyed by nodeType for sharing across flows
	nodeWorkers   map[string]core.NodeWorker
	muNodeWorkers sync.RWMutex

	options       RunnerOptions
	completeChans map[string]chan struct{}
	muCompChans   sync.Mutex
	muFlowRouters sync.RWMutex
	useRedis      bool
	redisAddr     string

	// Runner state
	isInitialized bool
	isRunning     bool
	muState       sync.RWMutex

	// Observability
	observabilityPublisher message.Publisher
	observabilityTopic     string
	hasObservability       bool
	muObservability        sync.RWMutex
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
		nodeWorkers:   make(map[string]core.NodeWorker),
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
		nodeWorkers:   make(map[string]core.NodeWorker),
		useRedis:      true,
		redisAddr:     redisAddr,
	}
}

// Init initializes the runner with all necessary components
func (r *Runner) Init() error {
	r.muState.Lock()
	defer r.muState.Unlock()

	if r.isInitialized {
		return nil
	}

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
		r.globalRouter = NewWatermillEventRouterWithRedis(r.redisAddr, nil)
		r.publisher = NewWatermillPublisher(r.globalRouter.Publisher)
		subscriber = r.globalRouter.Subscriber
	} else {
		log.Info().Msg("Setting up in-memory publisher")
		r.globalRouter = NewWatermillEventRouter(nil)
		r.publisher = NewWatermillPublisher(r.globalRouter.Publisher)
		subscriber = r.globalRouter.Subscriber
	}
	log.Info().Msg("Event publisher initialized")

	// Store subscriber for creating flow routers
	r.subscriber = subscriber

	// Set up global flow completion and failure handlers
	r.globalRouter.SetupFlowCompletionHandler(r.handleFlowCompleted)
	r.globalRouter.SetupFlowFailureHandler(r.handleFlowFailed)
	log.Info().Msg("Global flow completion and failure handlers registered")
	r.isInitialized = true

	log.Info().Msg("PocketFlow runner initialized")
	return nil
}

// RegisterNodeWorker registers a node worker with the router
func (r *Runner) RegisterNodeWorker(worker core.NodeWorker) (*Runner, error) {
	if _, ok := r.nodeWorkers[worker.NodeType()]; ok {
		log.Warn().Str("nodeType", worker.NodeType()).Msg("Node worker already registered")
		return nil, fmt.Errorf("node worker already registered: %s", worker.NodeType())
	}
	r.nodeWorkers[worker.NodeType()] = worker
	return r, nil
}

// RegisterNodeWorkers registers multiple node workers with the router
func (r *Runner) RegisterNodeWorkers(workers ...core.NodeWorker) error {
	for _, worker := range workers {
		if _, ok := r.nodeWorkers[worker.NodeType()]; ok {
			log.Warn().Str("nodeType", worker.NodeType()).Msg("Node worker already registered")
			return fmt.Errorf("node worker already registered: %s", worker.NodeType())
		}
		r.nodeWorkers[worker.NodeType()] = worker
	}
	return nil
}

// RegisterFlow registers a flow with the registry and creates a worker for it
func (r *Runner) RegisterFlow(flow core.Flow) *Runner {
	r.muFlowRouters.Lock()
	defer r.muFlowRouters.Unlock()

	// Check if we need to initialize first
	r.muState.RLock()
	if !r.isInitialized {
		r.muState.RUnlock()
		log.Fatal().Msg("Runner must be initialized before registering flows. Call Init() first.")
		return r
	}
	isRunning := r.isRunning
	r.muState.RUnlock()

	// Register flow with registry
	err := r.flowRegistry.RegisterFlow(flow.ID(), flow)
	if err != nil {
		log.Fatal().Err(err).Msg("Error registering flow")
		return r
	}

	// Create and register a flow worker
	flowRouter, err := NewFlowEventRouter(flow, r.stateStore, r.publisher, r.subscriber, r.nodeWorkers, nil)
	if err != nil {
		log.Fatal().Err(err).Msg("Error creating flow router")
		return r
	}
	r.flowRouters[flow.Type()] = flowRouter

	// Register observability middleware if configured
	r.muObservability.RLock()
	if r.hasObservability {
		log.Debug().
			Str("flowType", flow.Type()).
			Str("observabilityTopic", r.observabilityTopic).
			Interface("observabilityPublisher", r.observabilityPublisher).
			Msg("Registering observability middleware on new flow router")
		flowRouter.RegisterObservabilityMiddleware(r.observabilityPublisher, r.observabilityTopic)
	}
	r.muObservability.RUnlock()

	// If runner is already running, start this router immediately
	if isRunning {
		if err := flowRouter.Start(); err != nil {
			log.Error().Err(err).Str("flowType", flow.Type()).Msg("Failed to start flow router")
		}
	}

	log.Info().
		Str("flowType", flow.Type()).
		Str("flowID", flow.ID()).
		Bool("startedImmediately", isRunning).
		Msg("Flow registered")

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
				FlowType:        flow.Type(),
			},
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

// Start starts all registered flow routers
func (r *Runner) Start() error {
	r.muState.Lock()
	defer r.muState.Unlock()

	if !r.isInitialized {
		return fmt.Errorf("runner must be initialized before starting. Call Init() first")
	}

	if r.isRunning {
		return nil // Already running
	}

	log.Info().Msg("Starting PocketFlow runner")

	// Start the global router for system-wide handlers
	if err := r.globalRouter.Start(context.Background()); err != nil {
		log.Error().Err(err).Msg("Global router stopped with error")
	}
	log.Info().Msg("Global router started")

	// Start all flow routers
	if err := r.startAllFlowRouters(); err != nil {
		return fmt.Errorf("failed to start flow routers: %w", err)
	}

	r.isRunning = true
	log.Info().Msg("PocketFlow runner started successfully")
	return nil
}

// startAllFlowRouters starts all registered flow routers
func (r *Runner) startAllFlowRouters() error {
	r.muFlowRouters.RLock()
	defer r.muFlowRouters.RUnlock()

	var startErrors []error

	for flowType, flowRouter := range r.flowRouters {
		if !flowRouter.IsRunning() {
			log.Debug().Str("flowType", flowType).Msg("Starting flow router")
			if err := flowRouter.Start(); err != nil {
				startErrors = append(startErrors, fmt.Errorf("failed to start router for flow type %s: %w", flowType, err))
				log.Error().Err(err).Str("flowType", flowType).Msg("Failed to start flow router")
			} else {
				log.Info().Str("flowType", flowType).Msg("Flow router started")
			}
		}
	}

	if len(startErrors) > 0 {
		// Return the first error, but log all of them
		return startErrors[0]
	}

	log.Info().Int("routerCount", len(r.flowRouters)).Msg("All flow routers started")
	return nil
}

// stopAllFlowRouters stops all flow routers
func (r *Runner) stopAllFlowRouters() error {
	r.muFlowRouters.RLock()
	defer r.muFlowRouters.RUnlock()

	var stopErrors []error

	for flowType, flowRouter := range r.flowRouters {
		if flowRouter.IsRunning() {
			log.Debug().Str("flowType", flowType).Msg("Stopping flow router")
			if err := flowRouter.Stop(); err != nil {
				stopErrors = append(stopErrors, fmt.Errorf("failed to stop router for flow type %s: %w", flowType, err))
				log.Error().Err(err).Str("flowType", flowType).Msg("Failed to stop flow router")
			} else {
				log.Info().Str("flowType", flowType).Msg("Flow router stopped")
			}
		}
	}

	if len(stopErrors) > 0 {
		// Return the first error, but log all of them
		return stopErrors[0]
	}

	log.Info().Int("routerCount", len(r.flowRouters)).Msg("All flow routers stopped")
	return nil
}

// Stop gracefully stops the runner
func (r *Runner) Stop() error {
	r.muState.Lock()
	defer r.muState.Unlock()

	if !r.isRunning {
		return nil // Already stopped
	}

	log.Info().Msg("Stopping PocketFlow runner")

	// Stop all flow routers
	if err := r.stopAllFlowRouters(); err != nil {
		log.Error().Err(err).Msg("Error stopping flow routers")
		// Continue with shutdown even if some routers failed to stop
	}

	// Stop the global router
	if err := r.globalRouter.Stop(); err != nil {
		log.Error().Err(err).Msg("Error stopping global router")
		// Continue with shutdown even if global router failed to stop
	} else {
		log.Info().Msg("Global router stopped")
	}

	r.isRunning = false
	log.Info().Msg("PocketFlow runner stopped")
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

// IsInitialized returns true if the runner has been initialized
func (r *Runner) IsInitialized() bool {
	r.muState.RLock()
	defer r.muState.RUnlock()
	return r.isInitialized
}

// IsRunning returns true if the runner is currently running
func (r *Runner) IsRunning() bool {
	r.muState.RLock()
	defer r.muState.RUnlock()
	return r.isRunning
}

// GetRunningFlowRouters returns a list of running flow router types
func (r *Runner) GetRunningFlowRouters() []string {
	r.muFlowRouters.RLock()
	defer r.muFlowRouters.RUnlock()

	var running []string
	for flowType, router := range r.flowRouters {
		if router.IsRunning() {
			running = append(running, flowType)
		}
	}
	return running
}

// handleFlowCompleted handles flow completion events and notifies waiting channels
func (r *Runner) handleFlowCompleted(msg core.FlowCompletedMessage) error {
	// Call the user-defined handler first
	if r.options.OnFlowCompleted != nil {
		if err := r.options.OnFlowCompleted(r, msg); err != nil {
			log.Error().Err(err).Str("flowExecutionID", msg.FlowExecutionID).Msg("Error in flow completed handler")
		}
	}

	// Notify waiting channels
	r.muCompChans.Lock()
	if ch, exists := r.completeChans[msg.FlowExecutionID]; exists {
		select {
		case ch <- struct{}{}:
			log.Debug().Str("flowExecutionID", msg.FlowExecutionID).Msg("Notified waiting channel of flow completion")
		default:
			log.Warn().Str("flowExecutionID", msg.FlowExecutionID).Msg("Channel already has pending notification")
		}
		delete(r.completeChans, msg.FlowExecutionID)
	}
	r.muCompChans.Unlock()

	return nil
}

// handleFlowFailed handles flow failure events and notifies waiting channels
func (r *Runner) handleFlowFailed(msg core.FlowFailedMessage) error {
	// Call the user-defined handler first
	if r.options.OnFlowFailed != nil {
		if err := r.options.OnFlowFailed(r, msg); err != nil {
			log.Error().Err(err).Str("flowExecutionID", msg.FlowExecutionID).Msg("Error in flow failed handler")
		}
	}

	// Notify waiting channels (flow failed is also a completion)
	r.muCompChans.Lock()
	if ch, exists := r.completeChans[msg.FlowExecutionID]; exists {
		select {
		case ch <- struct{}{}:
			log.Debug().Str("flowExecutionID", msg.FlowExecutionID).Msg("Notified waiting channel of flow failure")
		default:
			log.Warn().Str("flowExecutionID", msg.FlowExecutionID).Msg("Channel already has pending notification")
		}
		delete(r.completeChans, msg.FlowExecutionID)
	}
	r.muCompChans.Unlock()

	return nil
}

// RegisterObserver registers an observability publisher and topic that will be used
// to register observability middleware on all existing and future flow routers
func (r *Runner) RegisterObserver(publisher message.Publisher, topic string) error {
	if publisher == nil {
		return fmt.Errorf("publisher is nil")
	}
	if topic == "" {
		return fmt.Errorf("topic is empty")
	}

	r.muObservability.Lock()
	defer r.muObservability.Unlock()

	log.Info().
		Interface("observabilityPublisher", publisher).
		Str("topic", topic).
		Msg("Registering observability observer with runner")

	// Store observability configuration
	r.observabilityPublisher = publisher
	r.observabilityTopic = topic
	r.hasObservability = true

	// Register observability middleware on all existing flow routers
	r.muFlowRouters.RLock()
	for flowType, flowRouter := range r.flowRouters {
		log.Debug().
			Str("flowType", flowType).
			Interface("observabilityPublisher", publisher).
			Str("topic", topic).
			Msg("Registering observability middleware on existing flow router")
		flowRouter.RegisterObservabilityMiddleware(publisher, topic)
	}
	r.muFlowRouters.RUnlock()

	log.Info().
		Int("existingRouters", len(r.flowRouters)).
		Str("topic", topic).
		Msg("Successfully registered observability observer")

	return nil
}
