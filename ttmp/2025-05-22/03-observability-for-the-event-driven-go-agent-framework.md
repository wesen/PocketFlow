# Observability Implementation for PocketFlow Go Event-Driven Agent Framework

This document outlines the design and implementation details for the comprehensive observability system for the PocketFlow Go event-driven agent framework, including Redis Streams integration with separate consumer groups.

## 1. Overview and Design Goals

### 1.1 Observability Requirements ✅ IMPLEMENTED

The observability system provides:

1. **Real-time Event Monitoring**: ✅ Track all messages flowing through the system via pub-sub subscription
2. **Flow Execution Tracing**: ✅ Follow the complete lifecycle of a flow execution with colorized console output
3. **Node Worker Monitoring**: ✅ Observe individual node worker performance and behavior

### 1.2 Core Design Principles ✅ IMPLEMENTED

- **Subscriber-based Architecture**: Observers subscribe to existing topics rather than intercepting publishers
- **Non-intrusive**: Zero impact on flow execution performance or reliability
- **Event-driven**: Leverages the existing pub-sub infrastructure
- **Pluggable**: Easy to add custom observers for different monitoring needs
- **Real-time**: Immediate visibility into system behavior

## 2. Implementation Architecture

### 2.1 Revised Key Interfaces ✅ IMPLEMENTED

```go
// Observer interface for monitoring system events via subscription
type Observer interface {
    // GetName returns the observer's identifier
    GetName() string
    
    // IsEnabled returns whether this observer is active
    IsEnabled() bool
    
    // SetEnabled enables or disables this observer
    SetEnabled(enabled bool)
    
    // GetSubscribedTopics returns the list of topics this observer wants to subscribe to
    GetSubscribedTopics() []string
    
    // HandleMessage processes a message from a subscribed topic
    HandleMessage(topic string, message []byte) error
}

// ObservableEvent represents any event that can be observed
type ObservableEvent interface {
    // GetEventType returns the type of event
    GetEventType() string
    
    // GetTimestamp returns when the event occurred
    GetTimestamp() time.Time
    
    // GetFlowExecutionID returns the associated flow execution ID (if any)
    GetFlowExecutionID() string
    
    // GetNodeExecutionID returns the associated node execution ID (if any)
    GetNodeExecutionID() string
    
    // GetMetadata returns additional event metadata
    GetMetadata() map[string]interface{}
    
    // ToJSON serializes the event to JSON
    ToJSON() ([]byte, error)
}

// ObservabilityManager coordinates multiple observers via subscriptions
type ObservabilityManager interface {
    // AddObserver adds an observer and subscribes it to its topics
    AddObserver(observer Observer) error
    
    // RemoveObserver removes an observer and unsubscribes it
    RemoveObserver(name string) error
    
    // GetObserver returns an observer by name
    GetObserver(name string) Observer
    
    // ListObservers returns all observers
    ListObservers() []Observer
    
    // EnableObserver enables an observer by name
    EnableObserver(name string) error
    
    // DisableObserver disables an observer by name  
    DisableObserver(name string) error
    
    // Start begins the observability system
    Start() error
    
    // Stop shuts down the observability system
    Stop() error
}

// FlowTracer specifically tracks flow-level events
type FlowTracer interface {
    Observer
    
    // OnFlowStarted handles flow start events
    OnFlowStarted(event FlowStartedEvent) error
    
    // OnFlowCompleted handles flow completion events
    OnFlowCompleted(event FlowCompletedEvent) error
    
    // OnFlowFailed handles flow failure events
    OnFlowFailed(event FlowFailedEvent) error
    
    // GetFlowStatus returns the current status of a flow
    GetFlowStatus(flowExecutionID string) (*FlowStatus, error)
}

// NodeTracer specifically tracks node-level events
type NodeTracer interface {
    Observer
    
    // OnNodeStarted handles node execution start events
    OnNodeStarted(event NodeStartedEvent) error
    
    // OnNodeCompleted handles node completion events
    OnNodeCompleted(event NodeCompletedEvent) error
    
    // OnNodeFailed handles node failure events
    OnNodeFailed(event NodeFailedEvent) error
}
```

## 3. Implementation Details ✅ COMPLETED

### 3.1 File Structure

```
go/event/observability/
├── interfaces.go           # Core interfaces and types
├── events.go              # Observable event definitions and factory functions
├── stdout_observer.go     # Console output observer implementation
├── manager.go             # ObservabilityManager implementation
├── observability.go       # Package entry point
└── README.md              # Documentation and usage guide
```

### 3.2 Topic Subscription Strategy ✅ IMPLEMENTED

Observers subscribe to existing PocketFlow topics:

| Topic Pattern | Events Captured | Observer Usage |
|---------------|-----------------|----------------|
| `flow.completed` | All flow completions | Universal monitoring |
| `flow.failed` | All flow failures | Error tracking |
| `node.completed` | All node completions | Node performance |
| `node.exec.failed` | All node failures | Node error tracking |
| `progress` | Progress updates | Status monitoring |
| `flow.*` | All flow-related events | Comprehensive flow tracking |
| `node.*` | All node-related events | Comprehensive node tracking |

### 3.3 Message Processing Pipeline ✅ IMPLEMENTED

1. **Subscription**: Observer subscribes to relevant topics via `GetSubscribedTopics()`
2. **Message Receipt**: Raw JSON messages received from topics
3. **Parsing**: Messages parsed based on `message_type` field 
4. **Event Creation**: Appropriate `ObservableEvent` instances created
5. **Formatting**: Events formatted for output (colorized console, logs, etc.)
6. **Output**: Processed events sent to destination (stdout, files, metrics systems)

### 3.4 Observable Event Types ✅ IMPLEMENTED

```go
// Base observable event that wraps existing PocketFlow messages
type BaseObservableEvent struct {
    EventType       string                 `json:"event_type"`
    Timestamp       time.Time              `json:"timestamp"`
    FlowExecutionID string                 `json:"flow_execution_id,omitempty"`
    NodeExecutionID string                 `json:"node_execution_id,omitempty"`
    Metadata        map[string]interface{} `json:"metadata,omitempty"`
    OriginalMessage interface{}            `json:"original_message,omitempty"`
    SpanID          string                 `json:"span_id,omitempty"`
    ParentSpanID    string                 `json:"parent_span_id,omitempty"`
    TraceID         string                 `json:"trace_id,omitempty"`
}

// Flow-specific observable events
type FlowStartedEvent struct {
    BaseObservableEvent
    FlowType         string                 `json:"flow_type"`
    FlowDefinitionID string                 `json:"flow_definition_id"`
    InitialData      map[string]interface{} `json:"initial_data,omitempty"`
}

type FlowCompletedEvent struct {
    BaseObservableEvent
    FlowType     string        `json:"flow_type"`
    FinalAction  string        `json:"final_action"`
    FinalResult  interface{}   `json:"final_result,omitempty"`
    Duration     time.Duration `json:"duration"`
    NodesExecuted int          `json:"nodes_executed"`
}

type FlowFailedEvent struct {
    BaseObservableEvent
    FlowType     string        `json:"flow_type"`
    ErrorMessage string        `json:"error_message"`
    ErrorDetails string        `json:"error_details,omitempty"`
    FailedNodeID string        `json:"failed_node_id,omitempty"`
    Duration     time.Duration `json:"duration"`
}

// Node-specific observable events
type NodeStartedEvent struct {
    BaseObservableEvent
    NodeType string                 `json:"node_type"`
    NodeID   string                 `json:"node_id"`
    Params   map[string]interface{} `json:"params,omitempty"`
}

type NodeCompletedEvent struct {
    BaseObservableEvent
    NodeType string        `json:"node_type"`
    NodeID   string        `json:"node_id"`
    Action   string        `json:"action"`
    Result   interface{}   `json:"result,omitempty"`
    Duration time.Duration `json:"duration"`
}

type NodeFailedEvent struct {
    BaseObservableEvent
    NodeType     string        `json:"node_type"`
    NodeID       string        `json:"node_id"`
    ErrorMessage string        `json:"error_message"`
    ErrorDetails string        `json:"error_details,omitempty"`
    RetryCount   int           `json:"retry_count"`
    WillRetry    bool          `json:"will_retry"`
    Duration     time.Duration `json:"duration"`
}

// Progress tracking events
type ProgressUpdateEvent struct {
    BaseObservableEvent
    Status   string  `json:"status"`
    Progress float64 `json:"progress"` // 0.0 to 1.0
    Message  string  `json:"message,omitempty"`
}
```

## 4. Built-in Observers ✅ IMPLEMENTED

### 4.1 StdoutObserver ✅ IMPLEMENTED

**Features:**
- Colorized console output with timestamps
- Configurable verbosity (show/hide detailed parameters and results)
- Automatic ID truncation for readability
- Event-specific formatting for different message types

**Usage:**
```go
// Basic observer
observer := observability.NewStdoutObserver("console")

// With custom options (colorized: true, verbose: false)
observer := observability.NewStdoutObserverWithOptions("console", true, false)
```

**Output Example:**
```
15:04:05.123 flow.started    [abc12345] Flow 'qa_chain' started (def: qa_def_001)
15:04:05.223 node.started    [abc12345] (def67890) Node 'question_node' (question) started
15:04:05.323 node.completed  [abc12345] (def67890) Node 'question_node' completed in 100ms with action 'default'
15:04:05.773 flow.completed  [abc12345] Flow 'qa_chain' completed in 650ms (2 nodes)
```

### 4.2 StdoutFlowTracer ✅ IMPLEMENTED

**Features:**
- Extends StdoutObserver with flow status tracking
- Maintains in-memory flow execution status
- Queryable flow status with performance metrics
- Automatic status updates based on flow lifecycle events

**Usage:**
```go
tracer := observability.NewStdoutFlowTracer("flow_tracer")

// Query flow status
status, err := tracer.GetFlowStatus(flowExecutionID)
```

**Status Information:**
```go
type FlowStatus struct {
    FlowExecutionID  string                 `json:"flow_execution_id"`
    FlowType         string                 `json:"flow_type"`
    Status           string                 `json:"status"` // "running", "completed", "failed"
    StartTime        time.Time              `json:"start_time"`
    EndTime          *time.Time             `json:"end_time,omitempty"`
    Duration         time.Duration          `json:"duration"`
    NodesExecuted    int                    `json:"nodes_executed"`
    FinalResult      interface{}            `json:"final_result,omitempty"`
    ErrorMessage     string                 `json:"error_message,omitempty"`
}
```

## 5. Integration Guide ✅ IMPLEMENTED WITH REDIS STREAMS

### 5.1 Main Application Integration

The observability system is integrated into `go/main.go` with command-line flags and now supports both Redis Streams and in-memory messaging:

**Command-line Flags:**
```bash
# Enable basic observability with Redis (default)
./go -flow basic -observability

# Enable verbose observability with Redis
./go -flow basic -observability-verbose

# Use in-memory messaging instead of Redis
./go -redis=false -flow basic -observability

# Use custom Redis address
./go -redis-addr redis:6379 -flow basic -observability

# Show help with examples
./go -help
```

**Integration Code (Updated for Redis Streams):**
```go
// Setup observability if enabled
var obsManager event.ObservabilityManager
if useObservability {
    // Create observability manager with appropriate subscriber
    if *useRedis {
        // For Redis, create a separate observability router with its own consumer group
        obsRouter, err := event.NewObservabilityRouterWithRedis(*redisAddr, nil)
        if err != nil {
            log.Error().Err(err).Msg("Failed to create observability router")
        } else {
            // Create observability manager with separate subscriber for Redis
            obsSubscriber := event.NewWatermillSubscriber(obsRouter.Subscriber)
            obsManager = event.NewObservabilityManager(obsSubscriber)
        }
    } else {
        // For in-memory, use the same subscriber as the main runner
        obsManager = event.NewObservabilityManager(runner.Subscriber())
    }
    
    // Add stdout observer with appropriate verbosity
    stdoutObserver := event.NewStdoutObserverWithOptions("console", true, *observabilityVerbose)
    obsManager.AddObserver(stdoutObserver)
    
    // Add flow tracer for detailed flow tracking
    flowTracer := event.NewStdoutFlowTracer("flow_tracer")
    obsManager.AddObserver(flowTracer)
}

// Start observability system after runner starts
if useObservability && obsManager != nil {
    obsManager.Start() // Subscribes to topics
    defer obsManager.Stop()
}
```

### 5.2 Core Infrastructure Updates ✅ IMPLEMENTED WITH REDIS STREAMS

**Added to Runner (`go/event/runner.go`):**
```go
// Runner struct now supports Redis configuration
type Runner struct {
    stateStore    core.StateStore
    flowRegistry  core.FlowRegistry
    publisher     core.EventPublisher
    router        *WatermillEventRouter
    options       RunnerOptions
    completeChans map[string]chan struct{}
    muCompChans   sync.Mutex
    useRedis      bool      // New: Redis configuration
    redisAddr     string    // New: Redis address
}

// NewRunnerWithRedis creates a new runner with Redis Streams messaging
func NewRunnerWithRedis(redisAddr string, opts ...Option) *Runner {
    // Implementation creates runner with Redis-backed router
}

// Subscriber returns the event subscriber
func (r *Runner) Subscriber() core.EventSubscriber {
    return NewWatermillSubscriber(r.router.Subscriber)
}
```

**Updated Watermill Integration (`go/event/watermill.go`):**
```go
// WatermillEventRouter now uses separate Publisher and Subscriber interfaces
type WatermillEventRouter struct {
    NodeWorkers      map[string]core.NodeWorker
    FlowWorkers      map[string]core.FlowWorker
    Publisher        message.Publisher   // Updated: separate interfaces
    Subscriber       message.Subscriber  // Updated: separate interfaces
    Router           *message.Router
    Logger           watermill.LoggerAdapter
}

// NewWatermillEventRouterWithRedis creates a router using Redis Streams
func NewWatermillEventRouterWithRedis(redisAddr string, logger watermill.LoggerAdapter) *WatermillEventRouter {
    // Implementation with Redis publisher/subscriber
}

// NewObservabilityRouterWithRedis creates observability router with separate consumer group
func NewObservabilityRouterWithRedis(redisAddr string, logger watermill.LoggerAdapter) (*WatermillEventRouter, error) {
    // Implementation with "pocketflow_observability" consumer group
}
```

**Docker Compose for Redis (`go/docker-compose.yml`):**
```yaml
version: '3.8'
services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    command: redis-server --appendonly yes
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5
volumes:
  redis_data:
```

## 6. Usage Examples ✅ WORKING

### 6.1 Basic Usage

```bash
# Start Redis (if using Docker)
cd go && docker-compose up -d

# Run with Redis and observability - see real-time events
cd go && ./go -flow basic -observability

# Run with verbose observability - see detailed parameters
cd go && ./go -flow qa -observability-verbose

# Run with in-memory messaging instead of Redis
cd go && ./go -redis=false -flow basic -observability

# Use custom Redis address
cd go && ./go -redis-addr redis:6379 -flow basic -observability

# Just visualize flow without execution
cd go && ./go -flow branching -visualize

# Start web UI with Redis
cd go && ./go -web

# Start web UI with in-memory messaging
cd go && ./go -redis=false -web
```

### 6.2 Custom Observer Implementation

```go
type MetricsObserver struct {
    name      string
    enabled   bool
    collector MetricsCollector
}

func (o *MetricsObserver) GetSubscribedTopics() []string {
    return []string{"flow.completed", "flow.failed", "node.completed", "node.exec.failed"}
}

func (o *MetricsObserver) HandleMessage(topic string, message []byte) error {
    // Parse message and send metrics to your monitoring system
    return nil
}

// Register custom observer
obsManager.AddObserver(NewMetricsObserver("metrics", collector))
```

## 7. Architecture Decisions & Lessons Learned ✅ FINAL WITH REDIS STREAMS

### 7.1 Key Architecture Decision: Subscriber-Based Design with Redis Consumer Groups

**Original Design Flaw:** Initially implemented as publisher interceptors/wrappers that attempted to observe events by wrapping the EventPublisher interface.

**Corrected Design:** Switched to pure subscriber-based architecture where observers subscribe directly to existing topics using Redis Streams with separate consumer groups.

**Benefits of Final Design:**
1. **Zero Performance Impact**: No interception overhead on message publishing
2. **True Event-Driven**: Leverages existing pub-sub infrastructure properly  
3. **Non-Intrusive**: No changes needed to existing flow/node worker code
4. **Decoupled**: Observers can be added/removed without affecting core system
5. **Scalable**: Multiple observers can subscribe independently
6. **Redis Consumer Groups**: Observability uses separate consumer group (`pocketflow_observability`) so it doesn't steal events from main application (`pocketflow_main`)
7. **Persistent Messaging**: Redis Streams provide message persistence and replay capabilities
8. **Multi-Instance Support**: Different application instances can share the same Redis infrastructure

### 7.2 Implementation Status

✅ **COMPLETED:**
- Core interfaces and event model
- StdoutObserver with colorized output and configurable verbosity
- StdoutFlowTracer with flow status tracking
- ObservabilityManager with subscription coordination
- Integration with main application via command-line flags
- Topic subscription strategy leveraging existing message routing
- Message parsing and event creation pipeline
- Complete documentation and usage examples
- **NEW: Redis Streams integration with separate consumer groups**
- **NEW: Docker Compose setup for Redis infrastructure**
- **NEW: Command-line flags for Redis configuration**
- **NEW: Fallback to in-memory messaging when Redis is disabled**

✅ **TESTED & WORKING:**
- Command-line integration with `-observability` and `-observability-verbose` flags
- Real-time event monitoring during flow execution
- Colorized console output with proper formatting
- Flow status tracking and querying
- Multiple observer coordination
- Error handling and graceful degradation
- **NEW: Redis Streams messaging with separate consumer groups**
- **NEW: Docker Compose Redis setup**
- **NEW: Observability isolation (doesn't steal events from main application)**

### 7.3 Next Developer Notes

The observability system is fully functional and ready for production use with Redis Streams support. Future enhancements could include:

1. **Additional Observers**: Database logging, metrics exporters, alerting systems
2. **Distributed Tracing**: Integration with OpenTelemetry or Jaeger
3. **Performance Metrics**: CPU, memory, latency tracking
4. **Custom Dashboards**: Web UI for flow monitoring
5. **Alert Rules**: Configurable alerting based on flow patterns
6. **Redis Cluster Support**: Scale Redis infrastructure horizontally
7. **Cross-Instance Monitoring**: Monitor flows across multiple application instances
8. **Message Replay**: Leverage Redis Streams' replay capabilities for debugging

The foundation is solid and extensible - new observers just need to implement the `Observer` interface and specify their topic subscriptions. The Redis integration provides enterprise-grade messaging capabilities while maintaining the simplicity of the original design.