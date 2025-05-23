# PocketFlow Go Observability System

The observability system provides comprehensive monitoring and tracing capabilities for PocketFlow's event-driven agent framework. It allows you to observe, track, and analyze the execution of flows and nodes in real-time.

## Features

- **Real-time Event Monitoring**: Track all messages flowing through the system
- **Flow Execution Tracing**: Follow the complete lifecycle of a flow execution
- **Node Worker Monitoring**: Observe individual node worker performance and behavior
- **Multiple Observer Support**: Add custom observers for different monitoring needs
- **Built-in Stdout Observer**: Colorized console output with configurable verbosity
- **Flow Status Tracking**: Query flow execution status and performance metrics

## Core Components

### Observer Interface

The `Observer` interface is the foundation of the observability system:

```go
type Observer interface {
    Observe(event ObservableEvent) error
    GetName() string
    IsEnabled() bool
    SetEnabled(enabled bool)
}
```

### ObservabilityManager

Coordinates multiple observers and creates observable events from PocketFlow messages:

```go
manager := observability.NewObservabilityManager()
manager.AddObserver(observer)
manager.NotifyObservers(event)
```

### EventPublisherObserver

Wraps your existing EventPublisher to automatically emit observable events:

```go
observablePublisher := observability.NewObservableEventPublisher(publisher, manager)
observablePublisher.PublishWithObservability(topic, event)
```

## Built-in Observers

### StdoutObserver

Outputs formatted events to stdout with optional colors and verbose mode:

```go
// Basic stdout observer
observer := observability.NewStdoutObserver("console")

// With custom options
observer := observability.NewStdoutObserverWithOptions("console", true, false) // colorized, not verbose
```

### StdoutFlowTracer

Extends StdoutObserver with flow status tracking capabilities:

```go
tracer := observability.NewStdoutFlowTracer("flow_tracer")

// Query flow status
status, err := tracer.GetFlowStatus(flowExecutionID)
```

## Quick Start

### 1. Basic Setup

```go
import "github.com/The-Pocket/PocketFlow/go/event/observability"

// Create observability manager
obsManager := observability.NewObservabilityManager()

// Add stdout observer
stdoutObserver := observability.NewStdoutObserver("console")
obsManager.AddObserver(stdoutObserver)

// Wrap your publisher
observablePublisher := observability.NewObservableEventPublisher(publisher, obsManager)
```

### 2. Integration with Runner

```go
// Get publisher from your runner
publisher := runner.Publisher()

// Create observability manager and add observers
obsManager := observability.NewObservabilityManager()
obsManager.AddObserver(observability.NewStdoutObserver("console"))
obsManager.AddObserver(observability.NewStdoutFlowTracer("tracer"))

// Wrap the publisher
observablePublisher := observability.NewObservableEventPublisher(publisher, obsManager)

// Use the observable publisher in your workers
// (Replace the original publisher with the observable one)
```

### 3. Publishing Observable Events

```go
// When publishing events, use PublishWithObservability instead of Publish
observablePublisher.PublishWithObservability(topic, message)

// This will:
// 1. Publish the message normally
// 2. Create an observable event
// 3. Notify all registered observers
```

## Event Types

The observability system supports the following event types:

- **Flow Events**:
  - `flow.started` - Flow execution begins
  - `flow.completed` - Flow execution completes successfully
  - `flow.failed` - Flow execution fails

- **Node Events**:
  - `node.started` - Node execution begins
  - `node.completed` - Node execution completes
  - `node.failed` - Node execution fails

- **Progress Events**:
  - `progress.update` - Progress updates with percentage and status

## Output Examples

### Colorized Console Output

```
15:04:05.123 flow.started    [abc12345] Flow 'qa_chain' started (def: qa_def_001)
15:04:05.223 node.started    [abc12345] (def67890) Node 'question_node' (question) started
15:04:05.323 node.completed  [abc12345] (def67890) Node 'question_node' (question) completed in 100ms with action 'default'
15:04:05.423 node.started    [abc12345] (ghi12345) Node 'answer_node' (llm_answer) started
15:04:05.723 node.completed  [abc12345] (ghi12345) Node 'answer_node' (llm_answer) completed in 300ms with action 'complete'
15:04:05.773 flow.completed  [abc12345] Flow 'qa_chain' completed in 650ms (2 nodes) with action 'complete'
```

### Flow Status Information

```go
status, err := flowTracer.GetFlowStatus(flowExecutionID)
if err == nil {
    fmt.Printf("Flow: %s\n", status.FlowType)
    fmt.Printf("Status: %s\n", status.Status)
    fmt.Printf("Duration: %v\n", status.Duration)
    fmt.Printf("Nodes Executed: %d\n", status.NodesExecuted)
}
```

## Advanced Usage

### Custom Observers

Implement the `Observer` interface to create custom monitoring solutions:

```go
type CustomObserver struct {
    name    string
    enabled bool
}

func (o *CustomObserver) Observe(event ObservableEvent) error {
    // Custom processing logic
    // e.g., send to metrics system, database, etc.
    return nil
}

func (o *CustomObserver) GetName() string { return o.name }
func (o *CustomObserver) IsEnabled() bool { return o.enabled }
func (o *CustomObserver) SetEnabled(enabled bool) { o.enabled = enabled }
```

### Observer Management

```go
// Enable/disable observers dynamically
obsManager.DisableObserver("console")
obsManager.EnableObserver("console")

// List all observers
observers := obsManager.ListObservers()
for _, obs := range observers {
    fmt.Printf("Observer: %s (enabled: %v)\n", obs.GetName(), obs.IsEnabled())
}

// Remove observer
obsManager.RemoveObserver("console")
```

### Error Handling

The observability system is designed to be non-intrusive. Observer failures won't stop your flow execution:

```go
// Even if observers fail, the original publish will succeed
err := observablePublisher.PublishWithObservability(topic, event)
// err will only contain errors from the original publisher, not observers
```

## Performance Considerations

- Observers are called synchronously, so avoid heavy processing in observers
- Consider using buffered/async observers for high-throughput scenarios
- The observability layer adds minimal overhead when observers are disabled
- Use the `IsEnabled()` check for expensive observer operations

## Examples

See the `examples/` directory for complete working examples:

- `observability_demo.go` - Basic observability features demonstration
- `runner_with_observability.go` - Integration with PocketFlow runner

## Testing

Run the observability tests:

```bash
cd go
go test ./event/observability/...
```

The test suite covers:
- Observer behavior and management
- Event creation and formatting
- Flow status tracking
- Error scenarios