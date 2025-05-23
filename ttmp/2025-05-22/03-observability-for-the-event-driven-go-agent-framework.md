# Observability Design for PocketFlow Go Event-Driven Agent Framework

This document outlines the design for a comprehensive observability system for the PocketFlow Go event-driven agent framework.

## 1. Overview and Design Goals

### 1.1 Observability Requirements

The observability system should provide:

1. **Real-time Event Monitoring**: Track all messages flowing through the system
2. **Flow Execution Tracing**: Follow the complete lifecycle of a flow execution
3. **Node Worker Monitoring**: Observe individual node worker performance and behavior

### 1.2 Core Design Principles

### 2.2 Key Interfaces

```go
// Observer interface for monitoring system events
type Observer interface {
    // Observe processes an event and optionally transforms it
    Observe(event ObservableEvent) error
    
    // GetName returns the observer's identifier
    GetName() string
    
    // IsEnabled returns whether this observer is active
    IsEnabled() bool
    
    // SetEnabled enables or disables this observer
    SetEnabled(enabled bool)
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

## 3. Event Model and Data Structures

### 3.1 Observable Event Types

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