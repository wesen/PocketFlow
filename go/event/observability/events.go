package observability

import (
	"encoding/json"
	"time"

	"github.com/The-Pocket/PocketFlow/go/event/core"
)

// Event type constants
const (
	EventTypeFlowStarted   = "flow.started"
	EventTypeFlowCompleted = "flow.completed"
	EventTypeFlowFailed    = "flow.failed"
	EventTypeNodeStarted   = "node.started"
	EventTypeNodeCompleted = "node.completed"
	EventTypeNodeFailed    = "node.failed"
	EventTypeProgressUpdate = "progress.update"
)

// BaseObservableEvent implements the ObservableEvent interface
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

// GetEventType returns the type of event
func (e *BaseObservableEvent) GetEventType() string {
	return e.EventType
}

// GetTimestamp returns when the event occurred
func (e *BaseObservableEvent) GetTimestamp() time.Time {
	return e.Timestamp
}

// GetFlowExecutionID returns the associated flow execution ID
func (e *BaseObservableEvent) GetFlowExecutionID() string {
	return e.FlowExecutionID
}

// GetNodeExecutionID returns the associated node execution ID
func (e *BaseObservableEvent) GetNodeExecutionID() string {
	return e.NodeExecutionID
}

// GetMetadata returns additional event metadata
func (e *BaseObservableEvent) GetMetadata() map[string]interface{} {
	return e.Metadata
}

// ToJSON serializes the event to JSON
func (e *BaseObservableEvent) ToJSON() ([]byte, error) {
	return json.Marshal(e)
}

// Flow-specific observable events

// FlowStartedEvent represents a flow start event
type FlowStartedEvent struct {
	BaseObservableEvent
	FlowType         string                 `json:"flow_type"`
	FlowDefinitionID string                 `json:"flow_definition_id"`
	InitialData      map[string]interface{} `json:"initial_data,omitempty"`
}

// FlowCompletedEvent represents a flow completion event
type FlowCompletedEvent struct {
	BaseObservableEvent
	FlowType      string        `json:"flow_type"`
	FinalAction   string        `json:"final_action"`
	FinalResult   interface{}   `json:"final_result,omitempty"`
	Duration      time.Duration `json:"duration"`
	NodesExecuted int           `json:"nodes_executed"`
}

// FlowFailedEvent represents a flow failure event
type FlowFailedEvent struct {
	BaseObservableEvent
	FlowType     string        `json:"flow_type"`
	ErrorMessage string        `json:"error_message"`
	ErrorDetails string        `json:"error_details,omitempty"`
	FailedNodeID string        `json:"failed_node_id,omitempty"`
	Duration     time.Duration `json:"duration"`
}

// Node-specific observable events

// NodeStartedEvent represents a node execution start event
type NodeStartedEvent struct {
	BaseObservableEvent
	NodeType string                 `json:"node_type"`
	NodeID   string                 `json:"node_id"`
	Params   map[string]interface{} `json:"params,omitempty"`
}

// NodeCompletedEvent represents a node completion event
type NodeCompletedEvent struct {
	BaseObservableEvent
	NodeType string        `json:"node_type"`
	NodeID   string        `json:"node_id"`
	Action   string        `json:"action"`
	Result   interface{}   `json:"result,omitempty"`
	Duration time.Duration `json:"duration"`
}

// NodeFailedEvent represents a node failure event
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

// ProgressUpdateEvent represents a progress update event
type ProgressUpdateEvent struct {
	BaseObservableEvent
	Status   string  `json:"status"`
	Progress float64 `json:"progress"` // 0.0 to 1.0
	Message  string  `json:"message,omitempty"`
}

// Helper functions to create events from PocketFlow messages

// CreateFlowStartedEvent creates a FlowStartedEvent from a FlowStartRequestedMessage
func CreateFlowStartedEvent(msg *core.FlowStartRequestedMessage) *FlowStartedEvent {
	return &FlowStartedEvent{
		BaseObservableEvent: BaseObservableEvent{
			EventType:       EventTypeFlowStarted,
			Timestamp:       msg.Timestamp,
			FlowExecutionID: msg.FlowExecutionID,
			OriginalMessage: msg,
		},
		FlowType:         msg.FlowType,
		FlowDefinitionID: msg.FlowDefinitionID,
		InitialData:      msg.InitialSharedData,
	}
}

// CreateFlowCompletedEvent creates a FlowCompletedEvent from a FlowCompletedMessage
func CreateFlowCompletedEvent(msg *core.FlowCompletedMessage) *FlowCompletedEvent {
	return &FlowCompletedEvent{
		BaseObservableEvent: BaseObservableEvent{
			EventType:       EventTypeFlowCompleted,
			Timestamp:       msg.Timestamp,
			FlowExecutionID: msg.FlowExecutionID,
			OriginalMessage: msg,
		},
		FlowType:    msg.FlowType,
		FinalAction: msg.FinalAction,
		FinalResult: msg.FinalResult,
		Duration:    time.Duration(msg.ExecutionMs) * time.Millisecond,
	}
}

// CreateFlowFailedEvent creates a FlowFailedEvent from a FlowFailedMessage
func CreateFlowFailedEvent(msg *core.FlowFailedMessage) *FlowFailedEvent {
	return &FlowFailedEvent{
		BaseObservableEvent: BaseObservableEvent{
			EventType:       EventTypeFlowFailed,
			Timestamp:       msg.Timestamp,
			FlowExecutionID: msg.FlowExecutionID,
			OriginalMessage: msg,
		},
		FlowType:     msg.FlowType,
		ErrorMessage: msg.ErrorMessage,
		ErrorDetails: msg.ErrorDetails,
		FailedNodeID: msg.FailedNodeID,
	}
}

// CreateNodeStartedEvent creates a NodeStartedEvent from an ExecRequestedMessage
func CreateNodeStartedEvent(msg *core.ExecRequestedMessage) *NodeStartedEvent {
	return &NodeStartedEvent{
		BaseObservableEvent: BaseObservableEvent{
			EventType:       EventTypeNodeStarted,
			Timestamp:       msg.Timestamp,
			FlowExecutionID: msg.FlowExecutionID,
			NodeExecutionID: msg.NodeExecutionID,
			OriginalMessage: msg,
		},
		NodeType: msg.NodeType,
		NodeID:   msg.NodeID,
		Params:   msg.Params,
	}
}

// CreateNodeCompletedEvent creates a NodeCompletedEvent from a NodeCompletedMessage
func CreateNodeCompletedEvent(msg *core.NodeCompletedMessage) *NodeCompletedEvent {
	return &NodeCompletedEvent{
		BaseObservableEvent: BaseObservableEvent{
			EventType:       EventTypeNodeCompleted,
			Timestamp:       msg.Timestamp,
			FlowExecutionID: msg.FlowExecutionID,
			NodeExecutionID: msg.NodeExecutionID,
			OriginalMessage: msg,
		},
		NodeType: msg.NodeType,
		NodeID:   msg.NodeID,
		Action:   msg.Action,
		Result:   msg.Result,
	}
}

// CreateNodeFailedEvent creates a NodeFailedEvent from an ExecFailedMessage
func CreateNodeFailedEvent(msg *core.ExecFailedMessage) *NodeFailedEvent {
	return &NodeFailedEvent{
		BaseObservableEvent: BaseObservableEvent{
			EventType:       EventTypeNodeFailed,
			Timestamp:       msg.Timestamp,
			FlowExecutionID: msg.FlowExecutionID,
			NodeExecutionID: msg.NodeExecutionID,
			OriginalMessage: msg,
		},
		NodeType:     msg.NodeType,
		NodeID:       msg.NodeID,
		ErrorMessage: msg.ErrorMessage,
		ErrorDetails: msg.ErrorDetails,
		RetryCount:   msg.RetryCount,
		WillRetry:    msg.WillRetry,
	}
}

// CreateProgressUpdateEvent creates a ProgressUpdateEvent from a ProgressUpdateMessage
func CreateProgressUpdateEvent(msg *core.ProgressUpdateMessage) *ProgressUpdateEvent {
	return &ProgressUpdateEvent{
		BaseObservableEvent: BaseObservableEvent{
			EventType:       EventTypeProgressUpdate,
			Timestamp:       msg.Timestamp,
			FlowExecutionID: msg.FlowExecutionID,
			NodeExecutionID: msg.NodeExecutionID,
			OriginalMessage: msg,
		},
		Status:   msg.Status,
		Progress: msg.Progress,
		Message:  msg.Message,
	}
}