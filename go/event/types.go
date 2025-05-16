package event

import (
	"time"
)

// Common event fields
type BaseEvent struct {
	EventID         string    `json:"event_id"`          // Unique ID of this event
	FlowExecutionID string    `json:"flow_execution_id"` // ID of the overall flow execution
	NodeExecutionID string    `json:"node_execution_id"` // ID of this specific node execution
	NodeType        string    `json:"node_type"`         // Type of node (e.g., "SummarizeNode")
	Timestamp       time.Time `json:"timestamp"`         // When this event was created
	CorrelationID   string    `json:"correlation_id"`    // For tracking related events
}

// Flow Events

// Flow initialization
type FlowStartRequested struct {
	BaseEvent
	FlowDefinitionID string                 `json:"flow_definition_id"`
	InitialSharedData map[string]interface{} `json:"initial_shared_data"`
	FlowParams       map[string]interface{} `json:"flow_params"`
}

// Flow completion
type FlowCompleted struct {
	BaseEvent
	FinalAction     string `json:"final_action"`
	ExecutionTimeMs int64  `json:"execution_time_ms"`
}

// Flow failure
type FlowFailed struct {
	BaseEvent
	ErrorMessage   string `json:"error_message"`
	ErrorDetails   string `json:"error_details"`
	FailedNodeType string `json:"failed_node_type"`
}

// Node Events

// Node preparation phase events
type NodePrepRequested struct {
	BaseEvent
	NodeParams map[string]interface{} `json:"node_params"`
}

type NodePrepCompleted struct {
	BaseEvent
	PrepResultRef string `json:"prep_result_ref"` // Reference to stored prep result
}

// Node execution phase events
type NodeExecRequested struct {
	BaseEvent
	PrepResultRef string `json:"prep_result_ref"`
}

type NodeExecCompleted struct {
	BaseEvent
	ExecResultRef string `json:"exec_result_ref"` // Reference to stored exec result
	RetryCount    int    `json:"retry_count"`
}

type NodeExecFailed struct {
	BaseEvent
	ErrorMessage string `json:"error_message"`
	RetryCount   int    `json:"retry_count"`
	WillRetry    bool   `json:"will_retry"`
}

// Node post-processing phase events
type NodePostRequested struct {
	BaseEvent
	PrepResultRef string `json:"prep_result_ref"`
	ExecResultRef string `json:"exec_result_ref"`
}

type NodePostCompleted struct {
	BaseEvent
	Action         string `json:"action"` // The action string to determine next node
	UpdatedDataRef string `json:"updated_data_ref"` // Reference to updated shared data
}

// Node transition events
type NodeTransitionRequested struct {
	BaseEvent
	FromNodeType string `json:"from_node_type"`
	Action       string `json:"action"`
	ToNodeType   string `json:"to_node_type"`
	ToNodeID     string `json:"to_node_id"`
}