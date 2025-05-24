package core

import (
	"time"
)

// Message type constants
// Flow message types
const (
	// Message sent to request flow start
	MessageTypeFlowStartRequested = "flow.start.requested"
	// Message sent after flow initialization
	MessageTypeFlowInitialized = "flow.initialized"
	// Message sent to request flow pause
	MessageTypeFlowPauseRequested = "flow.pause.requested"
	// Message sent to confirm flow is paused
	MessageTypeFlowPaused = "flow.paused"
	// Message sent to request flow resume
	MessageTypeFlowResumeRequested = "flow.resume.requested"
	// Message sent to request flow cancellation
	MessageTypeFlowCancelRequested = "flow.cancel.requested"
	// Message sent after flow successfully completes
	MessageTypeFlowCompleted = "flow.completed"
	// Message sent after flow fails
	MessageTypeFlowFailed = "flow.failed"
)

// Node message types
const (
	// Message sent to request node execution (legacy)
	MessageTypeExecRequested = "node.exec.requested"
	// Message sent after node completes execution
	MessageTypeNodeCompleted = "node.completed"
	// Message sent after node fails execution
	MessageTypeExecFailed = "node.exec.failed"
)

// Progress message types
const (
	// Message for progress updates
	MessageTypeProgressUpdate = "progress.update"
)

// BaseMessage is the common structure for all messages
type BaseMessage struct {
	MessageType     string    `json:"message_type"`                // Type of message
	FlowType string     `json:"flow_type"`
	FlowExecutionID string    `json:"flow_execution_id"`           // ID of the overall flow execution
	NodeExecutionID string    `json:"node_execution_id,omitempty"` // ID of this specific node execution (if applicable)
	Timestamp       time.Time `json:"timestamp"`                   // When this message was created
}

// Flow Control Messages

// Message sent to request flow start
type FlowStartRequestedMessage struct {
	BaseMessage
	FlowDefinitionID  string                 `json:"flow_definition_id"`
	InitialSharedData map[string]interface{} `json:"initial_shared_data"`
}

// Message sent after flow initialization
type FlowInitializedMessage struct {
	BaseMessage
	FlowDefinitionID string `json:"flow_definition_id"`
}

// Message sent after flow successfully completes
type FlowCompletedMessage struct {
	BaseMessage
	FinalAction string      `json:"final_action"`
	FinalResult interface{} `json:"final_result,omitempty"`
	ExecutionMs int64       `json:"execution_ms"`
}

// Message sent after flow fails
type FlowFailedMessage struct {
	BaseMessage
	NodeID       string `json:"node_id,omitempty"`
	ErrorMessage string `json:"error_message"`
	ErrorDetails string `json:"error_details,omitempty"`
	FailedNodeID string `json:"failed_node_id,omitempty"`
}

// Node Execution Messages

// Message sent to request node execution
type ExecRequestedMessage struct {
	BaseMessage
	NodeType string     `json:"node_type"`
	NodeID   string     `json:"node_id"`
	Params   NodeParams `json:"params,omitempty"`
}

// Message sent after node completes execution
type NodeCompletedMessage struct {
	BaseMessage
	NodeType     string      `json:"node_type"`
	NodeID       string      `json:"node_id"`
	Action       string      `json:"action"`
	Result       interface{} `json:"result,omitempty"`
	Success      bool        `json:"success"`
	ErrorMessage string      `json:"error_message,omitempty"`
}

// Message sent after node fails execution
type ExecFailedMessage struct {
	BaseMessage
	NodeType     string `json:"node_type"`
	NodeID       string `json:"node_id"`
	ErrorMessage string `json:"error_message"`
	ErrorDetails string `json:"error_details,omitempty"`
	RetryCount   int    `json:"retry_count"`
	WillRetry    bool   `json:"will_retry"`
}

// Progress Tracking Messages

// Message for progress updates
type ProgressUpdateMessage struct {
	BaseMessage
	Status   string  `json:"status"`
	Progress float64 `json:"progress"` // 0.0 to 1.0
	Message  string  `json:"message,omitempty"`
}

// Topic Constants
const (
	// Node topics
	TopicNodeCompleted = "node.completed"
	
	// Flow topics
	TopicFlowCompleted = "flow.completed"
	TopicFlowFailed    = "flow.failed"
	
	// Progress topic
	TopicProgress = "progress"
)