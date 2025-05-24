// Package observability provides interfaces and implementations for monitoring PocketFlow events
package observability

import (
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/The-Pocket/PocketFlow/go/event/observability/events"
)

// Observer interface for monitoring system events via subscription
type Observer interface {
	// GetName returns the observer's identifier
	GetName() string

	// IsEnabled returns whether this observer is active
	IsEnabled() bool

	// SetEnabled enables or disables this observer
	SetEnabled(enabled bool)

	// HandleMessage processes a message from a subscribed topic
	HandleMessage(msg *message.Message) error
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
	OnFlowStarted(event events.FlowStartedEvent) error

	// OnFlowCompleted handles flow completion events
	OnFlowCompleted(event events.FlowCompletedEvent) error

	// OnFlowFailed handles flow failure events
	OnFlowFailed(event events.FlowFailedEvent) error

	// GetFlowStatus returns the current status of a flow
	GetFlowStatus(flowExecutionID string) (*FlowStatus, error)
}

// NodeTracer specifically tracks node-level events
type NodeTracer interface {
	Observer

	// OnNodeStarted handles node execution start events
	OnNodeStarted(event events.NodeStartedEvent) error

	// OnNodeCompleted handles node completion events
	OnNodeCompleted(event events.NodeCompletedEvent) error

	// OnNodeFailed handles node failure events
	OnNodeFailed(event events.NodeFailedEvent) error
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

	// IsRunning returns true if the observability system is running
	IsRunning() bool
}

// FlowStatus represents the current status of a flow execution
type FlowStatus struct {
	FlowExecutionID  string                 `json:"flow_execution_id"`
	FlowType         string                 `json:"flow_type"`
	FlowDefinitionID string                 `json:"flow_definition_id"`
	Status           string                 `json:"status"` // "running", "completed", "failed", "paused"
	StartTime        time.Time              `json:"start_time"`
	EndTime          *time.Time             `json:"end_time,omitempty"`
	Duration         time.Duration          `json:"duration"`
	NodesExecuted    int                    `json:"nodes_executed"`
	CurrentNodeID    string                 `json:"current_node_id,omitempty"`
	FinalAction      string                 `json:"final_action,omitempty"`
	FinalResult      interface{}            `json:"final_result,omitempty"`
	ErrorMessage     string                 `json:"error_message,omitempty"`
	Metadata         map[string]interface{} `json:"metadata,omitempty"`
}

// ObservabilityRunner provides a simple interface to set up observability
type ObservabilityRunner interface {
	// SetupObservability configures observability for a runner
	SetupObservability(subscriber message.Subscriber) (ObservabilityManager, error)
}