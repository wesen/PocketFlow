// Package core provides the foundational interfaces for PocketFlow
package core

// NodeParams represents the parameters for a node instance
type NodeParams map[string]interface{}

// Node interface represents a node in a flow
type Node interface {
	// Get the unique ID of this node
	ID() string
	// Get the type of this node
	Type() string
	// Get the display name of this node
	Name() string
	// Get the parameters for this node
	Params() NodeParams
}

// Flow interface represents a flow definition
type Flow interface {
	// Get the unique ID of this flow
	ID() string
	// Get the type of this flow
	Type() string
	// Get the name of this flow
	Name() string
	// Get the starting node of this flow
	StartNode() Node
	// Get all nodes in this flow
	Nodes() map[string]Node
	// Get the next node based on current node and action
	GetNextNode(currentNodeID, action string) (Node, bool)
	// Generate a visualization of this flow
	Visualize() string
}

// NodeWorker interface for node workers
type NodeWorker interface {
	// Get the type of node this worker handles
	NodeType() string

	// Get the list of message types this worker can handle
	SupportedMessageTypes() []string

	// Handle a message
	HandleMessage(msg interface{}) error

	// Legacy methods
	HandlePrepRequested(event NodePrepRequested)
	HandleExecRequested(event NodeExecRequested)
	HandlePostRequested(event NodePostRequested)
	HandleExecFailed(event NodeExecFailed)

	NewNode(params NodeParams) Node
}

// FlowWorker interface for flow workers
type FlowWorker interface {
	// Get the type of flow this worker handles
	FlowType() string
	// Get the list of message types this worker can handle
	SupportedMessageTypes() []string
	// Handle a message
	HandleMessage(msg interface{}) error
	// Handle a node completion message
	HandleNodeCompletedMessage(msg interface{}) error
}

// FlowBuilder interface provides a fluent API for building flows
type FlowBuilder interface {
	// Begin sets the starting node for the flow
	Begin(node Node) FlowBuilder
	// Then creates a default transition from the previous node
	Then(node Node) FlowBuilder
	// On defines an action-based transition from the current node to the specified target node
	On(action string, targetNode Node) FlowBuilder
	// From switches the source node for subsequent transitions
	From(node Node) FlowBuilder
	// Build finalizes the flow definition
	Build() Flow
}

// EventPublisher interface for publishing events
type EventPublisher interface {
	Publish(topic string, event interface{}) error
}

// EventSubscriber interface for subscribing to events
type EventSubscriber interface {
	Subscribe(topic string, handler func([]byte)) error
}

// StateStore interface for storing and retrieving state
type StateStore interface {
	// Store shared data for a flow execution
	StoreSharedData(flowExecutionID string, data map[string]interface{}) error
	// Get shared data for a flow execution
	GetSharedData(flowExecutionID string) (map[string]interface{}, error)
	// Store the result of a node's prep/exec/post step
	StoreNodeResult(nodeExecutionID string, stepType string, result interface{}) (string, error)
	// Get a stored result by reference
	GetNodeResult(resultRef string) (interface{}, error)
	// Store flow definition
	StoreFlowDefinition(flowID string, definition Flow) error
	// Get flow definition
	GetFlowDefinition(flowID string) (Flow, error)
	// Get flow definition by execution ID
	GetFlowDefinitionByExecutionID(executionID string) (Flow, error)
	// Store a mapping between execution ID and definition ID
	StoreFlowExecution(executionID string, definitionID string) error
	// Update shared data with node result
	UpdateSharedData(flowExecutionID string, nodeID string, result interface{}) error
}

// FlowRegistry interface for managing flow definitions
type FlowRegistry interface {
	// Get a flow definition by ID
	GetFlow(flowID string) (Flow, error)
	// Get a flow definition by execution ID
	GetFlowByExecutionID(executionID string) (Flow, error)
	// Register a flow definition
	RegisterFlow(flowID string, flow Flow) error
	// Legacy methods
	GetFlowDefinition(flowID string) (*FlowDefinition, error)
}

// NodeContext provides access to the execution context of a node
type NodeContext struct {
	FlowExecutionID string
	NodeExecutionID string
	NodeID          string
	NodeType        string
	Params          NodeParams
	SharedData      map[string]interface{}
	StateStore      StateStore
}

// SimpleNodeHandler is a simplified interface for node handlers
type SimpleNodeHandler interface {
	// Prep handles the preparation phase
	Prep(ctx NodeContext) (interface{}, error)

	// Exec handles the execution phase
	Exec(ctx NodeContext, prepResult interface{}) (interface{}, error)

	// Post handles the post-processing phase and returns the action to take
	Post(ctx NodeContext, prepResult, execResult interface{}) (string, interface{}, error)
}

// NodeBuilder provides a fluent API for building nodes
type NodeBuilder interface {
	// WithName sets the display name for the node
	WithName(name string) NodeBuilder

	// WithParam adds a parameter to the node
	WithParam(key string, value interface{}) NodeBuilder

	// WithPrep sets the prep handler function
	WithPrep(handler func(ctx NodeContext) (interface{}, error)) NodeBuilder

	// WithExec sets the exec handler function
	WithExec(handler func(ctx NodeContext, prepResult interface{}) (interface{}, error)) NodeBuilder

	// WithPost sets the post handler function
	WithPost(handler func(ctx NodeContext, prepResult, execResult interface{}) (string, interface{}, error)) NodeBuilder

	// Build creates a Node instance with the specified configuration
	Build() NodeWorker
}

// Legacy placeholder types
type NodePrepRequested struct{}
type NodeExecRequested struct{}
type NodePostRequested struct{}
type NodeExecFailed struct{}
type FlowDefinition struct{}
