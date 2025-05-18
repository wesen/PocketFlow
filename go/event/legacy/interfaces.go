package event

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

// LLMClient interface for LLM API calls
type LLMClient interface {
	Call(prompt string) (string, error)
}

// Node interface represents a node in a flow
type Node interface {
	// Get the unique ID of this node
	ID() string

	// Get the type of this node
	Type() string

	// Get the display name of this node
	Name() string

	// Get the parameters for this node
	Params() map[string]interface{}
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

// FlowBuilder interface provides a fluent API for building flows
type FlowBuilder interface {
	// Begin sets the starting node for the flow
	Begin(node Node) FlowBuilder

	// Then creates a default transition from the previous node
	Then(node Node) FlowBuilder

	// On defines an action-based transition from the previous node
	On(action string) TransitionBuilder

	// From switches the source node for subsequent transitions
	From(node Node) FlowBuilder

	// Build finalizes the flow definition
	Build() Flow
}

// TransitionBuilder defines what happens for a specific action
type TransitionBuilder interface {
	// Then sets the destination node for this action
	Then(node Node) FlowBuilder
}

// NodeWorker interface for node workers
type NodeWorker interface {
	// Get the type of node this worker handles
	NodeType() string

	// Get the list of message types this worker can handle
	SupportedMessageTypes() []string

	// Handle a message
	HandleMessage(msg interface{}) error

	// For backwards compatibility
	HandlePrepRequested(event NodePrepRequested)
	HandleExecRequested(event NodeExecRequested)
	HandlePostRequested(event NodePostRequested)
	HandleExecFailed(event NodeExecFailed)
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

// FlowRegistry interface for managing flow definitions
type FlowRegistry interface {
	// Get a flow definition by ID
	GetFlow(flowID string) (Flow, error)

	// Get a flow definition by execution ID
	GetFlowByExecutionID(executionID string) (Flow, error)

	// Register a flow definition
	RegisterFlow(flowID string, flow Flow) error

	// Map execution ID to flow ID
	MapExecutionToFlow(executionID string, flowID string)

	// For backwards compatibility
	GetFlowDefinition(flowID string) (*FlowDefinition, error)
}

// Flow definition types
type NodeDefinition struct {
	ID     string                 `json:"id"`
	Type   string                 `json:"type"`
	Params map[string]interface{} `json:"params"`
}

type TransitionDefinition struct {
	Action   string `json:"action"`
	ToNodeID string `json:"to_node_id"`
}

type FlowDefinition struct {
	ID              string                                     `json:"id"`
	Name            string                                     `json:"name"`
	StartNodeType   string                                     `json:"start_node_type"`
	StartNodeID     string                                     `json:"start_node_id"`
	StartNodeParams map[string]interface{}                     `json:"start_node_params"`
	Nodes           map[string]NodeDefinition                  `json:"nodes"`
	Transitions     map[string]map[string]TransitionDefinition `json:"transitions"` // map[nodeType][action]Transition
}

// Helper method to get the next node based on current node type and action
func (f *FlowDefinition) GetNextNode(nodeType, action string) (*NodeDefinition, bool) {
	// Check if node type exists in transitions map
	nodeTransitions, exists := f.Transitions[nodeType]
	if !exists {
		return nil, false
	}

	// Check if action exists in node transitions
	transition, exists := nodeTransitions[action]
	if !exists {
		// Try default transition
		transition, exists = nodeTransitions["default"]
		if !exists {
			return nil, false
		}
	}

	// Get next node definition
	nextNode, exists := f.Nodes[transition.ToNodeID]
	if !exists {
		return nil, false
	}

	return &nextNode, true
}
