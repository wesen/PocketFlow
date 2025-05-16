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
	StoreFlowDefinition(flowID string, definition *FlowDefinition) error
	
	// Get flow definition
	GetFlowDefinition(flowID string) (*FlowDefinition, error)
	
	// Get flow definition by execution ID
	GetFlowDefinitionByExecutionID(executionID string) (*FlowDefinition, error)
	
	// Store a mapping between execution ID and definition ID
	StoreFlowExecution(executionID string, definitionID string) error
}

// NodeWorker interface for node workers
type NodeWorker interface {
	HandlePrepRequested(event NodePrepRequested)
	HandleExecRequested(event NodeExecRequested)
	HandlePostRequested(event NodePostRequested)
	HandleExecFailed(event NodeExecFailed)
}

// LLMClient interface for LLM API calls
type LLMClient interface {
	Call(prompt string) (string, error)
}

// FlowRegistry interface for managing flow definitions
type FlowRegistry interface {
	GetFlowDefinition(flowID string) (*FlowDefinition, error)
	GetFlowDefinitionByExecutionID(executionID string) (*FlowDefinition, error)
	RegisterFlow(flowID string, definition *FlowDefinition) error
}

// Flow definition types
type NodeDefinition struct {
	ID     string                 `json:"id"`
	Type   string                 `json:"type"`
	Params map[string]interface{} `json:"params"`
}

type TransitionDefinition struct {
	Action  string `json:"action"`
	ToNodeID string `json:"to_node_id"`
}

type FlowDefinition struct {
	ID            string                                  `json:"id"`
	Name          string                                  `json:"name"`
	StartNodeType string                                  `json:"start_node_type"`
	StartNodeID   string                                  `json:"start_node_id"`
	StartNodeParams map[string]interface{}                 `json:"start_node_params"`
	Nodes         map[string]NodeDefinition               `json:"nodes"`
	Transitions   map[string]map[string]TransitionDefinition `json:"transitions"` // map[nodeType][action]Transition
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