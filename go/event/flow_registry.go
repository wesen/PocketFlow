package event

import (
	"fmt"
	"sync"
)

// InMemoryFlowRegistry implements the FlowRegistry interface
type InMemoryFlowRegistry struct {
	flows        map[string]Flow
	execToFlowID map[string]string
	mutex        sync.RWMutex
}

// NewInMemoryFlowRegistry creates a new InMemoryFlowRegistry instance
func NewInMemoryFlowRegistry() *InMemoryFlowRegistry {
	return &InMemoryFlowRegistry{
		flows:        make(map[string]Flow),
		execToFlowID: make(map[string]string),
		mutex:        sync.RWMutex{},
	}
}

// RegisterFlow registers a flow definition with the registry
func (r *InMemoryFlowRegistry) RegisterFlow(flowID string, flow Flow) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.flows[flowID] = flow
	return nil
}

// GetFlow retrieves a flow definition from the registry
func (r *InMemoryFlowRegistry) GetFlow(flowID string) (Flow, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	flow, ok := r.flows[flowID]
	if !ok {
		return nil, fmt.Errorf("flow definition not found: %s", flowID)
	}

	return flow, nil
}

// GetFlowByExecutionID retrieves a flow definition by execution ID
func (r *InMemoryFlowRegistry) GetFlowByExecutionID(executionID string) (Flow, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	flowID, ok := r.execToFlowID[executionID]
	if !ok {
		return nil, fmt.Errorf("no flow found for execution ID: %s", executionID)
	}

	flow, ok := r.flows[flowID]
	if !ok {
		return nil, fmt.Errorf("flow definition not found: %s", flowID)
	}

	return flow, nil
}

// MapExecutionToFlow maps an execution ID to a flow ID
func (r *InMemoryFlowRegistry) MapExecutionToFlow(executionID string, flowID string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.execToFlowID[executionID] = flowID
}

// For backward compatibility

// GetFlowDefinition retrieves a flow definition from the registry (backward compatibility)
func (r *InMemoryFlowRegistry) GetFlowDefinition(flowID string) (*FlowDefinition, error) {
	flow, err := r.GetFlow(flowID)
	if err != nil {
		return nil, err
	}

	// Create a FlowDefinition based on the Flow interface
	return &FlowDefinition{
		ID:            flow.ID(),
		Name:          flow.Name(),
		StartNodeType: flow.StartNode().Type(),
		StartNodeID:   flow.StartNode().ID(),
	}, nil
}
