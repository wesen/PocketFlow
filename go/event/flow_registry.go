package event

import (
	"fmt"
	"sync"
)

// In-memory implementation of the FlowRegistry interface
type InMemoryFlowRegistry struct {
	flowDefs        map[string]*FlowDefinition
	executionToFlow map[string]string
	mutex           sync.RWMutex
}

func NewInMemoryFlowRegistry() *InMemoryFlowRegistry {
	return &InMemoryFlowRegistry{
		flowDefs:        make(map[string]*FlowDefinition),
		executionToFlow: make(map[string]string),
		mutex:           sync.RWMutex{},
	}
}

func (r *InMemoryFlowRegistry) RegisterFlow(flowID string, definition *FlowDefinition) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	
	// Make sure flowID matches definition.ID
	if definition.ID != flowID {
		return fmt.Errorf("flow ID mismatch: %s vs %s", flowID, definition.ID)
	}
	
	r.flowDefs[flowID] = definition
	return nil
}

func (r *InMemoryFlowRegistry) GetFlowDefinition(flowID string) (*FlowDefinition, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	
	def, exists := r.flowDefs[flowID]
	if !exists {
		return nil, fmt.Errorf("flow definition not found: %s", flowID)
	}
	
	return def, nil
}

func (r *InMemoryFlowRegistry) GetFlowDefinitionByExecutionID(executionID string) (*FlowDefinition, error) {
	r.mutex.RLock()
	flowID, exists := r.executionToFlow[executionID]
	r.mutex.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("no flow definition found for execution ID: %s", executionID)
	}
	
	return r.GetFlowDefinition(flowID)
}

func (r *InMemoryFlowRegistry) RegisterExecution(executionID string, flowID string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	
	r.executionToFlow[executionID] = flowID
}