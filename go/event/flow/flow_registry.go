package flow

import (
	"fmt"
	"sync"

	"github.com/The-Pocket/PocketFlow/go/event/core"
)

// InMemoryFlowRegistry maintains flow definitions in memory
type InMemoryFlowRegistry struct {
	flows        map[string]core.Flow
	execToFlowID map[string]string
	mutex        sync.RWMutex
}

// NewInMemoryFlowRegistry creates a new in-memory flow registry
func NewInMemoryFlowRegistry() *InMemoryFlowRegistry {
	return &InMemoryFlowRegistry{
		flows:        make(map[string]core.Flow),
		execToFlowID: make(map[string]string),
	}
}

// GetFlow gets a flow definition by ID
func (r *InMemoryFlowRegistry) GetFlow(flowID string) (core.Flow, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	
	if flow, exists := r.flows[flowID]; exists {
		return flow, nil
	}
	
	return nil, fmt.Errorf("flow not found: %s", flowID)
}

// GetFlowByExecutionID gets a flow definition by execution ID
func (r *InMemoryFlowRegistry) GetFlowByExecutionID(executionID string) (core.Flow, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	
	if flowID, exists := r.execToFlowID[executionID]; exists {
		if flow, found := r.flows[flowID]; found {
			return flow, nil
		}
	}
	
	return nil, fmt.Errorf("flow not found for execution: %s", executionID)
}

// RegisterFlow registers a flow definition
func (r *InMemoryFlowRegistry) RegisterFlow(flowID string, flow core.Flow) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	
	r.flows[flowID] = flow
	return nil
}

// MapExecutionToFlow maps an execution ID to a flow definition ID
func (r *InMemoryFlowRegistry) MapExecutionToFlow(executionID, flowID string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	
	r.execToFlowID[executionID] = flowID
}

// Legacy method
func (r *InMemoryFlowRegistry) GetFlowDefinition(flowID string) (*core.FlowDefinition, error) {
	return nil, fmt.Errorf("deprecated: use GetFlow instead")
}