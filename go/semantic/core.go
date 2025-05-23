// Package semantic provides the core semantic state management for PocketFlow
package semantic

import (
	"fmt"
	"reflect"
	"sync"
	"time"
)

// StateStore interface for semantic data management
type StateStore interface {
	// Semantic data access - now requires flowExecutionID
	GetBySemantic(flowExecutionID, key string) (interface{}, bool)
	SetSemantic(flowExecutionID, key string, value interface{}) error

	// Metadata access - now requires flowExecutionID  
	GetMetadata(flowExecutionID, key string) (*DataMetadata, bool)
	GetAllMetadata(flowExecutionID string) map[string]DataMetadata

	// Update with semantic registration
	UpdateWithSemantic(flowExecutionID, nodeID string, value interface{}, semanticKeys []string, tags []string) error

	// Core state operations
	GetSharedData(flowExecutionID string) (map[string]interface{}, error)
	UpdateSharedData(flowExecutionID, key string, value interface{}) error
}

// DataMetadata for each stored value
type DataMetadata struct {
	ProducerNodeID   string    `json:"producer_node_id"`
	ProducerNodeType string    `json:"producer_node_type"`
	Timestamp        time.Time `json:"timestamp"`
	DataType         string    `json:"data_type"`
	SemanticKeys     []string  `json:"semantic_keys"`
	FlowExecutionID  string    `json:"flow_execution_id"`
}

// TypedValue for storing values with semantic context
type TypedValue struct {
	Value       interface{}
	SemanticKey string
	Timestamp   time.Time
}

// NodeContext for semantic state access
type NodeContext struct {
	FlowExecutionID string
	NodeID          string
	NodeType        string
	Params          map[string]interface{}

	// Semantic state access
	SemanticData *SemanticDataAccessor
}

// SemanticDataAccessor with convenience methods
type SemanticDataAccessor struct {
	store           StateStore
	flowExecutionID string
}

func NewSemanticDataAccessor(store StateStore, flowExecutionID string) *SemanticDataAccessor {
	return &SemanticDataAccessor{
		store:           store,
		flowExecutionID: flowExecutionID,
	}
}

func (sda *SemanticDataAccessor) UserInput() (string, error) {
	value, exists := sda.store.GetBySemantic(sda.flowExecutionID, "user_input")
	if !exists {
		return "", fmt.Errorf("user_input not found")
	}
	if str, ok := value.(string); ok {
		return str, nil
	}
	return "", fmt.Errorf("user_input is not a string")
}

func (sda *SemanticDataAccessor) Intent() (string, error) {
	value, exists := sda.store.GetBySemantic(sda.flowExecutionID, "intent")
	if !exists {
		return "", fmt.Errorf("intent not found")
	}
	if str, ok := value.(string); ok {
		return str, nil
	}
	return "", fmt.Errorf("intent is not a string")
}

func (sda *SemanticDataAccessor) Response() (string, error) {
	value, exists := sda.store.GetBySemantic(sda.flowExecutionID, "response")
	if !exists {
		return "", fmt.Errorf("response not found")
	}
	if str, ok := value.(string); ok {
		return str, nil
	}
	return "", fmt.Errorf("response is not a string")
}

func (sda *SemanticDataAccessor) GetString(key string) (string, error) {
	value, exists := sda.store.GetBySemantic(sda.flowExecutionID, key)
	if !exists {
		return "", fmt.Errorf("key '%s' not found", key)
	}
	if str, ok := value.(string); ok {
		return str, nil
	}
	return "", fmt.Errorf("key '%s' is not a string", key)
}

func (sda *SemanticDataAccessor) GetInt(key string) (int, error) {
	value, exists := sda.store.GetBySemantic(sda.flowExecutionID, key)
	if !exists {
		return 0, fmt.Errorf("key '%s' not found", key)
	}
	if intValue, ok := value.(int); ok {
		return intValue, nil
	}
	return 0, fmt.Errorf("key '%s' is not an int", key)
}

func (sda *SemanticDataAccessor) GetBool(key string) (bool, error) {
	value, exists := sda.store.GetBySemantic(sda.flowExecutionID, key)
	if !exists {
		return false, fmt.Errorf("key '%s' not found", key)
	}
	if boolValue, ok := value.(bool); ok {
		return boolValue, nil
	}
	return false, fmt.Errorf("key '%s' is not a bool", key)
}

func (sda *SemanticDataAccessor) GetFloat64(key string) (float64, error) {
	value, exists := sda.store.GetBySemantic(sda.flowExecutionID, key)
	if !exists {
		return 0, fmt.Errorf("key '%s' not found", key)
	}
	if floatValue, ok := value.(float64); ok {
		return floatValue, nil
	}
	return 0, fmt.Errorf("key '%s' is not a float64", key)
}

// GetStore provides access to the underlying StateStore
func (sda *SemanticDataAccessor) GetStore() StateStore {
	return sda.store
}

// SemanticNodeHandler interface for nodes that declare semantic outputs
type SemanticNodeHandler interface {
	DeclareOutputs() []SemanticOutput
}

type SemanticOutput struct {
	Key         string   `json:"key"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
}

// NodeHandler with semantic capabilities
type NodeHandler interface {
	Prep(ctx NodeContext) (interface{}, error)
	Exec(ctx NodeContext, prepResult interface{}) (interface{}, error)
	Post(ctx NodeContext, prepResult, execResult interface{}) (string, interface{}, []SemanticOutput, error)
}

// LayeredSharedState provides semantic-first state management
type LayeredSharedState struct {
	// Layer 1: Semantic mappings
	SemanticLayer map[string]interface{} // semantic_key -> value

	// Layer 4: Metadata
	Metadata map[string]DataMetadata // semantic_key -> metadata

	// Flow organization
	FlowData map[string]*LayeredSharedState // flowExecutionID -> state

	// Synchronization
	mutex sync.RWMutex
}

// LayeredStateStore implements the StateStore interface with semantic capabilities
type LayeredStateStore struct {
	globalState *LayeredSharedState
	mutex       sync.RWMutex
}

// NewLayeredStateStore creates a new semantic state store
func NewLayeredStateStore() *LayeredStateStore {
	return &LayeredStateStore{
		globalState: &LayeredSharedState{
			SemanticLayer: make(map[string]interface{}),
			Metadata:      make(map[string]DataMetadata),
			FlowData:      make(map[string]*LayeredSharedState),
		},
	}
}

// GetBySemantic retrieves a value by semantic key for a specific flow execution
func (lss *LayeredStateStore) GetBySemantic(flowExecutionID, key string) (interface{}, bool) {
	lss.mutex.RLock()
	defer lss.mutex.RUnlock()

	// Get flow-specific state
	flowState, exists := lss.globalState.FlowData[flowExecutionID]
	if !exists {
		return nil, false
	}

	value, exists := flowState.SemanticLayer[key]
	return value, exists
}

// SetSemantic stores a value with a semantic key for a specific flow execution
func (lss *LayeredStateStore) SetSemantic(flowExecutionID, key string, value interface{}) error {
	lss.mutex.Lock()
	defer lss.mutex.Unlock()

	// Ensure flow state exists
	if lss.globalState.FlowData[flowExecutionID] == nil {
		lss.globalState.FlowData[flowExecutionID] = &LayeredSharedState{
			SemanticLayer: make(map[string]interface{}),
			Metadata:      make(map[string]DataMetadata),
			FlowData:      make(map[string]*LayeredSharedState),
		}
	}

	lss.globalState.FlowData[flowExecutionID].SemanticLayer[key] = value

	return nil
}

// GetMetadata retrieves metadata for a semantic key for a specific flow execution
func (lss *LayeredStateStore) GetMetadata(flowExecutionID, key string) (*DataMetadata, bool) {
	lss.mutex.RLock()
	defer lss.mutex.RUnlock()

	// Get flow-specific state
	flowState, exists := lss.globalState.FlowData[flowExecutionID]
	if !exists {
		return nil, false
	}

	metadata, exists := flowState.Metadata[key]
	if !exists {
		return nil, false
	}

	return &metadata, true
}

// GetAllMetadata retrieves all metadata for a specific flow execution
func (lss *LayeredStateStore) GetAllMetadata(flowExecutionID string) map[string]DataMetadata {
	lss.mutex.RLock()
	defer lss.mutex.RUnlock()

	result := make(map[string]DataMetadata)
	
	// Get flow-specific state
	flowState, exists := lss.globalState.FlowData[flowExecutionID]
	if !exists {
		return result
	}

	for key, metadata := range flowState.Metadata {
		result[key] = metadata
	}

	return result
}

// UpdateWithSemantic updates state with semantic registration for a specific flow execution
func (lss *LayeredStateStore) UpdateWithSemantic(flowExecutionID, nodeID string, value interface{}, semanticKeys []string, tags []string) error {
	lss.mutex.Lock()
	defer lss.mutex.Unlock()

	// Ensure flow state exists
	if lss.globalState.FlowData[flowExecutionID] == nil {
		lss.globalState.FlowData[flowExecutionID] = &LayeredSharedState{
			SemanticLayer: make(map[string]interface{}),
			Metadata:      make(map[string]DataMetadata),
			FlowData:      make(map[string]*LayeredSharedState),
		}
	}

	flowState := lss.globalState.FlowData[flowExecutionID]
	timestamp := time.Now()
	valueType := reflect.TypeOf(value)

	// Store value under all semantic keys
	for _, key := range semanticKeys {
		flowState.SemanticLayer[key] = value

		// Create metadata
		metadata := DataMetadata{
			ProducerNodeID:   nodeID,
			ProducerNodeType: "", // Will be filled by calling code if available
			Timestamp:        timestamp,
			DataType:         valueType.String(),
			SemanticKeys:     semanticKeys,
			FlowExecutionID:  flowExecutionID,
		}
		flowState.Metadata[key] = metadata
	}

	return nil
}

// GetSharedData retrieves shared data for a flow execution
func (lss *LayeredStateStore) GetSharedData(flowExecutionID string) (map[string]interface{}, error) {
	lss.mutex.RLock()
	defer lss.mutex.RUnlock()

	if flowState, exists := lss.globalState.FlowData[flowExecutionID]; exists {
		// Convert semantic layer to traditional shared data format
		result := make(map[string]interface{})
		for key, value := range flowState.SemanticLayer {
			result[key] = value
		}
		return result, nil
	}

	// Return empty map if flow execution not found
	return make(map[string]interface{}), nil
}

// UpdateSharedData updates shared data for a flow execution
func (lss *LayeredStateStore) UpdateSharedData(flowExecutionID, key string, value interface{}) error {
	lss.mutex.Lock()
	defer lss.mutex.Unlock()

	// Ensure flow state exists
	if _, exists := lss.globalState.FlowData[flowExecutionID]; !exists {
		lss.globalState.FlowData[flowExecutionID] = &LayeredSharedState{
			SemanticLayer: make(map[string]interface{}),
			Metadata:      make(map[string]DataMetadata),
			FlowData:      make(map[string]*LayeredSharedState),
		}
	}

	// Update the semantic layer
	lss.globalState.FlowData[flowExecutionID].SemanticLayer[key] = value

	return nil
}
