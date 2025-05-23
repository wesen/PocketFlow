# Robust Shared State Design Patterns for PocketFlow Go

## Problem Summary

The current shared state system stores node results using nodeID as keys, making data access unpredictable and requiring fragile iteration-based patterns. This document outlines design patterns to create a more robust shared state abstraction.

## Current Issues

1. **Opaque Keys**: NodeIDs are UUIDs, making data access unpredictable
2. **No Semantic Naming**: Can't access data by logical names like "user_input"
3. **Fragile Access Patterns**: Require iteration and heuristic filtering
4. **No Type Safety**: Runtime type checking with potential panics
5. **Poor Debugging**: Can't easily inspect shared state structure
6. **No Schema Validation**: No guarantees about data structure

## Design Pattern Options

### 1. Semantic Key Mapping Pattern

**Concept**: Allow nodes to declare semantic output names that get mapped to their nodeIDs.

```go
type NodeOutputMapping struct {
    NodeID      string
    SemanticKey string
    DataType    reflect.Type
    Description string
}

type EnhancedSharedState struct {
    RawData     map[string]interface{}          // nodeID -> value (existing)
    SemanticMap map[string]string              // semantic_key -> nodeID
    TypeMap     map[string]reflect.Type        // nodeID -> expected_type
    Metadata    map[string]NodeOutputMapping   // nodeID -> mapping info
}

// Node declares what it produces
type SemanticNodeHandler interface {
    NodeHandler
    DeclareOutputs() []NodeOutputMapping
}

// Enhanced context with semantic access
type EnhancedNodeContext struct {
    NodeContext
    SemanticData *EnhancedSharedState
}

func (ctx *EnhancedNodeContext) GetByKey(key string) (interface{}, bool) {
    if nodeID, exists := ctx.SemanticData.SemanticMap[key]; exists {
        value, ok := ctx.SemanticData.RawData[nodeID]
        return value, ok
    }
    return nil, false
}

func (ctx *EnhancedNodeContext) GetTyped[T any](key string) (T, error) {
    var zero T
    value, exists := ctx.GetByKey(key)
    if !exists {
        return zero, fmt.Errorf("key %s not found", key)
    }
    if typed, ok := value.(T); ok {
        return typed, nil
    }
    return zero, fmt.Errorf("key %s has wrong type", key)
}
```

**Usage Example**:
```go
type UserInputHandler struct{}

func (h *UserInputHandler) DeclareOutputs() []NodeOutputMapping {
    return []NodeOutputMapping{
        {
            SemanticKey: "user_input",
            DataType:    reflect.TypeOf(""),
            Description: "Raw user input text",
        },
    }
}

func (h *UserInputHandler) Post(ctx NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
    // Framework automatically maps this output to "user_input" key
    return "default", execResult, nil
}

// Consumer node
func (h *IntentHandler) Prep(ctx EnhancedNodeContext) (interface{}, error) {
    userInput, err := ctx.GetTyped[string]("user_input")
    if err != nil {
        return nil, err
    }
    return userInput, nil
}
```

**Pros**: 
- Predictable semantic access
- Backward compatible with existing nodeID storage
- Type safety with generics
- Clear data contracts

**Cons**: 
- Requires node modification to declare outputs
- Additional mapping overhead
- Potential key conflicts between nodes

### 2. Typed Context Pattern

**Concept**: Define strongly-typed contexts for specific flow types with known data fields.

```go
// Flow-specific contexts
type ChatFlowContext struct {
    UserInput    string    `json:"user_input"`
    Intent       string    `json:"intent"`
    Response     string    `json:"response"`
    Confidence   float64   `json:"confidence"`
    Timestamp    time.Time `json:"timestamp"`
}

type WorkflowContext struct {
    TaskID       string                 `json:"task_id"`
    Status       string                 `json:"status"`
    Progress     float64                `json:"progress"`
    Results      map[string]interface{} `json:"results"`
    Errors       []string              `json:"errors"`
}

// Context manager
type TypedContextManager struct {
    flowType    string
    rawData     map[string]interface{}
    contextType reflect.Type
}

func (tcm *TypedContextManager) GetContext() (interface{}, error) {
    // Populate typed context from raw shared data
    contextValue := reflect.New(tcm.contextType).Elem()
    
    for i := 0; i < contextValue.NumField(); i++ {
        field := contextValue.Type().Field(i)
        jsonTag := field.Tag.Get("json")
        
        if value := tcm.findValueByType(field.Type); value != nil {
            contextValue.Field(i).Set(reflect.ValueOf(value))
        }
    }
    
    return contextValue.Interface(), nil
}

// Enhanced node handler with typed context
type TypedNodeHandler[T any] interface {
    PrepTyped(ctx T) (interface{}, error)
    ExecTyped(ctx T, prepResult interface{}) (interface{}, error)
    PostTyped(ctx T, prepResult, execResult interface{}) (string, interface{}, error)
}
```

**Usage Example**:
```go
type ChatIntentHandler struct{}

func (h *ChatIntentHandler) PrepTyped(ctx ChatFlowContext) (interface{}, error) {
    if ctx.UserInput == "" {
        return nil, fmt.Errorf("user input required")
    }
    return ctx.UserInput, nil
}

func (h *ChatIntentHandler) ExecTyped(ctx ChatFlowContext, prepResult interface{}) (interface{}, error) {
    userInput := prepResult.(string)
    intent := classifyIntent(userInput)
    return intent, nil
}

func (h *ChatIntentHandler) PostTyped(ctx ChatFlowContext, prepResult, execResult interface{}) (string, interface{}, error) {
    intent := execResult.(string)
    if intent == "general" {
        return "general_path", intent, nil
    }
    return "specific_path", intent, nil
}
```

**Pros**: 
- Strong type safety at compile time
- Clear data contracts per flow type
- IDE support with autocomplete
- Easy testing with known structures

**Cons**: 
- Flow-specific implementations required
- Less flexible for dynamic data
- Requires pre-planning of all data fields

### 3. Data Contract Pattern

**Concept**: Define explicit contracts for what data each node provides and consumes.

```go
type DataContract struct {
    Provides []DataSpec `json:"provides"`
    Requires []DataSpec `json:"requires"`
}

type DataSpec struct {
    Key         string      `json:"key"`
    Type        string      `json:"type"`
    Description string      `json:"description"`
    Optional    bool        `json:"optional"`
    Schema      interface{} `json:"schema,omitempty"`
}

type ContractAwareNode interface {
    Node
    GetDataContract() DataContract
}

type ContractRegistry struct {
    contracts map[string]DataContract  // nodeType -> contract
    flowSpec  FlowDataSpec
}

type FlowDataSpec struct {
    FlowType     string               `json:"flow_type"`
    DataFlow     map[string][]string  `json:"data_flow"`  // nodeID -> []required_keys
    Validation   []ValidationRule     `json:"validation"`
}

type ValidationRule struct {
    NodeID      string `json:"node_id"`
    RequiredKey string `json:"required_key"`
    SourceNodes []string `json:"source_nodes"`
}

// Enhanced state manager with contract validation
type ContractStateManager struct {
    StateStore
    registry *ContractRegistry
}

func (csm *ContractStateManager) ValidateDataFlow(flowID string) error {
    // Validate that all required data will be available
    spec := csm.registry.flowSpec
    
    for nodeID, requiredKeys := range spec.DataFlow {
        for _, key := range requiredKeys {
            if !csm.canSatisfyRequirement(key, nodeID) {
                return fmt.Errorf("node %s requires key %s but no provider found", nodeID, key)
            }
        }
    }
    return nil
}

func (csm *ContractStateManager) GetContractedData(nodeID string, key string) (interface{}, error) {
    contract := csm.registry.contracts[nodeID]
    
    // Find requirement spec
    var reqSpec *DataSpec
    for _, req := range contract.Requires {
        if req.Key == key {
            reqSpec = &req
            break
        }
    }
    
    if reqSpec == nil {
        return nil, fmt.Errorf("node %s has no contract requirement for key %s", nodeID, key)
    }
    
    // Find provider
    value, provider := csm.findProvider(key)
    if value == nil {
        if !reqSpec.Optional {
            return nil, fmt.Errorf("required key %s not provided for node %s", key, nodeID)
        }
        return nil, nil
    }
    
    // Validate type
    if !csm.validateType(value, reqSpec.Type) {
        return nil, fmt.Errorf("key %s has wrong type for node %s", key, nodeID)
    }
    
    return value, nil
}
```

**Usage Example**:
```go
type UserInputNode struct {
    BaseNode
}

func (n *UserInputNode) GetDataContract() DataContract {
    return DataContract{
        Provides: []DataSpec{
            {
                Key:         "user_input",
                Type:        "string",
                Description: "Raw user input text",
                Optional:    false,
            },
        },
        Requires: []DataSpec{},
    }
}

type IntentClassifierNode struct {
    BaseNode
}

func (n *IntentClassifierNode) GetDataContract() DataContract {
    return DataContract{
        Provides: []DataSpec{
            {
                Key:         "intent",
                Type:        "string", 
                Description: "Classified user intent",
                Optional:    false,
            },
            {
                Key:         "confidence",
                Type:        "float64",
                Description: "Classification confidence score",
                Optional:    true,
            },
        },
        Requires: []DataSpec{
            {
                Key:         "user_input",
                Type:        "string",
                Description: "User input to classify",
                Optional:    false,
            },
        },
    }
}

// Flow definition with contract validation
flow := NewFlowBuilder().
    Begin(userInputNode).
    Then(intentNode).
    WithContractValidation(true).
    Build()

// This would fail at build time if contracts don't match
```

**Pros**: 
- Explicit data dependencies
- Compile-time validation of data flow
- Self-documenting flows
- Better error messages

**Cons**: 
- Requires significant boilerplate
- Complex validation logic
- May be over-engineering for simple flows

### 4. Query/Accessor Pattern

**Concept**: Provide rich query methods to find data in shared state without knowing keys.

```go
type SharedStateQuery struct {
    state map[string]interface{}
}

func (q *SharedStateQuery) FindByType[T any]() (T, bool) {
    var zero T
    targetType := reflect.TypeOf(zero)
    
    for _, value := range q.state {
        if reflect.TypeOf(value) == targetType {
            if typed, ok := value.(T); ok {
                return typed, true
            }
        }
    }
    return zero, false
}

func (q *SharedStateQuery) FindAllByType[T any]() []T {
    var results []T
    targetType := reflect.TypeOf(*new(T))
    
    for _, value := range q.state {
        if reflect.TypeOf(value) == targetType {
            if typed, ok := value.(T); ok {
                results = append(results, typed)
            }
        }
    }
    return results
}

func (q *SharedStateQuery) FindByPredicate(predicate func(interface{}) bool) []interface{} {
    var results []interface{}
    for _, value := range q.state {
        if predicate(value) {
            results = append(results, value)
        }
    }
    return results
}

func (q *SharedStateQuery) FindString(filter func(string) bool) (string, bool) {
    for _, value := range q.state {
        if str, ok := value.(string); ok && filter(str) {
            return str, true
        }
    }
    return "", false
}

func (q *SharedStateQuery) FindLatestByType[T any]() (T, bool) {
    // Could add timestamp tracking to find most recent
    return q.FindByType[T]()
}

type EnhancedNodeContext struct {
    NodeContext
    Query *SharedStateQuery
}
```

**Usage Example**:
```go
func (h *GeneralHandler) Prep(ctx EnhancedNodeContext) (interface{}, error) {
    // Find user input by characteristics
    userInput, found := ctx.Query.FindString(func(s string) bool {
        return len(s) > 0 && 
               !strings.Contains(s, "_intent") && 
               !strings.HasPrefix(s, "20") // not a timestamp
    })
    
    if !found {
        return nil, fmt.Errorf("user input not found")
    }
    
    return userInput, nil
}

func (h *ProcessorHandler) Prep(ctx EnhancedNodeContext) (interface{}, error) {
    // Find all classification results
    classifications := ctx.Query.FindAllByType[ClassificationResult]()
    
    // Find the most confident one
    var bestResult ClassificationResult
    for _, result := range classifications {
        if result.Confidence > bestResult.Confidence {
            bestResult = result
        }
    }
    
    return bestResult, nil
}

func (h *DataAggregatorHandler) Prep(ctx EnhancedNodeContext) (interface{}, error) {
    // Find all numeric results for aggregation
    numbers := ctx.Query.FindByPredicate(func(v interface{}) bool {
        switch v.(type) {
        case int, int64, float64, float32:
            return true
        default:
            return false
        }
    })
    
    return numbers, nil
}
```

**Pros**: 
- Flexible querying without knowing exact keys
- Type-safe access patterns
- Works with existing storage model
- Minimal changes to current architecture

**Cons**: 
- Still requires heuristics for data identification
- Performance overhead from iteration
- Ambiguous results if multiple items match

### 5. Layered State Pattern

**Concept**: Maintain both raw nodeID storage and a semantic layer on top.

```go
type LayeredSharedState struct {
    // Layer 1: Raw storage (existing pattern)
    RawData map[string]interface{} // nodeID -> value
    
    // Layer 2: Semantic mappings
    SemanticLayer map[string]interface{} // semantic_key -> value
    
    // Layer 3: Typed collections
    TypedCollections map[reflect.Type][]interface{} // type -> []values
    
    // Layer 4: Metadata
    Metadata map[string]DataMetadata // key -> metadata
}

type DataMetadata struct {
    ProducerNodeID   string    `json:"producer_node_id"`
    ProducerNodeType string    `json:"producer_node_type"`
    Timestamp        time.Time `json:"timestamp"`
    DataType         string    `json:"data_type"`
    SemanticKeys     []string  `json:"semantic_keys"`
    Tags             []string  `json:"tags"`
}

type LayeredStateManager struct {
    state *LayeredSharedState
    mutex sync.RWMutex
}

func (lsm *LayeredStateManager) StoreResult(nodeID, nodeType string, value interface{}, semanticKeys []string, tags []string) error {
    lsm.mutex.Lock()
    defer lsm.mutex.Unlock()
    
    // Layer 1: Raw storage (preserve existing behavior)
    lsm.state.RawData[nodeID] = value
    
    // Layer 2: Semantic mappings
    for _, key := range semanticKeys {
        lsm.state.SemanticLayer[key] = value
    }
    
    // Layer 3: Type collections
    valueType := reflect.TypeOf(value)
    lsm.state.TypedCollections[valueType] = append(lsm.state.TypedCollections[valueType], value)
    
    // Layer 4: Metadata
    lsm.state.Metadata[nodeID] = DataMetadata{
        ProducerNodeID:   nodeID,
        ProducerNodeType: nodeType,
        Timestamp:        time.Now(),
        DataType:         valueType.String(),
        SemanticKeys:     semanticKeys,
        Tags:             tags,
    }
    
    return nil
}

func (lsm *LayeredStateManager) GetBySemantic(key string) (interface{}, bool) {
    lsm.mutex.RLock()
    defer lsm.mutex.RUnlock()
    
    value, exists := lsm.state.SemanticLayer[key]
    return value, exists
}

func (lsm *LayeredStateManager) GetByType[T any]() ([]T, bool) {
    lsm.mutex.RLock()
    defer lsm.mutex.RUnlock()
    
    targetType := reflect.TypeOf(*new(T))
    values, exists := lsm.state.TypedCollections[targetType]
    if !exists {
        return nil, false
    }
    
    var results []T
    for _, value := range values {
        if typed, ok := value.(T); ok {
            results = append(results, typed)
        }
    }
    
    return results, len(results) > 0
}

func (lsm *LayeredStateManager) GetByTag(tag string) []interface{} {
    lsm.mutex.RLock()
    defer lsm.mutex.RUnlock()
    
    var results []interface{}
    for nodeID, metadata := range lsm.state.Metadata {
        for _, metaTag := range metadata.Tags {
            if metaTag == tag {
                if value, exists := lsm.state.RawData[nodeID]; exists {
                    results = append(results, value)
                }
                break
            }
        }
    }
    
    return results
}
```

**Usage Example**:
```go
// Enhanced post method with semantic registration
func (h *UserInputHandler) Post(ctx NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
    userInput := execResult.(string)
    
    // Register with semantic keys and tags
    if layeredManager, ok := ctx.StateStore.(*LayeredStateManager); ok {
        layeredManager.StoreResult(
            ctx.NodeID, 
            "user_input",
            userInput,
            []string{"user_input", "query", "input_text"}, // semantic keys
            []string{"user_data", "text", "input"},        // tags
        )
    }
    
    return "default", userInput, nil
}

// Enhanced prep with layered access
func (h *IntentHandler) Prep(ctx EnhancedNodeContext) (interface{}, error) {
    if layeredManager, ok := ctx.StateStore.(*LayeredStateManager); ok {
        // Option 1: Semantic access
        if userInput, exists := layeredManager.GetBySemantic("user_input"); exists {
            return userInput, nil
        }
        
        // Option 2: Type-based access
        if strings, found := layeredManager.GetByType[string](); found {
            for _, s := range strings {
                if len(s) > 0 && !strings.Contains(s, "_intent") {
                    return s, nil
                }
            }
        }
        
        // Option 3: Tag-based access
        userDataItems := layeredManager.GetByTag("user_data")
        for _, item := range userDataItems {
            if str, ok := item.(string); ok {
                return str, nil
            }
        }
    }
    
    return nil, fmt.Errorf("user input not found")
}
```

**Pros**: 
- Multiple access patterns available
- Backward compatible with existing code
- Rich metadata for debugging
- Flexible tagging system

**Cons**: 
- Memory overhead from multiple storage layers
- Complex state management
- Potential consistency issues between layers

## Recommendation

**Recommended Approach**: **Layered State Pattern + Query/Accessor Pattern**

This combination provides:

1. **Backward compatibility** with existing nodeID-based storage
2. **Multiple access patterns** for different use cases
3. **Gradual migration** path from current implementation
4. **Rich debugging capabilities** with metadata and tags
5. **Type safety** with generic query methods
6. **Flexibility** for both simple and complex flows

### Implementation Strategy

1. **Phase 1**: Implement LayeredStateManager as wrapper around existing StateStore
2. **Phase 2**: Add Query/Accessor methods for common patterns
3. **Phase 3**: Migrate existing nodes to use semantic keys gradually
4. **Phase 4**: Add optional contract validation for flows that need it

### Migration Path

```go
// Current usage (still works)
for key, value := range ctx.SharedData {
    if userInput, ok := value.(string); ok && !strings.Contains(userInput, "_intent") {
        return userInput, nil
    }
}

// Enhanced usage (new option)
if userInput, exists := ctx.StateQuery.GetBySemantic("user_input"); exists {
    return userInput.(string), nil
}

// Type-safe usage (best option)
userInput, err := ctx.StateQuery.GetTyped[string]("user_input")
if err != nil {
    return nil, err
}
return userInput, nil
```

This approach provides immediate benefits while allowing gradual adoption across the codebase.