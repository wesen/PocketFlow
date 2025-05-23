# Semantic Shared State Implementation Design

## Overview

This document outlines the implementation of the **Layered State Pattern + Query/Accessor Pattern** for PocketFlow Go's shared state system. This design addresses the current issues with opaque nodeID-based keys by providing a clean, modern state management system.

## Design Goals

1. **Semantic Access**: Enable predictable data access via semantic keys
2. **Type Safety**: Provide compile-time type checking for data access
3. **Rich Querying**: Support flexible data discovery patterns
4. **Better Debugging**: Clear visibility into shared state structure
5. **Clean Architecture**: Remove fragile iteration-based access patterns

## Core Architecture

### 1. State Store Interface

```go
// State store interface for semantic data management
type StateStore interface {
    // Semantic data access
    GetBySemantic(key string) (interface{}, bool)
    GetTyped[T any](key string) (T, error)
    SetSemantic(key string, value interface{}) error
    
    // Type-based querying
    FindByType[T any]() ([]T, bool)
    FindLatestByType[T any]() (T, bool)
    
    // Tag-based access
    FindByTag(tag string) []interface{}
    FindByTags(tags []string) []interface{}
    
    // Metadata access
    GetMetadata(key string) (*DataMetadata, bool)
    GetAllMetadata() map[string]DataMetadata
    
    // Update with semantic registration
    UpdateWithSemantic(flowExecutionID, nodeID string, value interface{}, semanticKeys []string, tags []string) error
    
    // Core state operations
    GetSharedData(flowExecutionID string) (map[string]interface{}, error)
    UpdateSharedData(flowExecutionID, key string, value interface{}) error
}
```

### 2. Data Structures

```go
// Metadata for each stored value
type DataMetadata struct {
    ProducerNodeID   string    `json:"producer_node_id"`
    ProducerNodeType string    `json:"producer_node_type"`
    Timestamp        time.Time `json:"timestamp"`
    DataType         string    `json:"data_type"`
    SemanticKeys     []string  `json:"semantic_keys"`
    Tags             []string  `json:"tags"`
    FlowExecutionID  string    `json:"flow_execution_id"`
}

// Layered storage structure
type LayeredSharedState struct {
    // Layer 1: Semantic mappings
    SemanticLayer map[string]interface{} // semantic_key -> value
    
    // Layer 2: Type collections  
    TypedCollections map[reflect.Type][]TypedValue // type -> []values
    
    // Layer 3: Tag-based access
    TaggedData map[string][]string // tag -> []semantic_keys
    
    // Layer 4: Metadata
    Metadata map[string]DataMetadata // semantic_key -> metadata
    
    // Flow organization
    FlowData map[string]*LayeredSharedState // flowExecutionID -> state
    
    // Synchronization
    mutex sync.RWMutex
}

type TypedValue struct {
    Value       interface{}
    SemanticKey string
    Timestamp   time.Time
}
```

### 3. Node Context

```go
// Node context for semantic state access
type NodeContext struct {
    FlowExecutionID string
    NodeID          string
    NodeType        string
    Params          map[string]interface{}
    
    // Semantic state access
    StateQuery   *StateQuery
    SemanticData *SemanticDataAccessor
}

// Query helper for flexible data access
type StateQuery struct {
    store StateStore
}

func (sq *StateQuery) GetBySemantic(key string) (interface{}, bool) {
    return sq.store.GetBySemantic(key)
}

func (sq *StateQuery) GetTyped[T any](key string) (T, error) {
    return sq.store.GetTyped[T](key)
}

func (sq *StateQuery) FindByType[T any]() ([]T, bool) {
    return sq.store.FindByType[T]()
}

func (sq *StateQuery) FindString(filter func(string) bool) (string, bool) {
    strings, found := sq.FindByType[string]()
    if !found {
        return "", false
    }
    
    for _, s := range strings {
        if filter(s) {
            return s, true
        }
    }
    return "", false
}

// Semantic accessor with convenience methods
type SemanticDataAccessor struct {
    store StateStore
}

func (sda *SemanticDataAccessor) UserInput() (string, error) {
    return sda.store.GetTyped[string]("user_input")
}

func (sda *SemanticDataAccessor) Intent() (string, error) {
    return sda.store.GetTyped[string]("intent")
}

func (sda *SemanticDataAccessor) Response() (string, error) {
    return sda.store.GetTyped[string]("response")
}
```

### 4. Node Handler Interface

```go
// Interface for nodes that declare semantic outputs
type SemanticNodeHandler interface {
    DeclareOutputs() []SemanticOutput
}

type SemanticOutput struct {
    Key         string   `json:"key"`
    Description string   `json:"description"`
    Tags        []string `json:"tags"`
}

// Node handler with semantic capabilities
type NodeHandler interface {
    Prep(ctx NodeContext) (interface{}, error)
    Exec(ctx NodeContext, prepResult interface{}) (interface{}, error)
    Post(ctx NodeContext, prepResult, execResult interface{}) (string, interface{}, []SemanticOutput, error)
}
```

## Implementation Strategy

### Phase 1: Core Infrastructure
1. Implement `LayeredStateStore` with semantic-first design
2. Create `StateQuery` and `SemanticDataAccessor` helpers
3. Build `NodeContext` with clean semantic access
4. Add comprehensive tests for all new functionality

### Phase 2: Worker Support
1. Create `NodeWorker` for semantic handlers
2. Add automatic semantic registration in worker execution
3. Create factory methods for semantic contexts and flows

### Phase 3: Migration and Examples
1. Convert branching example to semantic patterns
2. Create migration utilities and documentation
3. Performance testing and optimization

## Code Patterns
```go
// Type-safe semantic access
func (h *WeatherHandler) Prep(ctx NodeContext) (interface{}, error) {
    userInput, err := ctx.StateQuery.GetTyped[string]("user_input")
    if err != nil {
        return nil, err
    }
    return userInput, nil
}

// Convenience methods for common data
func (h *ChatHandler) Prep(ctx NodeContext) (interface{}, error) {
    userInput, err := ctx.SemanticData.UserInput()
    if err != nil {
        return nil, err
    }
    return userInput, nil
}

// Flexible querying for complex scenarios
func (h *AggregatorHandler) Prep(ctx NodeContext) (interface{}, error) {
    // Find all numeric values for aggregation
    numbers := ctx.StateQuery.FindByTags([]string{"numeric", "metric"})
    return numbers, nil
}
```

## File Changes Required

### New Files
- `go/event/core/semantic_state.go` - Semantic interfaces and types
- `go/event/impl/layered_state_store.go` - Semantic-first state implementation
- `go/event/impl/semantic_node_worker.go` - Semantic worker support
- `go/event/impl/state_query.go` - Query helper implementations
- `go/event/examples/branching/semantic_branch_flow.go` - Clean semantic examples

## Migration Examples

### Branching Flow Migration

#### Before (Fragile Iteration Pattern):
```go
func (h *WeatherHandler) Prep(ctx core.NodeContext) (interface{}, error) {
    var userQuery string
    for key, value := range ctx.SharedData {
        if key == "started_at" {
            continue
        }
        if query, ok := value.(string); ok && query != "" {
            if !strings.Contains(query, "_intent") {
                userQuery = query
                break
            }
        }
    }
    if userQuery == "" {
        return nil, fmt.Errorf("user input not found in shared data")
    }
    return userQuery, nil
}
```

#### After (Clean Semantic Pattern):
```go
func (h *WeatherHandler) Prep(ctx NodeContext) (interface{}, error) {
    userInput, err := ctx.SemanticData.UserInput()
    if err != nil {
        return nil, fmt.Errorf("user input not found: %w", err)
    }
    return userInput, nil
}

func (h *WeatherHandler) DeclareOutputs() []SemanticOutput {
    return []SemanticOutput{
        {
            Key:         "weather_response",
            Description: "Formatted weather information",
            Tags:        []string{"response", "weather", "text"},
        },
        {
            Key:         "weather_data",
            Description: "Raw weather data structure",
            Tags:        []string{"data", "weather", "structured"},
        },
    }
}
```

## Benefits Achieved

1. **Eliminated Fragile Code**: No more brittle iteration and string filtering
2. **Type Safety**: Compile-time checking prevents runtime errors
3. **Semantic Clarity**: Clear intent with `ctx.SemanticData.UserInput()`
4. **Better Performance**: O(1) semantic lookups vs O(n) iteration
5. **Rich Metadata**: Self-documenting code with semantic output declarations
6. **Maintainable**: Changes to data structure don't break access patterns
