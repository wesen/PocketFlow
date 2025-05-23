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
    
    // Metadata access
    GetMetadata(key string) (*DataMetadata, bool)
    GetAllMetadata() map[string]DataMetadata
    
    // Update with semantic registration
    UpdateWithSemantic(flowExecutionID, nodeID string, value interface{}, semanticKeys []string) error
    
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
    SemanticData *SemanticDataAccessor
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
2. Create `SemanticDataAccessor` helpers
3. Build `NodeContext` with clean semantic access
4. Add comprehensive tests for all new functionality

### Phase 2: Worker Support
1. Update `NodeWorker` 
2. Add automatic semantic registration in worker execution
3. Create factory methods for semantic contexts and flows

### Phase 3: Migration and Examples
1. Convert branching example to semantic patterns

## Code Patterns
```go
// Type-safe semantic access
func (h *WeatherHandler) Prep(ctx NodeContext) (interface{}, error) {
    userInput, err := ctx.SemanticData.UserInput()
    if err != nil {
        return nil, err
    }
    return userInput, nil
}
```

## File Changes Required

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
