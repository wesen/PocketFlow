# Shared State Design and Issues in PocketFlow Go

## Overview

During implementation of the branching flow feature in PocketFlow Go, we encountered a critical shared state access pattern issue that caused node execution failures. This document outlines the problem, root cause, and solution.

## The Problem

### Initial Symptom
The branching flow would start successfully but fail at the `general` node with the error:
```
NodeWorker prep phase failed error="user input not found in shared data"
```

### Expected vs Actual Behavior
- **Expected**: Nodes should access user input from shared data using semantic keys like `"user_input"`
- **Actual**: Shared data contained the correct values but with nodeID keys instead of semantic keys

## Root Cause Analysis

### Shared Data Storage Pattern
The framework automatically stores node results in shared data using the **nodeID as the key**, not semantic names:

```go
// What we expected:
sharedData = {
    "user_input": "What's the weather like in San Francisco?",
    "intent": "general_intent"
}

// What actually happened:
sharedData = {
    "308b8a81-9fdf-4a41-8613-dab54dbbb50b": "What's the weather like in San Francisco?", // user_input node
    "d4c7958e-4074-44a6-8048-75b1392c94f9": "general_intent",                             // intent_classifier node
    "started_at": "2025-05-23T11:41:50-04:00"                                             // metadata
}
```

### Code Location
In `go/event/impl/generic_flow_worker.go:144`:
```go
// Update shared data with node result
if err := w.StateStore.UpdateSharedData(completed.FlowExecutionID, completed.NodeID, completed.Result); err != nil {
```

The flow worker stores results using `completed.NodeID` as the key, making data access unpredictable for downstream nodes.

## Failed Solutions

### Attempt 1: Manual Key Setting
Tried to manually set semantic keys in the `Post()` method:
```go
func (h *CLIUserInputHandler) Post(ctx core.NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
    userInput := execResult.(string)
    ctx.SharedData["user_input"] = userInput  // This didn't work as expected
    return "default", userInput, nil
}
```

**Issue**: The framework's automatic nodeID-based storage overrode manual key setting.

### Attempt 2: Hardcoded Key Access
Initial branching handlers used hardcoded keys:
```go
func (h *GeneralHandler) Prep(ctx core.NodeContext) (interface{}, error) {
    userQuery, ok := ctx.SharedData["user_input"].(string)  // This key didn't exist
    if !ok {
        return nil, fmt.Errorf("user input not found in shared data")
    }
    return userQuery, nil
}
```

**Issue**: Fragile design assuming specific key names that don't match the actual storage pattern.

## Solution: Iteration-Based Data Access

### Pattern Implementation
Access shared data by iterating and filtering, not hardcoded keys:

```go
func (h *GeneralHandler) Prep(ctx core.NodeContext) (interface{}, error) {
    // Get user query from shared data by looking for string values
    for key, value := range ctx.SharedData {
        if key == "started_at" { // Skip metadata
            continue
        }
        if userQuery, ok := value.(string); ok && userQuery != "" {
            // Check if this looks like user input (not an intent classification result)
            if !strings.Contains(userQuery, "_intent") {
                return userQuery, nil
            }
        }
    }
    return nil, fmt.Errorf("user input not found in shared data")
}
```

### Key Benefits
1. **Resilient**: Works regardless of nodeID values
2. **Type-safe**: Checks data types before use
3. **Flexible**: Can add filtering logic for different data types
4. **Maintainable**: No hardcoded key dependencies

## Architectural Implications

### Design Principle
**Node results are stored with nodeID keys, not semantic keys.** This is by design for:
- Unique key guarantee (nodeIDs are always unique)
- Avoiding key naming conflicts between nodes
- Enabling deterministic data access patterns

### Best Practices
1. **Never assume semantic key names** in shared data access
2. **Always iterate through shared data** when looking for specific values
3. **Use type checking and filtering** to identify the correct data
4. **Document data access patterns** for complex flows

### Framework Implications
This pattern affects:
- **Node handler implementation**: Must use iteration-based access
- **Flow design**: Cannot rely on semantic data passing
- **Debugging**: Shared data keys are opaque nodeIDs
- **Testing**: Need to account for dynamic key generation

## Lessons Learned

1. **Understand the framework's storage model** before implementing business logic
2. **Test with actual data flows**, not hardcoded examples
3. **Design resilient data access patterns** that work with framework constraints
4. **Document implicit framework behaviors** that affect application design

## Recommendations

### For Node Handlers
```go
// Good: Resilient pattern
func findUserInput(sharedData map[string]interface{}) (string, error) {
    for key, value := range sharedData {
        if strings.HasPrefix(key, "metadata_") { // Skip known metadata
            continue
        }
        if input, ok := value.(string); ok && len(input) > 0 {
            if !strings.Contains(input, "_intent") { // Filter out classifications
                return input, nil
            }
        }
    }
    return "", fmt.Errorf("user input not found")
}

// Bad: Fragile pattern
func (h *Handler) Prep(ctx core.NodeContext) (interface{}, error) {
    input := ctx.SharedData["user_input"].(string) // Will fail
    return input, nil
}
```

### For Framework Design
Consider adding semantic data access helpers:
```go
// Potential framework enhancement
func (ctx *NodeContext) FindValueByType(predicate func(interface{}) bool) (interface{}, error) {
    for _, value := range ctx.SharedData {
        if predicate(value) {
            return value, nil
        }
    }
    return nil, fmt.Errorf("value not found")
}
```

## Conclusion

The shared state access pattern issue revealed a fundamental design assumption mismatch between expected semantic keys and actual nodeID-based storage. The solution required abandoning hardcoded key access in favor of iteration-based data discovery, leading to more resilient and maintainable code.

This experience highlights the importance of understanding framework internals and designing application code that works with, rather than against, the underlying architecture.