# Event-Driven PocketFlow Go Implementation Analysis

This document analyzes the Go implementation of PocketFlow, focusing on its event-driven architecture using the Watermill router for pub/sub messaging.

## Overview

The Go implementation of PocketFlow uses an event-driven architecture where:
- Components communicate through events published to specific topics
- The Watermill library handles the pub/sub messaging infrastructure
- Each node in the flow processes its own events asynchronously
- A central state store maintains shared data across the flow execution

## Event Topics & Handlers

Based on the code in `main.go` and `event/flow_orchestrator.go`, the following topics and handlers are configured:

| Topic | Handler/Publisher | Purpose |
|-------|---------|---------|
| `flow.start.requested` | `FlowOrchestrator.HandleFlowStartRequested` | Initiates a new flow execution |
| `flow.completed` | `flow.completed.handler` | Captures successful flow completion |
| `flow.failed` | `flow.failed.handler` / `FlowOrchestrator` | Captures flow execution failures |
| `node.{type}.prep.requested` | `NodeWorker.HandlePrepRequested` | Requests preparation phase for a node |
| `node.transition.requested` | (Implicit) | Signals transition between nodes |
| `node.{type}.exec.requested` | `NodeWorker` | Requests execution phase for a node |
| `node.{type}.post.completed` | `FlowOrchestrator.HandleNodePostCompleted` | Processes completion of a node |

The program defines implicit handlers for node worker topics through `watermillRouter.SetupNodeWorkerHandlersWithWorkers(nodeWorkers)`.

## Worker Orchestration Steps

The orchestration of node workers follows a specific event sequence controlled by the `FlowOrchestrator` in `event/flow_orchestrator.go`:

### 1. Flow Initialization

When a flow starts (`flow.start.requested` event), the `FlowOrchestrator.HandleFlowStartRequested` function:

```go
// event/flow_orchestrator.go
func (o *FlowOrchestrator) HandleFlowStartRequested(event FlowStartRequested) {
    // Store initial shared data
    o.StateStore.StoreSharedData(event.FlowExecutionID, event.InitialSharedData)
    
    // Get flow definition
    flowDef, err := o.FlowRegistry.GetFlowDefinition(event.FlowDefinitionID)
    
    // Store mapping between execution ID and definition ID
    err = o.StateStore.StoreFlowExecution(event.FlowExecutionID, event.FlowDefinitionID)
    
    // Create node execution ID for start node
    nodeExecID := generateUUID()
    
    // Request start node prep
    o.Publisher.Publish("node."+flowDef.StartNodeType+".prep.requested", NodePrepRequested{
        BaseEvent: BaseEvent{
            EventID:         generateUUID(),
            FlowExecutionID: event.FlowExecutionID,
            NodeExecutionID: nodeExecID,
            NodeType:        flowDef.StartNodeType,
            Timestamp:       time.Now(),
            CorrelationID:   event.CorrelationID,
        },
        NodeParams: flowDef.StartNodeParams,
    })
}
```

The function:
1. Stores initial shared data in the state store
2. Retrieves the flow definition from the registry
3. Stores the mapping between execution ID and definition ID
4. Generates a unique node execution ID
5. Publishes a `node.{type}.prep.requested` event to start the first node

### 2. Node Execution Cycle

Each node in the flow goes through three phases, orchestrated through events:

#### Preparation Phase
- Topic: `node.{type}.prep.requested`
- Handler: Node-specific worker's `HandlePrepRequested` method
- Purpose: Read shared data and prepare for execution
- Output: Publishes `node.{type}.prep.completed` when done

#### Execution Phase
- Topic: `node.{type}.exec.requested`
- Handler: Node-specific worker's `HandleExecRequested` method
- Purpose: Execute the node's core logic (e.g., LLM call)
- Output: Publishes `node.{type}.exec.completed` when done

#### Post-Processing Phase
- Topic: `node.{type}.post.requested`
- Handler: Node-specific worker's `HandlePostRequested` method
- Purpose: Update shared data and determine next action
- Output: Publishes `node.{type}.post.completed` with an action string

### 3. Node Transitions

After a node completes its post-processing phase, the `FlowOrchestrator.HandleNodePostCompleted` function is called:

```go
// event/flow_orchestrator.go
func (o *FlowOrchestrator) HandleNodePostCompleted(event NodePostCompleted) {
    // Get flow definition for this execution
    flowDef, err := o.FlowRegistry.GetFlowDefinitionByExecutionID(event.FlowExecutionID)
    
    // Find next node based on current node type and action
    nextNode, exists := flowDef.GetNextNode(event.NodeType, event.Action)
    
    if !exists {
        // Flow is complete, no next node
        o.Publisher.Publish("flow.completed", FlowCompleted{
            BaseEvent: BaseEvent{
                EventID:         generateUUID(),
                FlowExecutionID: event.FlowExecutionID,
                NodeExecutionID: "",
                NodeType:        "",
                Timestamp:       time.Now(),
                CorrelationID:   event.CorrelationID,
            },
            FinalAction:     event.Action,
            ExecutionTimeMs: 0,
        })
        return
    }
    
    // Create new node execution ID
    nextNodeExecID := generateUUID()
    
    // Request transition to next node
    o.Publisher.Publish("node.transition.requested", NodeTransitionRequested{
        BaseEvent: BaseEvent{
            EventID:         generateUUID(),
            FlowExecutionID: event.FlowExecutionID,
            NodeExecutionID: nextNodeExecID,
            NodeType:        nextNode.Type,
            Timestamp:       time.Now(),
            CorrelationID:   event.CorrelationID,
        },
        FromNodeType: event.NodeType,
        Action:       event.Action,
        ToNodeType:   nextNode.Type,
        ToNodeID:     nextNode.ID,
    })
    
    // Request next node prep
    o.Publisher.Publish("node."+nextNode.Type+".prep.requested", NodePrepRequested{
        BaseEvent: BaseEvent{
            EventID:         generateUUID(),
            FlowExecutionID: event.FlowExecutionID,
            NodeExecutionID: nextNodeExecID,
            NodeType:        nextNode.Type,
            Timestamp:       time.Now(),
            CorrelationID:   event.CorrelationID,
        },
        NodeParams: nextNode.Params,
    })
}
```

This function:
1. Retrieves the flow definition for the current execution
2. Determines the next node based on the current node type and action
3. If no next node exists, publishes a `flow.completed` event
4. Otherwise, publishes:
   - A `node.transition.requested` event to signal the transition
   - A `node.{type}.prep.requested` event to start the next node

### 4. Flow Completion

When there are no more nodes to execute, the `FlowOrchestrator` publishes a `flow.completed` event:

```go
// from event/flow_orchestrator.go
o.Publisher.Publish("flow.completed", FlowCompleted{
    BaseEvent: BaseEvent{
        EventID:         generateUUID(),
        FlowExecutionID: event.FlowExecutionID,
        NodeExecutionID: "",
        NodeType:        "",
        Timestamp:       time.Now(),
        CorrelationID:   event.CorrelationID,
    },
    FinalAction:     event.Action,
    ExecutionTimeMs: 0, // Could calculate this if needed
})
```

In `main.go`, a handler for this event is registered:

```go
// main.go
watermillRouter.Router.AddNoPublisherHandler(
    "flow.completed.handler",
    "flow.completed",
    watermillRouter.PubSub,
    func(msg *message.Message) error {
        log.Info().Msg("🎉 Flow completed successfully!")
        
        // Extract the event from the message payload
        var event event.FlowCompleted
        err := json.Unmarshal(msg.Payload, &event)
        
        // Get the final results from shared data
        sharedData, err := stateStore.GetSharedData(event.FlowExecutionID)
        
        log.Info().Msg("📋 Flow results:")
        log.Info().Interface("question", sharedData["question"]).Msg("Question")
        log.Info().Interface("user_answer", sharedData["user_answer"]).Msg("User answer")
        log.Info().Interface("llm_response", sharedData["llm_response"]).Msg("LLM response")
        
        // Signal flow completion
        flowCompletedCh <- struct{}{}
        return nil
    }
)
```

### 5. Error Handling

If an error occurs during orchestration, the `FlowOrchestrator` publishes a `flow.failed` event:

```go
// from event/flow_orchestrator.go
o.Publisher.Publish("flow.failed", FlowFailed{
    BaseEvent: BaseEvent{
        EventID:         generateUUID(),
        FlowExecutionID: event.FlowExecutionID,
        Timestamp:       time.Now(),
        CorrelationID:   event.CorrelationID,
    },
    ErrorMessage:   "Failed to get flow definition",
    ErrorDetails:   err.Error(),
    FailedNodeType: event.NodeType,
})
```

In `main.go`, a handler for this event is registered:

```go
// main.go
watermillRouter.Router.AddNoPublisherHandler(
    "flow.failed.handler",
    "flow.failed",
    watermillRouter.PubSub,
    func(msg *message.Message) error {
        // Extract the event
        var event event.FlowFailed
        err := json.Unmarshal(msg.Payload, &event)
        if err != nil {
            log.Error().Err(err).Msg("Error unmarshaling flow failed event")
        } else {
            log.Error().Str("errorMessage", event.ErrorMessage).Str("errorDetails", event.ErrorDetails).Msg("❌ Flow failed!")
        }
        // Signal flow completion (with error)
        flowCompletedCh <- struct{}{}
        return nil
    }
)
```

## Complete Topic List

Based on the analysis of `event/flow_orchestrator.go` and `main.go`, here is the comprehensive list of topics used in the event-driven PocketFlow architecture:

| Topic | Direction | Source/Handler |
|-------|-----------|----------------|
| `flow.start.requested` | Published | `main.go` publisher |
| `flow.completed` | Published | `FlowOrchestrator.HandleNodePostCompleted` |
| `flow.failed` | Published | Multiple points in `FlowOrchestrator` |
| `node.{type}.prep.requested` | Published | `FlowOrchestrator.HandleFlowStartRequested`, `FlowOrchestrator.HandleNodePostCompleted` |
| `node.{type}.prep.completed` | Subscribed | (Implicit in node worker) |
| `node.{type}.exec.requested` | Published | (Implicit in node worker) |
| `node.{type}.exec.completed` | Subscribed | (Implicit in node worker) |
| `node.{type}.exec.failed` | Published | (Implicit in node worker) |
| `node.{type}.post.requested` | Published | (Implicit in node worker) |
| `node.{type}.post.completed` | Subscribed | `FlowOrchestrator.HandleNodePostCompleted` |
| `node.transition.requested` | Published | `FlowOrchestrator.HandleNodePostCompleted` |

## Example Flow Implementation

The `main.go` file implements a simple question-answer flow with two node types:

1. **QuestionNode**: Defined in `event/question_node.go` (inferred)
   - Presents a question to the user
   - Captures the user's response

2. **AnswerNode**: Defined in `event/answer_node.go` (inferred)
   - Uses a mock LLM client to generate a response based on user input

The transition from `QuestionNode` to `AnswerNode` is defined in the `testFlow` structure:

```go
// main.go
testFlow := &event.FlowDefinition{
    // ...
    Transitions: map[string]map[string]event.TransitionDefinition{
        "question": {
            "default": {
                Action:   "default",
                ToNodeID: "answer-1",
            },
        },
        "answer": {}, // No transitions from answer node (end of flow)
    },
}
```

## Complete Event Flow Sequence

Based on the code in `event/flow_orchestrator.go` and `main.go`, the detailed event sequence for the example question-answer flow is:

1. **Flow Initialization**:
   - `flow.start.requested` → `FlowOrchestrator.HandleFlowStartRequested`
   - Store initial shared data
   - Create node execution ID for question node
   - `node.question.prep.requested` → `QuestionNodeWorker`

2. **Question Node Execution**:
   - `QuestionNodeWorker` processes prep (read shared data)
   - `node.question.prep.completed` → (Internal to worker)
   - `node.question.exec.requested` → `QuestionNodeWorker`
   - `QuestionNodeWorker` executes (ask user for input)
   - `node.question.exec.completed` → (Internal to worker)
   - `node.question.post.requested` → `QuestionNodeWorker`
   - `QuestionNodeWorker` updates shared data with question and answer
   - `node.question.post.completed` → `FlowOrchestrator.HandleNodePostCompleted`

3. **Transition to Answer Node**:
   - `FlowOrchestrator` determines next node (answer-1)
   - `node.transition.requested` → (Notification event)
   - `node.answer.prep.requested` → `AnswerNodeWorker`

4. **Answer Node Execution**:
   - `AnswerNodeWorker` processes prep (read user's answer)
   - `node.answer.prep.completed` → (Internal to worker)
   - `node.answer.exec.requested` → `AnswerNodeWorker`
   - `AnswerNodeWorker` executes (call mock LLM)
   - `node.answer.exec.completed` → (Internal to worker)
   - `node.answer.post.requested` → `AnswerNodeWorker`
   - `AnswerNodeWorker` updates shared data with LLM response
   - `node.answer.post.completed` → `FlowOrchestrator.HandleNodePostCompleted`

5. **Flow Completion**:
   - `FlowOrchestrator` finds no next node for answer + "default"
   - `flow.completed` → `flow.completed.handler`
   - Handler retrieves and logs final results

## Conclusion

The Go implementation of PocketFlow demonstrates a sophisticated event-driven architecture where:

1. The `FlowOrchestrator` (`event/flow_orchestrator.go`) manages the overall flow by:
   - Handling flow start requests
   - Processing node completions
   - Managing transitions between nodes
   - Publishing events to coordinate the workflow

2. Node workers handle their specific tasks through:
   - Preparation phase (reading from shared state)
   - Execution phase (performing the core logic)
   - Post-processing phase (updating shared state and determining next action)

3. The Watermill router (`main.go`) connects all components through:
   - Topic-based messaging
   - Event handlers that process each message
   - No direct coupling between components

This architecture provides clear separation of concerns, making it suitable for complex, extensible LLM workflows with high scalability potential. 