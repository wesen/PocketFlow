# CQRS Flow Orchestration with Conditional Events

This document explains how event conditions and flow orchestration work in the proposed CQRS-based architecture for PocketFlow, with specific focus on implementing a question-answer flow similar to the declarative DSL approach.

## Understanding Event Conditions

In the CQRS-based architecture, the `EventCondition` function allows for conditional branching based on event content. This is similar to the action-based transitions in the declarative DSL (`On("action").Then(nextNode)`), but operates at the event level.

### How EventCondition Works

The `EventCondition` is a function that takes an event and returns a boolean:

```go
type Step struct {
    EventType      string
    NextCommand    interface{}
    EventCondition func(event interface{}) bool
}
```

When an event of the specified `EventType` is received, the `EventCondition` function evaluates whether the next command should be executed. If the condition returns true, the `NextCommand` is sent to the command bus.

For example, a condition might check if a question was answered with "yes" or "no":

```go
EventCondition: func(event interface{}) bool {
    questionEvent := event.(*QuestionAnsweredEvent)
    return strings.ToLower(questionEvent.Answer) == "yes"
}
```

## Flow Orchestration Implementation

The flow orchestration in CQRS combines a flow definition with event handlers that manage the flow execution. Here's how it works:

### 1. Flow Definition

```go
type Flow struct {
    ID            string
    StartCommand  interface{}
    Steps         []Step
}

type Step struct {
    EventType      string             // Type of event to listen for
    NextCommand    interface{}        // Command to send when event is received
    EventCondition func(event interface{}) bool  // Optional condition to evaluate
}
```

### 2. Flow Orchestrator

The flow orchestrator ties everything together:

```go
type FlowOrchestrator struct {
    commandBus *cqrs.CommandBus
    eventBus   *cqrs.EventBus
    flows      map[string]*Flow
    activeFlows map[string]string  // Maps flowExecutionID to flowID
}

func NewFlowOrchestrator(commandBus *cqrs.CommandBus, eventBus *cqrs.EventBus) *FlowOrchestrator {
    return &FlowOrchestrator{
        commandBus: commandBus,
        eventBus:   eventBus,
        flows:      make(map[string]*Flow),
        activeFlows: make(map[string]string),
    }
}

func (o *FlowOrchestrator) RegisterFlow(flow *Flow) {
    o.flows[flow.ID] = flow
}

func (o *FlowOrchestrator) StartFlow(ctx context.Context, flowID string, flowExecutionID string) error {
    flow, exists := o.flows[flowID]
    if !exists {
        return fmt.Errorf("flow not found: %s", flowID)
    }
    
    o.activeFlows[flowExecutionID] = flowID
    
    // Start the flow by sending the initial command
    return o.commandBus.Send(ctx, flow.StartCommand)
}
```

### 3. Event Handler for Flow Progression

```go
func (o *FlowOrchestrator) HandleEvent(ctx context.Context, event interface{}, eventType string) error {
    // Get flow execution ID from event metadata
    meta := cqrs.GetEventMetadata(event)
    flowExecID := meta["flow_execution_id"]
    
    // Check if this event is part of an active flow
    flowID, exists := o.activeFlows[flowExecID]
    if !exists {
        return nil  // Not part of a flow, ignore
    }
    
    flow := o.flows[flowID]
    
    // Find matching steps for this event type
    for _, step := range flow.Steps {
        if step.EventType == eventType {
            // Check condition if provided
            if step.EventCondition != nil {
                if !step.EventCondition(event) {
                    continue  // Condition not met, try next step
                }
            }
            
            // Condition met or no condition, send next command
            if step.NextCommand != nil {
                return o.commandBus.Send(ctx, step.NextCommand)
            } else {
                // No next command, flow is complete for this branch
                delete(o.activeFlows, flowExecID)
                return nil
            }
        }
    }
    
    return nil  // No matching steps found
}
```

## Question-Answer Flow Example

Let's implement a question-answer flow similar to the one in the declarative DSL guide:

### 1. Define Commands and Events

```go
// Commands
type AskQuestionCommand struct {
    FlowExecutionID string `json:"-"`  // Metadata, not serialized
    Question        string `json:"question"`
}

type AnalyzeAnswerCommand struct {
    FlowExecutionID string `json:"-"`
    Question        string `json:"question"`
    Answer          string `json:"answer"`
}

// Events
type QuestionAskedEvent struct {
    FlowExecutionID string `json:"-"`
    Question        string `json:"question"`
}

type QuestionAnsweredEvent struct {
    FlowExecutionID string `json:"-"`
    Question        string `json:"question"`
    Answer          string `json:"answer"`
}

type AnswerAnalyzedEvent struct {
    FlowExecutionID string `json:"-"`
    Question        string `json:"question"`
    Answer          string `json:"answer"`
    Analysis        string `json:"analysis"`
    RequiresFollowUp bool   `json:"requires_follow_up"`
}
```

### 2. Implement Command Handlers

```go
// Question node handler
func questionHandler(eventBus *cqrs.EventBus) cqrs.CommandHandler {
    return cqrs.NewCommandHandler(
        "question_handler",
        func(ctx context.Context, cmd *AskQuestionCommand) error {
            // Display question to user
            fmt.Println(cmd.Question)
            
            // Emit question asked event
            err := eventBus.Publish(ctx, &QuestionAskedEvent{
                FlowExecutionID: cmd.FlowExecutionID,
                Question: cmd.Question,
            })
            if err != nil {
                return err
            }
            
            // Get user answer
            var answer string
            fmt.Print("> ")
            fmt.Scanln(&answer)
            
            // Emit question answered event
            return eventBus.Publish(ctx, &QuestionAnsweredEvent{
                FlowExecutionID: cmd.FlowExecutionID,
                Question: cmd.Question,
                Answer: answer,
            })
        },
    )
}

// Answer node handler
func answerAnalysisHandler(eventBus *cqrs.EventBus) cqrs.CommandHandler {
    return cqrs.NewCommandHandler(
        "answer_analysis_handler",
        func(ctx context.Context, cmd *AnalyzeAnswerCommand) error {
            // In a real app, this would call an LLM or other logic
            analysis := fmt.Sprintf("Analysis of '%s' to question '%s'", 
                cmd.Answer, cmd.Question)
            
            // Determine if follow-up is needed
            requiresFollowUp := len(cmd.Answer) < 5 // Just an example condition
            
            // Emit answer analyzed event
            return eventBus.Publish(ctx, &AnswerAnalyzedEvent{
                FlowExecutionID: cmd.FlowExecutionID,
                Question: cmd.Question,
                Answer: cmd.Answer,
                Analysis: analysis,
                RequiresFollowUp: requiresFollowUp,
            })
        },
    )
}
```

### 3. Define the Flow with Conditional Paths

```go
// Define the flow with branching based on whether follow-up is needed
questionAnswerFlow := &Flow{
    ID: "question_answer_flow",
    StartCommand: &AskQuestionCommand{
        Question: "What would you like to know about PocketFlow?",
    },
    Steps: []Step{
        {
            // When question is answered, analyze the answer
            EventType: "QuestionAnsweredEvent",
            NextCommand: &AnalyzeAnswerCommand{},
            // No condition - always proceed
        },
        {
            // When answer is analyzed and follow-up is needed
            EventType: "AnswerAnalyzedEvent",
            NextCommand: &AskQuestionCommand{
                Question: "Could you please elaborate more on your answer?",
            },
            // Only proceed if follow-up is required
            EventCondition: func(event interface{}) bool {
                return event.(*AnswerAnalyzedEvent).RequiresFollowUp
            },
        },
        {
            // When answer is analyzed and no follow-up is needed
            EventType: "AnswerAnalyzedEvent",
            NextCommand: nil, // End of flow
            // Only proceed if no follow-up is required
            EventCondition: func(event interface{}) bool {
                return !event.(*AnswerAnalyzedEvent).RequiresFollowUp
            },
        },
    },
}
```

### 4. Command/Event Propagation

To propagate the flow execution ID and other metadata between commands and events:

```go
// Add a command processor middleware to handle metadata
func flowMetadataMiddleware(eventBus *cqrs.EventBus) cqrs.CommandProcessor {
    return func(next cqrs.CommandHandler) cqrs.CommandHandler {
        return func(ctx context.Context, cmd interface{}) error {
            // Extract metadata if available
            if cmd, ok := cmd.(interface{ GetFlowExecutionID() string }); ok {
                flowExecID := cmd.GetFlowExecutionID()
                
                // Create context with flow execution ID
                ctx = context.WithValue(ctx, "flow_execution_id", flowExecID)
            }
            
            return next(ctx, cmd)
        }
    }
}

// Add an event processor middleware
func flowEventMetadataMiddleware() cqrs.EventProcessor {
    return func(next cqrs.EventHandler) cqrs.EventHandler {
        return func(ctx context.Context, event interface{}) error {
            // Get flow execution ID from context
            if flowExecID, ok := ctx.Value("flow_execution_id").(string); ok {
                // Add to event metadata if the event has the right method
                if event, ok := event.(interface{ SetFlowExecutionID(string) }); ok {
                    event.SetFlowExecutionID(flowExecID)
                }
            }
            
            return next(ctx, event)
        }
    }
}
```

## Complete Flow Orchestration Example

Putting it all together with the CQRS facade:

```go
func main() {
    // Initialize Watermill components
    logger := watermill.NewStdLogger(false, false)
    pubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)
    router, _ := message.NewRouter(message.RouterConfig{}, logger)
    
    // Set up CQRS facade
    cqrsFacade, err := cqrs.NewFacade(cqrs.FacadeConfig{
        GenerateCommandsTopic: func(commandName string) string {
            return "command." + commandName
        },
        GenerateEventsTopic: func(eventName string) string {
            return "event." + eventName
        },
        CommandHandlers: func(cb *cqrs.CommandBus, eb *cqrs.EventBus) []cqrs.CommandHandler {
            return []cqrs.CommandHandler{
                questionHandler(eb),
                answerAnalysisHandler(eb),
            }
        },
        EventHandlers: func(cb *cqrs.CommandBus, eb *cqrs.EventBus) []cqrs.EventHandler {
            return []cqrs.EventHandler{
                // Register event handler for flow orchestration
                cqrs.NewEventHandler(
                    "flow_orchestrator_handler",
                    func(ctx context.Context, event interface{}) error {
                        // Get event type from metadata
                        eventType := cqrs.GetEventType(event)
                        return flowOrchestrator.HandleEvent(ctx, event, eventType)
                    },
                ),
            }
        },
        Router:             router,
        CommandsPublisher:  pubSub,
        EventsPublisher:    pubSub,
        CommandsSubscriberConstructor: func(handlerName string) (message.Subscriber, error) {
            return pubSub, nil
        },
        EventsSubscriberConstructor: func(handlerName string) (message.Subscriber, error) {
            return pubSub, nil
        },
        Logger:    logger,
        Marshaler: cqrs.JSONMarshaler{},
    })
    if err != nil {
        panic(err)
    }
    
    // Create flow orchestrator
    flowOrchestrator := NewFlowOrchestrator(cqrsFacade.CommandBus(), cqrsFacade.EventBus())
    
    // Register flow
    flowOrchestrator.RegisterFlow(questionAnswerFlow)
    
    // Start the router
    go func() {
        if err := router.Run(context.Background()); err != nil {
            panic(err)
        }
    }()
    
    // Generate a flow execution ID
    flowExecutionID := uuid.New().String()
    
    // Start the flow
    err = flowOrchestrator.StartFlow(context.Background(), "question_answer_flow", flowExecutionID)
    if err != nil {
        panic(err)
    }
    
    // Wait for completion
    select {}
}
```

## Comparison with Declarative DSL

This CQRS approach achieves the same goals as the declarative DSL example but with different patterns:

| Declarative DSL Feature | CQRS Implementation |
|------------------------|---------------------|
| `Begin(nodeA)` | `StartCommand: &AskQuestionCommand{}` |
| `On("action").Then(nodeB)` | `EventType: "EventName", EventCondition: func(event) bool {}` |
| Node transition logic | EventCondition functions |
| Flow branching | Multiple Steps with the same EventType but different conditions |
| Shared data | Event payloads and command parameters |

## Benefits of CQRS for Flow Orchestration

1. **Type Safety**: Commands and events are strongly typed
2. **Decoupling**: Commands and events have clear boundaries
3. **Testability**: Each handler can be tested in isolation
4. **Observability**: Every step in the flow generates events that can be monitored and logged
5. **Scalability**: CQRS pattern can scale horizontally as the system grows

## Conclusion

The CQRS-based approach with event conditions provides a powerful framework for building complex flows with conditional logic. While it may require more initial setup compared to the declarative DSL, it offers greater flexibility, type safety, and decoupling which are valuable in larger applications. 