# Alternative PocketFlow Architecture Using Watermill's Router and CQRS

This document proposes an alternative architecture for the Go implementation of PocketFlow, leveraging more of Watermill's built-in features, particularly the Router's `AddHandler` functionality and the CQRS component.

## Current vs. Proposed Architecture

The current implementation uses Watermill as a messaging backbone but implements much of the orchestration logic manually. This proposal suggests using more of Watermill's built-in features to simplify the code and leverage the library's full capabilities.

### Advantages of Proposed Architecture

1. **Reduced Custom Code**: Utilize Watermill's Router and CQRS patterns instead of custom orchestration
2. **Middleware Support**: Easy integration of logging, retries, and metrics
3. **Simplified Event Handling**: Use the `AddHandler` approach for clearer handler management
4. **Better Message Transformation**: Use handler functions that transform input messages to output messages
5. **Type Safety**: Use CQRS for type-safe command and event handling

## Router-Based Architecture

Instead of implementing a custom orchestrator, we can use Watermill's Router directly:

```go
// Initialize the router
router, err := message.NewRouter(message.RouterConfig{}, logger)
if err != nil {
    panic(err)
}

// Add middleware for all handlers
router.AddMiddleware(
    middleware.CorrelationID,
    middleware.Retry{
        MaxRetries:      3,
        InitialInterval: time.Millisecond * 100,
        Logger:          logger,
    }.Middleware,
    middleware.Recoverer,
)

// Add handlers for node execution
router.AddHandler(
    "question_node_handler",
    "node.question.exec.requested",
    pubSub,
    "node.question.exec.completed",
    pubSub,
    questionNodeHandler,
)

router.AddHandler(
    "answer_node_handler",
    "node.answer.exec.requested",
    pubSub,
    "node.answer.exec.completed",
    pubSub,
    answerNodeHandler,
)
```

### Handler Functions

With this approach, handler functions would directly transform input messages to output messages:

```go
func questionNodeHandler(msg *message.Message) ([]*message.Message, error) {
    // Unmarshal input event
    var execRequested NodeExecRequested
    if err := json.Unmarshal(msg.Payload, &execRequested); err != nil {
        return nil, err
    }
    
    // Execute question logic
    question := "What would you like to know about?"
    fmt.Println(question)
    
    // Read user input
    var userInput string
    fmt.Scanln(&userInput)
    
    // Create output event
    completed := NodeExecCompleted{
        BaseEvent: BaseEvent{
            EventID:         uuid.New().String(),
            FlowExecutionID: execRequested.FlowExecutionID,
            NodeExecutionID: execRequested.NodeExecutionID,
            NodeType:        execRequested.NodeType,
            Timestamp:       time.Now(),
            CorrelationID:   execRequested.CorrelationID,
        },
        Result: userInput,
    }
    
    // Marshal and create output message
    payload, _ := json.Marshal(completed)
    outputMsg := message.NewMessage(uuid.New().String(), payload)
    
    // Copy all metadata from input message
    for k, v := range msg.Metadata {
        outputMsg.Metadata.Set(k, v)
    }
    
    return []*message.Message{outputMsg}, nil
}
```

### State Management

For state management, we can use Watermill's message metadata or a separate state store:

```go
// Using message metadata for simple state
func addStateToMessage(msg *message.Message, key string, value interface{}) {
    jsonValue, _ := json.Marshal(value)
    msg.Metadata.Set(key, string(jsonValue))
}

func getStateFromMessage(msg *message.Message, key string, target interface{}) error {
    jsonValue, exists := msg.Metadata.Get(key)
    if !exists {
        return errors.New("state not found")
    }
    return json.Unmarshal([]byte(jsonValue), target)
}
```

## CQRS-Based Architecture

For more complex flows, we can leverage Watermill's CQRS component for type-safe command and event handling:

```go
// Define commands and events
type AskQuestionCommand struct {
    Question string `json:"question"`
}

type QuestionAnsweredEvent struct {
    Question string `json:"question"`
    Answer   string `json:"answer"`
}

// Initialize the CQRS facade
cqrsFacade, err := cqrs.NewFacade(cqrs.FacadeConfig{
    GenerateCommandsTopic: func(commandName string) string {
        return "command." + commandName
    },
    GenerateEventsTopic: func(eventName string) string {
        return "event." + eventName
    },
    CommandHandlers: func(cb *cqrs.CommandBus, eb *cqrs.EventBus) []cqrs.CommandHandler {
        return []cqrs.CommandHandler{
            askQuestionHandler(eb),
        }
    },
    EventHandlers: func(cb *cqrs.CommandBus, eb *cqrs.EventBus) []cqrs.EventHandler {
        return []cqrs.EventHandler{
            processAnswerHandler(cb),
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
    Logger: logger,
    Marshaler: cqrs.JSONMarshaler{},
})
```

### Command Handlers

Command handlers would implement the business logic for each node:

```go
func askQuestionHandler(eventBus *cqrs.EventBus) cqrs.CommandHandler {
    return cqrs.NewCommandHandler(
        "ask_question_handler",
        func(ctx context.Context, cmd *AskQuestionCommand) error {
            fmt.Println(cmd.Question)
            
            var answer string
            fmt.Scanln(&answer)
            
            // Publish an event with the answer
            return eventBus.Publish(ctx, &QuestionAnsweredEvent{
                Question: cmd.Question,
                Answer:   answer,
            })
        },
    )
}
```

### Event Handlers

Event handlers would respond to events generated by command handlers:

```go
func processAnswerHandler(commandBus *cqrs.CommandBus) cqrs.EventHandler {
    return cqrs.NewEventHandler(
        "process_answer_handler",
        func(ctx context.Context, event *QuestionAnsweredEvent) error {
            // Process the answer through an LLM
            llmResponse := fmt.Sprintf("This is a response to your answer: %s", event.Answer)
            
            // Store the results
            storeResults(event.Question, event.Answer, llmResponse)
            
            // No follow-up commands in this example
            return nil
        },
    )
}
```

## Flow Definition and Execution

With this architecture, flow definitions would focus on the sequence of commands and events:

```go
// Define a flow
type Flow struct {
    ID          string
    StartCommand interface{}
    Steps       []Step
}

type Step struct {
    EventType      string
    NextCommand    interface{}
    EventCondition func(event interface{}) bool
}

// Execute a flow
func ExecuteFlow(ctx context.Context, flow Flow, commandBus *cqrs.CommandBus) error {
    // Start the flow with the initial command
    return commandBus.Send(ctx, flow.StartCommand)
    // Event handlers will take care of subsequent steps
}
```

## Example Question-Answer Flow Implementation

Here's how the complete question-answer flow would be implemented:

```go
func main() {
    // Initialize Watermill components
    logger := watermill.NewStdLogger(false, false)
    pubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)
    router, _ := message.NewRouter(message.RouterConfig{}, logger)
    
    // Set up CQRS
    cqrsFacade, _ := cqrs.NewFacade(cqrs.FacadeConfig{
        // Configuration as shown above
    })
    
    // Define the flow
    questionAnswerFlow := Flow{
        ID: "question_answer_flow",
        StartCommand: &AskQuestionCommand{
            Question: "What would you like to know about?",
        },
        Steps: []Step{
            {
                EventType:   "QuestionAnsweredEvent",
                NextCommand: nil, // End of flow
            },
        },
    }
    
    // Start the router
    go func() {
        if err := router.Run(context.Background()); err != nil {
            panic(err)
        }
    }()
    
    // Execute the flow
    err := ExecuteFlow(context.Background(), questionAnswerFlow, cqrsFacade.CommandBus())
    if err != nil {
        panic(err)
    }
    
    // Wait for completion
    // In a real application, you'd use proper signaling for this
    time.Sleep(5 * time.Second)
}
```

## Benefits Over Current Implementation

1. **Cleaner Message Handling**: The `AddHandler` pattern provides a cleaner way to transform messages
2. **Type Safety**: The CQRS approach ensures commands and events are properly typed
3. **Middleware Support**: Easily add retry, correlation ID, and other middleware
4. **Simpler Flow Definition**: Flows are defined in terms of commands and events, which maps well to the domain
5. **Built-in Marshaling**: CQRS handles marshaling and unmarshaling of commands and events
6. **Reduced Boilerplate**: Less custom code for event routing and handling

## Integration with Existing Code

To integrate with the existing code base:

1. **Gradual Migration**: Start by replacing the custom orchestrator with Watermill's Router
2. **Keep State Store**: Maintain the existing SQLite state store for persistence
3. **Adapt Event Formats**: Ensure existing event formats work with the new architecture
4. **Change Node Workers**: Convert node workers to handler functions or CQRS command/event handlers

## Conclusion

Adopting Watermill's Router with `AddHandler` and potentially the CQRS component would simplify the PocketFlow Go implementation while providing more built-in features. This approach aligns with established patterns for event-driven systems and takes full advantage of Watermill's capabilities. 