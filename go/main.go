package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/The-Pocket/PocketFlow/go/event"
	"github.com/The-Pocket/PocketFlow/go/logger"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
)

func main() {
	// Initialize the logger
	log := logger.Get()
	log.Info().Msg("Initializing PocketFlow event-driven system")
	
	// Create a unique flow execution ID
	flowExecutionID := uuid.New().String()
	log.Info().Str("flowExecutionID", flowExecutionID).Msg("Generated flow execution ID")
	
	// Initialize the state store
	log.Debug().Msg("Creating SQLite state store in memory")
	stateStore, err := event.NewSQLiteStateStore(":memory:")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create state store")
		os.Exit(1)
	}
	log.Info().Msg("SQLite state store initialized successfully")
	
	// Create a flow registry
	log.Debug().Msg("Creating in-memory flow registry")
	flowRegistry := event.NewInMemoryFlowRegistry()
	log.Info().Msg("Flow registry created")
	
	// Set up the router
	log.Debug().Msg("Setting up event router")
	watermillRouter := event.NewWatermillEventRouter(nil, nil)
	publisher := event.NewWatermillPublisher(watermillRouter.PubSub)
	log.Info().Msg("Event router and publisher initialized")
	
	// Create the flow orchestrator
	log.Debug().Msg("Creating flow orchestrator")
	orchestrator := event.NewFlowOrchestrator(publisher, stateStore, flowRegistry)
	log.Info().Msg("Flow orchestrator created")
	
	// Set up mock LLM client
	log.Debug().Msg("Creating mock LLM client")
	mockLLM := event.NewMockLLMClient()
	mockLLM.AddResponse("Given the user's response", "This is a detailed explanation from the LLM based on your input.")
	log.Info().Msg("Mock LLM client initialized with predefined responses")
	
	// Create the node workers
	log.Debug().Msg("Creating node workers")
	questionNode := event.NewQuestionNodeWorker(publisher, stateStore, "What is your question?")
	answerNode := event.NewAnswerNodeWorker(publisher, stateStore, mockLLM)
	log.Info().Msg("Node workers created")
	
	// Register the nodes with the router
	log.Debug().Msg("Registering node workers with router")
	nodeWorkers := map[string]event.NodeWorker{
		"question": questionNode,
		"answer":   answerNode,
	}
	log.Info().Str("nodes", "question,answer").Msg("Node workers registered")
	
	// Update the router with orchestrator and nodes
	log.Debug().Msg("Updating router with orchestrator and node workers")
	watermillRouter.FlowOrchestrator = orchestrator
	watermillRouter.NodeWorkers = nodeWorkers
	watermillRouter.SetupNodeWorkerHandlers()
	log.Info().Msg("Node worker handlers registered with router")
	
	// Define our test flow
	log.Debug().Msg("Defining test flow")
	testFlow := &event.FlowDefinition{
		ID:             "test-flow",
		Name:           "Test Question-Answer Flow",
		StartNodeType:  "question",
		StartNodeID:    "question-1",
		StartNodeParams: map[string]interface{}{
			"question": "What would you like to know about?",
		},
		Nodes: map[string]event.NodeDefinition{
			"question-1": {
				ID:     "question-1",
				Type:   "question",
				Params: map[string]interface{}{},
			},
			"answer-1": {
				ID:     "answer-1",
				Type:   "answer",
				Params: map[string]interface{}{},
			},
		},
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
	
	// Register the flow with the registry
	log.Debug().Str("flowID", testFlow.ID).Msg("Registering flow with registry")
	flowRegistry.RegisterFlow(testFlow.ID, testFlow)
	log.Info().Str("flowID", testFlow.ID).Msg("Flow registered successfully")
	
	// Set up a channel to capture flow completed events
	log.Debug().Msg("Setting up flow completion channel")
	flowCompletedCh := make(chan struct{})
	
	// Add a special handler for flow.completed events
	log.Debug().Msg("Setting up flow.completed event handler")
	watermillRouter.Router.AddNoPublisherHandler(
		"flow.completed.handler",
		"flow.completed",
		watermillRouter.PubSub,
		func(msg *message.Message) error {
			log.Info().Msg("🎉 Flow completed successfully!")
			
			// Extract the event from the message payload
			var event event.FlowCompleted
			err := json.Unmarshal(msg.Payload, &event)
			if err != nil {
				log.Error().Err(err).Msg("Error unmarshaling event")
				return nil
			}
			
			log.Debug().Str("flowExecutionID", event.FlowExecutionID).Msg("Getting final results from shared data")
			// Get the final results from shared data
			sharedData, err := stateStore.GetSharedData(event.FlowExecutionID)
			if err != nil {
				log.Error().Err(err).Msg("Error getting final results")
				return nil
			}
			
			log.Info().Msg("📋 Flow results:")
			log.Info().Interface("question", sharedData["question"]).Msg("Question")
			log.Info().Interface("user_answer", sharedData["user_answer"]).Msg("User answer")
			log.Info().Interface("llm_response", sharedData["llm_response"]).Msg("LLM response")
			
			// Signal flow completion
			log.Debug().Msg("Signaling flow completion")
			flowCompletedCh <- struct{}{}
			return nil
		},
	)
	
	// Add handler for flow.failed events
	log.Debug().Msg("Setting up flow.failed event handler")
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
			log.Debug().Msg("Signaling flow completion (with error)")
			flowCompletedCh <- struct{}{}
			return nil
		},
	)
	
	// Start the router in a goroutine
	log.Debug().Msg("Creating context for router")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	log.Info().Msg("Starting router in background goroutine")
	go func() {
		if err := watermillRouter.Router.Run(ctx); err != nil {
			log.Fatal().Err(err).Msg("Router error")
		}
	}()
	
	// Handle OS signals for graceful shutdown
	log.Debug().Msg("Setting up signal handlers for graceful shutdown")
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	log.Info().Msg("Signal handlers registered")
	
	log.Info().Msg("🚀 Starting PocketFlow Go example...")
	log.Info().Str("flowExecutionID", flowExecutionID).Msg("Ready to execute flow")
	
	// Prepare initial shared data
	log.Debug().Msg("Preparing initial shared data")
	initialData := map[string]interface{}{
		"started_at": time.Now().Format(time.RFC3339),
	}
	
	// Start the flow by sending a flow start request event
	log.Info().Msg("Publishing flow.start.requested event")
	publisher.Publish("flow.start.requested", event.FlowStartRequested{
		BaseEvent: event.BaseEvent{
			EventID:         uuid.New().String(),
			FlowExecutionID: flowExecutionID,
			Timestamp:       time.Now(),
			CorrelationID:   uuid.New().String(),
		},
		FlowDefinitionID: testFlow.ID,
		InitialSharedData: initialData,
		FlowParams:        map[string]interface{}{},
	})
	
	// Wait for either flow completion or interruption
	log.Info().Msg("Waiting for flow completion or interruption")
	select {
		case <-flowCompletedCh:
			log.Info().Msg("✅ Flow execution finished successfully")
			
		case <-sigCh:
			log.Warn().Msg("⚠️ Interrupted. Shutting down...")
			cancel()
	}
	
	// Clean up
	log.Debug().Msg("Stopping event router")
	err = watermillRouter.Stop()
	if err != nil {
		log.Error().Err(err).Msg("Error stopping router")
	}
	log.Info().Msg("PocketFlow execution completed")
}