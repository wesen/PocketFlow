package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/The-Pocket/PocketFlow/go/event"
	"github.com/The-Pocket/PocketFlow/go/logger"
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
	watermillRouter := event.NewWatermillEventRouter(nil)
	publisher := event.NewWatermillPublisher(watermillRouter.PubSub)
	log.Info().Msg("Event router and publisher initialized")

	// Create the flow orchestrator
	log.Debug().Msg("Creating flow orchestrator")
	orchestrator := event.NewFlowOrchestrator(publisher, stateStore, flowRegistry)
	log.Info().Msg("Flow orchestrator created")

	// Update the router with the orchestrator
	log.Debug().Msg("Adding orchestrator to router")
	watermillRouter.UpdateOrchestrator(orchestrator)

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
	watermillRouter.RegisterAllNodeWorkers(questionNode, answerNode)
	log.Info().Str("nodes", "question,answer").Msg("Node workers registered")

	// Define nodes for our test flow using the builder pattern
	log.Debug().Msg("Creating nodes for test flow")
	questionNodeDef := event.NewNode("question", map[string]interface{}{
		"question": "What would you like to know about?",
	})
	answerNodeDef := event.NewNode("answer", map[string]interface{}{})

	// Define our test flow using the builder pattern
	log.Debug().Msg("Defining test flow with builder pattern")
	testFlow := event.NewFlowBuilder().
		Begin(questionNodeDef).
		Then(answerNodeDef).
		Build()

	// Register the flow with the registry
	log.Debug().Str("flowID", testFlow.ID()).Msg("Registering flow with registry")
	orchestrator.RegisterFlow(testFlow)
	log.Info().Str("flowID", testFlow.ID()).Msg("Flow registered successfully")

	// Create a flow worker for the test flow
	log.Debug().Msg("Creating flow worker for test flow")
	qaFlowWorker := event.NewGenericFlowWorker(
		testFlow.Type(),
		publisher,
		stateStore,
		flowRegistry,
		orchestrator,
	)
	log.Info().Str("flowType", testFlow.Type()).Msg("Flow worker created")

	// Register the flow worker with the router
	log.Debug().Msg("Registering flow worker with router")
	watermillRouter.RegisterFlowWorker(qaFlowWorker)
	log.Info().Str("flowType", testFlow.Type()).Msg("Flow worker registered")

	// Set up a channel to capture flow completed events
	log.Debug().Msg("Setting up flow completion channel")
	flowCompletedCh := make(chan struct{})

	// Set up handler for flow.completed events
	log.Debug().Msg("Setting up flow.completed event handler")
	watermillRouter.SetupFlowCompletionHandler(func(completed event.FlowCompletedMessage) error {
		log.Info().Msg("🎉 Flow completed successfully!")

		log.Debug().Str("flowExecutionID", completed.FlowExecutionID).Msg("Getting final results from shared data")
		// Get the final results from shared data
		sharedData, err := stateStore.GetSharedData(completed.FlowExecutionID)
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
	})

	// Set up handler for flow.failed events
	log.Debug().Msg("Setting up flow.failed event handler")
	watermillRouter.SetupFlowFailureHandler(func(failed event.FlowFailedMessage) error {
		log.Error().Str("errorMessage", failed.ErrorMessage).Str("errorDetails", failed.ErrorDetails).Msg("❌ Flow failed!")

		// Signal flow completion (with error)
		log.Debug().Msg("Signaling flow completion (with error)")
		flowCompletedCh <- struct{}{}
		return nil
	})

	// Set up handler for progress updates
	log.Debug().Msg("Setting up progress event handler")
	watermillRouter.SetupProgressHandler(func(progress event.ProgressUpdateMessage) error {
		log.Info().
			Str("status", progress.Status).
			Float64("progress", progress.Progress).
			Str("message", progress.Message).
			Msg("📊 Progress update")
		return nil
	})

	// Start the router in a goroutine
	log.Debug().Msg("Creating context for router")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Info().Msg("Starting router in background goroutine")
	go func() {
		if err := watermillRouter.Start(ctx); err != nil {
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

	// Start the flow using the orchestrator
	log.Info().Msg("Starting flow")
	orchestrator.StartFlow(testFlow.Type(), testFlow.ID(), initialData)

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
