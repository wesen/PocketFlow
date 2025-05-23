// Package main provides the entrypoint for the PocketFlow Go example application.
// It demonstrates how to use the PocketFlow event-driven system to create and run
// various types of flows, handling user interaction and LLM integration.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/The-Pocket/PocketFlow/go/event"
	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/The-Pocket/PocketFlow/go/event/examples/branching"
	"github.com/The-Pocket/PocketFlow/go/event/examples/qa"
	"github.com/The-Pocket/PocketFlow/go/event/impl"
	"github.com/The-Pocket/PocketFlow/go/event/observability"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// Parse command-line flags
	flowType := flag.String("flow", "basic", "Flow type to run (basic, qa, branching)")
	visualizeOnly := flag.Bool("visualize", false, "Only visualize the flow without running it")
	enableObservability := flag.Bool("observability", false, "Enable observability with colorized console output")
	observabilityVerbose := flag.Bool("observability-verbose", false, "Enable verbose observability output (implies -observability)")
	
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nPocketFlow Go - Event-driven LLM application framework\n\n")
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  %s -flow basic                    # Run basic QA flow\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -flow qa -observability        # Run QA flow with observability\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -flow branching -observability-verbose  # Run branching flow with verbose tracing\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -visualize -flow basic         # Just show the flow diagram\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nFlags:\n")
		flag.PrintDefaults()
	}
	
	flag.Parse()

	// Setup zerolog with console logger
	zerolog := log.Output(zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
		NoColor:    false,
	})
	log.Logger = zerolog

	// Check if observability should be enabled
	useObservability := *enableObservability || *observabilityVerbose
	
	// Create a runner with default options
	// Create a closure with the runner reference
	completionHandler := func(runner *event.Runner, completed core.FlowCompletedMessage) error {
		log.Info().Msg("🎉 Flow completed successfully!")

		// Get the final results from shared data
		sharedData, err := runner.GetSharedData(completed.FlowExecutionID)
		if err != nil {
			log.Error().Err(err).Msg("Error getting final results")
			return nil
		}

		log.Info().Msg("📋 Flow results:")
		for key, value := range sharedData {
			log.Info().Interface(key, value).Msg("Data")
		}

		return nil
	}

	runner := event.NewRunner(event.WithDebugMode(true),
		event.WithFlowCompletedHandler(completionHandler),
	)

	// Initialize the runner
	if err := runner.Init(); err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize runner")
		os.Exit(1)
	}

	// Setup observability if enabled
	var observablePublisher *observability.ObservableEventPublisher
	if useObservability {
		log.Info().Msg("🔍 Setting up observability...")
		
		// Create observability manager
		obsManager := observability.NewObservabilityManager()
		
		// Add stdout observer with appropriate verbosity
		stdoutObserver := observability.NewStdoutObserverWithOptions("console", true, *observabilityVerbose)
		err := obsManager.AddObserver(stdoutObserver)
		if err != nil {
			log.Error().Err(err).Msg("Failed to add stdout observer")
		} else {
			log.Info().Bool("verbose", *observabilityVerbose).Msg("✓ Added colorized console observer")
		}
		
		// Add flow tracer for detailed flow tracking
		flowTracer := observability.NewStdoutFlowTracer("flow_tracer")
		err = obsManager.AddObserver(flowTracer)
		if err != nil {
			log.Error().Err(err).Msg("Failed to add flow tracer")
		} else {
			log.Info().Msg("✓ Added flow tracer for status tracking")
		}
		
		// Wrap the runner's publisher with observability
		observablePublisher = observability.NewObservableEventPublisher(runner.Publisher(), obsManager)
		
		log.Info().Int("observers", len(obsManager.ListObservers())).Msg("🎯 Observability system ready")
	}

	// Create and prepare the requested flow
	var flow core.Flow
	var flowName string

	switch *flowType {
	case "basic":
		flowName = "Basic QA Flow"
		flow = setupBasicFlow(runner, observablePublisher)

	case "qa":
		flowName = "Question-Answering Flow"
		flow = qa.CreateQAFlow()
		runner.RegisterFlow(flow)

	case "branching":
		flowName = "Branching Intent Flow"
		flow = branching.CreateBranchingFlow()
		runner.RegisterFlow(flow)

	default:
		log.Fatal().Str("flowType", *flowType).Msg("Unknown flow type")
		os.Exit(1)
	}

	// If we only want to visualize, print the diagram and exit
	if *visualizeOnly {
		diagram := flow.Visualize()
		fmt.Printf("\n%s Flow Visualization:\n\n%s\n", flowName, diagram)
		return
	}

	// Start the runner
	_, cancel := runner.Start()
	defer cancel()

	log.Info().Str("flowName", flowName).Msg("🚀 Starting PocketFlow Go example...")

	// Show observability status
	if useObservability {
		log.Info().Msg("📊 Observability enabled - watch for real-time flow and node execution events below")
		if *observabilityVerbose {
			log.Info().Msg("🔍 Verbose mode enabled - showing detailed parameters and results")
		}
	}

	// Run the flow and wait for completion
	initialData := map[string]interface{}{
		"started_at": time.Now().Format(time.RFC3339),
	}

	flowID, err := runner.RunFlowAndWait(flow, initialData)
	if err != nil {
		log.Error().Err(err).Msg("Flow execution failed")
	} else {
		log.Info().Str("flowExecutionID", flowID).Msg("✅ Flow execution finished successfully")
	}

	// Clean up
	if err := runner.Stop(); err != nil {
		log.Error().Err(err).Msg("Error stopping runner")
	}
	log.Info().Msg("PocketFlow execution completed")
}

func setupBasicFlow(runner *event.Runner, observablePublisher *observability.ObservableEventPublisher) core.Flow {
	// Set up mock LLM client
	log.Debug().Msg("Creating mock LLM client")
	mockLLM := event.NewMockLLMClient()
	mockLLM.AddResponse("Given the user's response", "This is a detailed explanation from the LLM based on your input.")
	log.Info().Msg("Mock LLM client initialized with predefined responses")

	// Create node workers
	log.Debug().Msg("Creating node workers")
	
	// Use observable publisher if available, otherwise use the regular publisher
	var publisher core.EventPublisher = runner.Publisher()
	if observablePublisher != nil {
		publisher = observablePublisher
		log.Info().Msg("🔍 Using observable publisher for enhanced tracing")
	}
	
	questionNode := event.NewQuestionNodeWorker(
		publisher,
		runner.StateStore(),
		"What is your question?",
	)
	answerNode := event.NewAnswerNodeWorker(publisher, runner.StateStore(), mockLLM)
	log.Info().Msg("Node workers created")

	// Register the nodes with the router
	runner.RegisterNodeWorkers(questionNode, answerNode)
	log.Info().Str("nodes", "question,answer").Msg("Node workers registered")

	// Define nodes for the flow
	log.Debug().Msg("Creating nodes for test flow")
	questionNodeDef := questionNode.NewNode(event.NodeParams{
		"question": "What would you like to know about?",
	})
	answerNodeDef := answerNode.NewNode(event.NodeParams{})

	// Define the flow
	log.Debug().Msg("Defining test flow with builder pattern")
	testFlow := impl.NewFlowBuilder("basic").
		Begin(questionNodeDef).
		Then(answerNodeDef).
		Build()

	// Register the flow
	runner.RegisterFlow(testFlow)
	log.Info().Str("flowID", testFlow.ID()).Msg("Flow registered successfully")

	return testFlow
}
