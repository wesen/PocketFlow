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
	"github.com/The-Pocket/PocketFlow/go/web"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// Parse command-line flags
	flowType := flag.String("flow", "basic", "Flow type to run (basic, qa, branching)")
	visualizeOnly := flag.Bool("visualize", false, "Only visualize the flow without running it")
	enableObservability := flag.Bool("observability", false, "Enable observability with colorized console output")
	observabilityVerbose := flag.Bool("observability-verbose", false, "Enable verbose observability output (implies -observability)")
	webUI := flag.Bool("web", false, "Start web UI server instead of running flows directly")
	webPort := flag.Int("web-port", 8080, "Port for web UI server")
	useRedis := flag.Bool("redis", true, "Use Redis Streams for messaging (default: true)")
	redisAddr := flag.String("redis-addr", "localhost:6379", "Redis address (default: localhost:6379)")
	
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nPocketFlow Go - Event-driven LLM application framework\n\n")
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  %s -flow basic                    # Run basic QA flow with Redis\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -flow qa -observability        # Run QA flow with observability\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -flow branching -observability-verbose  # Run branching flow with verbose tracing\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -visualize -flow basic         # Just show the flow diagram\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -web                           # Start web UI server on port 8080\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -redis=false -flow basic       # Run with in-memory messaging\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -redis-addr redis:6379 -flow basic  # Run with custom Redis address\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nFlags:\n")
		flag.PrintDefaults()
	}
	
	flag.Parse()

	// Setup zerolog with console logger
	zerolog := log.Output(zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
		NoColor:    false,
	}).With().Caller().Logger()
	log.Logger = zerolog

	// Check if observability should be enabled
	useObservability := *enableObservability || *observabilityVerbose
	
	// If web UI is requested, start the web server
	if *webUI {
		if err := startWebServer(*webPort, *useRedis, *redisAddr); err != nil {
			log.Fatal().Err(err).Msg("Web server failed to start")
		}
		return
	}
	
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

	// Create runner with Redis or in-memory messaging
	var runner *event.Runner
	if *useRedis {
		log.Info().Str("redisAddr", *redisAddr).Msg("🔗 Using Redis Streams for messaging")
		runner = event.NewRunnerWithRedis(*redisAddr, event.WithDebugMode(true),
			event.WithFlowCompletedHandler(completionHandler),
		)
	} else {
		log.Info().Msg("💾 Using in-memory messaging")
		runner = event.NewRunner(event.WithDebugMode(true),
			event.WithFlowCompletedHandler(completionHandler),
		)
	}

	// Initialize the runner
	if err := runner.Init(); err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize runner")
		os.Exit(1)
	}

	// Setup observability if enabled
	var obsManager event.ObservabilityManager
	if useObservability {
		log.Info().Msg("🔍 Setting up observability...")
		
		// Create observability manager with the runner's subscriber
		if *useRedis {
			// For Redis, create a separate observability router with its own consumer group
			obsRouter, err := event.NewObservabilityRouterWithRedis(*redisAddr, nil)
			if err != nil {
				log.Error().Err(err).Msg("Failed to create observability router")
			} else {
				// Create observability manager with separate subscriber for Redis
				obsSubscriber := event.NewWatermillSubscriber(obsRouter.Subscriber)
				obsManager = event.NewObservabilityManager(obsSubscriber)
			}
		} else {
			// For in-memory, use the same subscriber as the main runner
			obsManager = event.NewObservabilityManager(runner.Subscriber())
		}
		
		// Add stdout observer with appropriate verbosity
		stdoutObserver := event.NewStdoutObserverWithOptions("console", true, *observabilityVerbose)
		err := obsManager.AddObserver(stdoutObserver)
		if err != nil {
			log.Error().Err(err).Msg("Failed to add stdout observer")
		} else {
			log.Info().Bool("verbose", *observabilityVerbose).Msg("✓ Added colorized console observer")
		}
		
		// Add flow tracer for detailed flow tracking
		flowTracer := event.NewStdoutFlowTracer("flow_tracer")
		err = obsManager.AddObserver(flowTracer)
		if err != nil {
			log.Error().Err(err).Msg("Failed to add flow tracer")
		} else {
			log.Info().Msg("✓ Added flow tracer for status tracking")
		}
		
		log.Info().Int("observers", len(obsManager.ListObservers())).Msg("🎯 Observability system ready")
	}

	// Create and prepare the requested flow
	var flow core.Flow
	var flowName string

	switch *flowType {
	case "basic":
		flowName = "Basic QA Flow"
		flow = setupBasicFlow(runner)

	case "qa":
		flowName = "Question-Answering Flow"
		flow = qa.CreateQAFlow()
		runner.RegisterFlow(flow)
		setupQANodeWorkers(runner)

	case "branching":
		flowName = "Branching Intent Flow"
		flow = branching.CreateBranchingFlow()
		runner.RegisterFlow(flow)
		setupBranchingNodeWorkers(runner)

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

	// Start observability system if enabled
	if useObservability && obsManager != nil {
		if err := obsManager.Start(); err != nil {
			log.Error().Err(err).Msg("Failed to start observability system")
		} else {
			log.Info().Msg("🎯 Observability system started and subscribed to topics")
		}
		defer func() {
			if err := obsManager.Stop(); err != nil {
				log.Error().Err(err).Msg("Failed to stop observability system")
			}
		}()
	}

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

func setupBasicFlow(runner *event.Runner) core.Flow {
	// Set up mock LLM client
	log.Debug().Msg("Creating mock LLM client")
	mockLLM := event.NewMockLLMClient()
	mockLLM.AddResponse("Given the user's response", "This is a detailed explanation from the LLM based on your input.")
	log.Info().Msg("Mock LLM client initialized with predefined responses")

	// Create node workers
	log.Debug().Msg("Creating node workers")
	
	questionNode := event.NewQuestionNodeWorker(
		runner.Publisher(),
		runner.StateStore(),
		"What is your question?",
	)
	answerNode := event.NewAnswerNodeWorker(runner.Publisher(), runner.StateStore(), mockLLM)
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

// setupQANodeWorkers registers node workers for the QA flow
func setupQANodeWorkers(runner *event.Runner) {
	// Create mock LLM for the answer node
	mockLLM := qa.NewMockLLMClient()
	mockLLM.AddResponse("", "This is a detailed explanation from the LLM based on your input.")

	// Create node workers using SimpleNode
	questionWorker := event.NewSimpleNode("question", &qa.QuestionHandler{}, runner.Publisher(), runner.StateStore())
	answerWorker := event.NewSimpleNode("answer", qa.NewAnswerHandler(mockLLM), runner.Publisher(), runner.StateStore())

	// Register the node workers
	runner.RegisterNodeWorkers(questionWorker, answerWorker)
}

// setupBranchingNodeWorkers registers node workers for the branching flow
func setupBranchingNodeWorkers(runner *event.Runner) {
	// Create a simple user input worker that simulates user input
	userInputWorker := event.NewSimpleNode("user_input", &CLIUserInputHandler{}, runner.Publisher(), runner.StateStore())
	
	// Create node workers for all branching flow node types
	intentClassifierWorker := event.NewSimpleNode("intent_classifier", &branching.IntentClassifierHandler{}, runner.Publisher(), runner.StateStore())
	weatherWorker := event.NewSimpleNode("weather", &branching.WeatherHandler{}, runner.Publisher(), runner.StateStore())
	timeWorker := event.NewSimpleNode("time", &branching.TimeHandler{}, runner.Publisher(), runner.StateStore())
	helpWorker := event.NewSimpleNode("help", &branching.HelpHandler{}, runner.Publisher(), runner.StateStore())
	generalWorker := event.NewSimpleNode("general", &branching.GeneralHandler{}, runner.Publisher(), runner.StateStore())

	// Register all the node workers
	runner.RegisterNodeWorkers(userInputWorker, intentClassifierWorker, weatherWorker, timeWorker, helpWorker, generalWorker)
}

// CLIUserInputHandler implements SimpleNodeHandler for command line user input
type CLIUserInputHandler struct{}

// Prep handles the preparation phase
func (h *CLIUserInputHandler) Prep(ctx core.NodeContext) (interface{}, error) {
	// Get prompt from params or use default
	prompt := "What would you like to know?"
	if val, ok := ctx.Params["prompt"]; ok {
		if promptStr, ok := val.(string); ok && promptStr != "" {
			prompt = promptStr
		}
	}
	return prompt, nil
}

// Exec handles the actual processing
func (h *CLIUserInputHandler) Exec(ctx core.NodeContext, prepResult interface{}) (interface{}, error) {
	// For CLI, we'll simulate user input
	// In a real implementation, this could read from stdin
	userInput := "What's the weather like in San Francisco?"
	log.Info().Str("simulated_input", userInput).Msg("📝 Simulated user input")
	return userInput, nil
}

// Post handles the post-processing and determines next action
func (h *CLIUserInputHandler) Post(ctx core.NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
	userInput := execResult.(string)
	return "default", userInput, nil
}

// startWebServer starts the web UI server
func startWebServer(port int, useRedis bool, redisAddr string) error {
	log.Info().Int("port", port).Msg("🌐 Starting PocketFlow Web UI server...")

	// Create a runner for the web server (use Redis by default for web server)
	var runner *event.Runner
	if useRedis {
		runner = event.NewRunnerWithRedis(redisAddr, event.WithDebugMode(true))
	} else {
		runner = event.NewRunner(event.WithDebugMode(true))
	}
	if err := runner.Init(); err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize runner for web server")
		return err
	}

	// Setup observability manager
	obsManager := event.NewObservabilityManager(runner.Subscriber())
	
	// Add stdout observer for server-side logging
	stdoutObserver := event.NewStdoutObserverWithOptions("server-console", true, false)
	if err := obsManager.AddObserver(stdoutObserver); err != nil {
		log.Error().Err(err).Msg("Failed to add stdout observer")
	}

	// Start the runner
	_, cancel := runner.Start()
	defer cancel()

	// Start observability system
	if err := obsManager.Start(); err != nil {
		log.Error().Err(err).Msg("Failed to start observability system")
	}
	defer func() {
		if err := obsManager.Stop(); err != nil {
			log.Error().Err(err).Msg("Failed to stop observability system")
		}
	}()

	// Create and start web server
	server := web.NewServer(runner, obsManager)
	
	log.Info().Int("port", port).Msg("🎯 Web UI available at http://localhost:%d")
	
	if err := server.Start(port); err != nil {
		log.Error().Err(err).Msg("Web server failed")
		return err
	}

	return nil
}
