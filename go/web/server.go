// Package web provides HTTP and WebSocket server for PocketFlow observability UI
package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/The-Pocket/PocketFlow/go/event"
	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/The-Pocket/PocketFlow/go/event/examples/branching"
	"github.com/The-Pocket/PocketFlow/go/event/examples/qa"
	"github.com/The-Pocket/PocketFlow/go/event/impl"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
)

// Server provides HTTP and WebSocket endpoints for PocketFlow UI
type Server struct {
	runner     *event.Runner
	obsManager event.ObservabilityManager
	upgrader   websocket.Upgrader
	clients    map[*websocket.Conn]bool
	clientsMux sync.RWMutex
	flows      map[string]FlowDefinition
}

// FlowDefinition represents an available flow type
type FlowDefinition struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Type        string `json:"type"`
}

// FlowExecutionRequest represents a request to start a flow
type FlowExecutionRequest struct {
	FlowType    string                 `json:"flow_type"`
	InitialData map[string]interface{} `json:"initial_data,omitempty"`
}

// FlowExecutionResponse represents the response from starting a flow
type FlowExecutionResponse struct {
	Success         bool   `json:"success"`
	FlowExecutionID string `json:"flow_execution_id,omitempty"`
	Error           string `json:"error,omitempty"`
}

// NewServer creates a new web server instance
func NewServer(runner *event.Runner, obsManager event.ObservabilityManager) *Server {
	return &Server{
		runner:     runner,
		obsManager: obsManager,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins for demo
			},
		},
		clients: make(map[*websocket.Conn]bool),
		flows:   getAvailableFlows(),
	}
}

// Start starts the web server on the specified port
func (s *Server) Start(port int) error {
	// Setup routes
	http.HandleFunc("/", s.handleIndex)
	http.HandleFunc("/ws", s.handleWebSocket)
	http.HandleFunc("/api/flows", s.handleListFlows)
	http.HandleFunc("/api/flows/start", s.handleStartFlow)
	http.HandleFunc("/api/flows/status", s.handleFlowStatus)
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("web/static/"))))

	// Add WebSocket observer to observability manager
	wsObserver := NewWebSocketObserver(s)
	if err := s.obsManager.AddObserver(wsObserver); err != nil {
		log.Error().Err(err).Msg("Failed to add WebSocket observer")
		return err
	}

	addr := fmt.Sprintf(":%d", port)
	log.Info().Str("addr", addr).Msg("Starting PocketFlow Web UI server")
	return http.ListenAndServe(addr, nil)
}

// handleIndex serves the main UI page
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "web/static/index.html")
}

// handleWebSocket handles WebSocket connections
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error().Err(err).Msg("Failed to upgrade to WebSocket")
		return
	}
	defer conn.Close()

	// Register client
	s.clientsMux.Lock()
	s.clients[conn] = true
	s.clientsMux.Unlock()

	log.Info().Msg("New WebSocket client connected")

	// Remove client when connection closes
	defer func() {
		s.clientsMux.Lock()
		delete(s.clients, conn)
		s.clientsMux.Unlock()
		log.Info().Msg("WebSocket client disconnected")
	}()

	// Keep connection alive
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			log.Debug().Err(err).Msg("WebSocket read error, closing connection")
			break
		}
	}
}

// BroadcastEvent sends an event to all connected WebSocket clients
func (s *Server) BroadcastEvent(event interface{}) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	s.clientsMux.RLock()
	clients := make([]*websocket.Conn, 0, len(s.clients))
	for client := range s.clients {
		clients = append(clients, client)
	}
	s.clientsMux.RUnlock()

	// Send to all clients
	for _, client := range clients {
		if err := client.WriteMessage(websocket.TextMessage, eventJSON); err != nil {
			log.Debug().Err(err).Msg("Failed to send WebSocket message, removing client")
			s.clientsMux.Lock()
			delete(s.clients, client)
			s.clientsMux.Unlock()
			client.Close()
		}
	}

	return nil
}

// handleListFlows returns available flow types
func (s *Server) handleListFlows(w http.ResponseWriter, r *http.Request) {
	flows := make([]FlowDefinition, 0, len(s.flows))
	for _, flow := range s.flows {
		flows = append(flows, flow)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(flows)
}

// handleStartFlow starts a new flow execution
func (s *Server) handleStartFlow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req FlowExecutionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(FlowExecutionResponse{
			Success: false,
			Error:   "Invalid JSON in request body",
		})
		return
	}

	// Create the requested flow
	var flow core.Flow
	switch req.FlowType {
	case "basic":
		flow = s.createBasicFlow()
	case "qa":
		flow = qa.CreateQAFlow()
		s.setupQANodeWorkers()
		s.runner.RegisterFlow(flow)
	case "branching":
		flow = branching.CreateBranchingFlow()
		s.setupBranchingNodeWorkers()
		s.runner.RegisterFlow(flow)
	case "delay-test":
		flow = s.createDelayTestFlow()
	default:
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(FlowExecutionResponse{
			Success: false,
			Error:   fmt.Sprintf("Unknown flow type: %s", req.FlowType),
		})
		return
	}

	// Add timestamp if not provided
	if req.InitialData == nil {
		req.InitialData = make(map[string]interface{})
	}
	if _, exists := req.InitialData["started_at"]; !exists {
		req.InitialData["started_at"] = time.Now().Format(time.RFC3339)
	}

	// Execute the flow asynchronously
	go func() {
		flowID, err := s.runner.RunFlowAndWait(flow, req.InitialData)
		if err != nil {
			log.Error().Err(err).Str("flowType", req.FlowType).Msg("Flow execution failed")
		} else {
			log.Info().Str("flowExecutionID", flowID).Str("flowType", req.FlowType).Msg("Flow execution completed")
		}
	}()

	// Return immediate response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(FlowExecutionResponse{
		Success:         true,
		FlowExecutionID: "will-be-generated", // Actual ID will be available in events
	})
}

// handleFlowStatus returns the status of active flows
func (s *Server) handleFlowStatus(w http.ResponseWriter, r *http.Request) {
	// This would require integration with the flow tracer
	// For now, return empty status
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"active_flows": []interface{}{},
	})
}

// getAvailableFlows returns the list of available flow types
func getAvailableFlows() map[string]FlowDefinition {
	return map[string]FlowDefinition{
		"basic": {
			ID:          "basic",
			Name:        "Basic QA Flow",
			Description: "Simple question-answer flow with user input and LLM response",
			Type:        "qa",
		},
		"qa": {
			ID:          "qa",
			Name:        "Question-Answering Flow",
			Description: "Enhanced QA flow with structured question-answer processing",
			Type:        "qa",
		},
		"branching": {
			ID:          "branching",
			Name:        "Branching Intent Flow",
			Description: "Flow with conditional branching based on user intent classification",
			Type:        "agent",
		},
		"delay-test": {
			ID:          "delay-test",
			Name:        "Delay Test Flow",
			Description: "Flow with delay nodes to demonstrate real-time event streaming",
			Type:        "test",
		},
	}
}

// createBasicFlow creates a basic QA flow (same as in main.go)
func (s *Server) createBasicFlow() core.Flow {
	// Set up mock LLM client
	mockLLM := event.NewMockLLMClient()
	mockLLM.AddResponse("Given the user's response", "This is a detailed explanation from the LLM based on your input.")

	// Create node workers
	questionNode := event.NewQuestionNodeWorker(
		s.runner.Publisher(),
		s.runner.StateStore(),
		"What is your question?",
	)
	answerNode := event.NewAnswerNodeWorker(s.runner.Publisher(), s.runner.StateStore(), mockLLM)

	// Register the nodes with the router
	s.runner.RegisterNodeWorkers(questionNode, answerNode)

	// Define nodes for the flow
	questionNodeDef := questionNode.NewNode(event.NodeParams{
		"question": "What would you like to know about?",
	})
	answerNodeDef := answerNode.NewNode(event.NodeParams{})

	// Define the flow
	testFlow := impl.NewFlowBuilder("basic").
		Begin(questionNodeDef).
		Then(answerNodeDef).
		Build()

	// Register the flow
	s.runner.RegisterFlow(testFlow)

	return testFlow
}

// UserInputHandler implements SimpleNodeHandler for simulated user input in web context
type UserInputHandler struct{}

// Prep handles the preparation phase
func (h *UserInputHandler) Prep(ctx core.NodeContext) (interface{}, error) {
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
func (h *UserInputHandler) Exec(ctx core.NodeContext, prepResult interface{}) (interface{}, error) {
	// In a web context, we'll simulate user input
	// In a real implementation, this would wait for user input via WebSocket
	userInput := "What's the weather like today?"
	return userInput, nil
}

// Post handles the post-processing and determines next action
func (h *UserInputHandler) Post(ctx core.NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
	userInput := execResult.(string)
	return "default", userInput, nil
}

// setupQANodeWorkers registers node workers for the QA flow
func (s *Server) setupQANodeWorkers() {
	// Create mock LLM for the answer node
	mockLLM := qa.NewMockLLMClient()
	mockLLM.AddResponse("", "This is a detailed explanation from the LLM based on your input.")

	// Create node workers using SimpleNode
	questionWorker := event.NewSimpleNode("question", &qa.QuestionHandler{}, s.runner.Publisher(), s.runner.StateStore())
	answerWorker := event.NewSimpleNode("answer", qa.NewAnswerHandler(mockLLM), s.runner.Publisher(), s.runner.StateStore())

	// Register the node workers
	s.runner.RegisterNodeWorkers(questionWorker, answerWorker)
}

// setupBranchingNodeWorkers registers node workers for the branching flow
func (s *Server) setupBranchingNodeWorkers() {
	// Create a simple user input worker that simulates user input
	userInputWorker := event.NewSimpleNode("user_input", &UserInputHandler{}, s.runner.Publisher(), s.runner.StateStore())
	
	// Create node workers for all branching flow node types
	intentClassifierWorker := event.NewSimpleNode("intent_classifier", &branching.IntentClassifierHandler{}, s.runner.Publisher(), s.runner.StateStore())
	weatherWorker := event.NewSimpleNode("weather", &branching.WeatherHandler{}, s.runner.Publisher(), s.runner.StateStore())
	timeWorker := event.NewSimpleNode("time", &branching.TimeHandler{}, s.runner.Publisher(), s.runner.StateStore())
	helpWorker := event.NewSimpleNode("help", &branching.HelpHandler{}, s.runner.Publisher(), s.runner.StateStore())
	generalWorker := event.NewSimpleNode("general", &branching.GeneralHandler{}, s.runner.Publisher(), s.runner.StateStore())

	// Register all the node workers
	s.runner.RegisterNodeWorkers(userInputWorker, intentClassifierWorker, weatherWorker, timeWorker, helpWorker, generalWorker)
}

// createDelayTestFlow creates a flow with delay nodes for testing
func (s *Server) createDelayTestFlow() core.Flow {
	// Create delay node worker
	delayWorker := NewDelayNodeWorker(s.runner.Publisher(), s.runner.StateStore())
	s.runner.RegisterNodeWorkers(delayWorker)

	// Create nodes with different delays
	delay1 := delayWorker.NewNode(event.NodeParams{
		"delay_seconds": 2,
		"message":       "First delay complete",
	})
	delay2 := delayWorker.NewNode(event.NodeParams{
		"delay_seconds": 3,
		"message":       "Second delay complete",
	})
	delay3 := delayWorker.NewNode(event.NodeParams{
		"delay_seconds": 1,
		"message":       "Final delay complete",
	})

	// Build the flow
	delayFlow := impl.NewFlowBuilder("delay-test").
		Begin(delay1).
		Then(delay2).
		Then(delay3).
		Build()

	s.runner.RegisterFlow(delayFlow)
	return delayFlow
}