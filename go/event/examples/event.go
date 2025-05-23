// Package event provides the main entry point for the PocketFlow Go implementation
package examples

import (
	"fmt"

	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/The-Pocket/PocketFlow/go/event/flow"
	"github.com/The-Pocket/PocketFlow/go/event/node"
	"github.com/The-Pocket/PocketFlow/go/event/observability"
	"github.com/The-Pocket/PocketFlow/go/event/store"
	"github.com/The-Pocket/PocketFlow/go/semantic"
	"github.com/rs/zerolog/log"
)

// Re-export core interfaces
type (
	// Core interfaces
	Node            = core.Node
	Flow            = core.Flow
	NodeWorker      = core.NodeWorker
	FlowWorker      = core.FlowWorker
	FlowBuilder     = core.FlowBuilder
	EventPublisher  = core.EventPublisher

	FlowRegistry      = core.FlowRegistry
	SimpleNodeHandler = core.SimpleNodeHandler
	NodeBuilder       = core.NodeBuilder
	NodeParams        = core.NodeParams

	// Messages
	BaseMessage               = core.BaseMessage
	FlowStartRequestedMessage = core.FlowStartRequestedMessage
	FlowInitializedMessage    = core.FlowInitializedMessage
	FlowCompletedMessage      = core.FlowCompletedMessage
	FlowFailedMessage         = core.FlowFailedMessage
	ExecRequestedMessage      = core.ExecRequestedMessage
	NodeCompletedMessage      = core.NodeCompletedMessage
	ExecFailedMessage         = core.ExecFailedMessage
	ProgressUpdateMessage     = core.ProgressUpdateMessage

	// Observability interfaces
	Observer             = observability.Observer
	ObservableEvent      = observability.ObservableEvent
	FlowTracer           = observability.FlowTracer
	NodeTracer           = observability.NodeTracer
	ObservabilityManager = observability.ObservabilityManager
	FlowStatus           = observability.FlowStatus

	// Observable events
	FlowStartedEvent    = observability.FlowStartedEvent
	FlowCompletedEvent  = observability.FlowCompletedEvent
	FlowFailedEvent     = observability.FlowFailedEvent
	NodeStartedEvent    = observability.NodeStartedEvent
	NodeCompletedEvent  = observability.NodeCompletedEvent
	NodeFailedEvent     = observability.NodeFailedEvent
	ProgressUpdateEvent = observability.ProgressUpdateEvent
)

// Constants
const (
	// Message types
	MessageTypeFlowStartRequested  = core.MessageTypeFlowStartRequested
	MessageTypeFlowInitialized     = core.MessageTypeFlowInitialized
	MessageTypeFlowPauseRequested  = core.MessageTypeFlowPauseRequested
	MessageTypeFlowPaused          = core.MessageTypeFlowPaused
	MessageTypeFlowResumeRequested = core.MessageTypeFlowResumeRequested
	MessageTypeFlowCancelRequested = core.MessageTypeFlowCancelRequested
	MessageTypeFlowCompleted       = core.MessageTypeFlowCompleted
	MessageTypeFlowFailed          = core.MessageTypeFlowFailed
	MessageTypeExecRequested       = core.MessageTypeExecRequested
	MessageTypeNodeCompleted       = core.MessageTypeNodeCompleted
	MessageTypeExecFailed          = core.MessageTypeExecFailed
	MessageTypeProgressUpdate      = core.MessageTypeProgressUpdate
)

// Factory functions - export from impl package

// Re-export implementation methods from impl package
var (
	// Flow factory methods
	NewFlowBuilder = flow.NewFlowBuilder

	// State and Registry factory methods
	NewSQLiteStateStore     = store.NewSQLiteStateStore
	NewInMemoryFlowRegistry = flow.NewInMemoryFlowRegistry

	// Worker factory methods
	// These are only accessible through the impl package to avoid redeclaration
	// NewFlowOrchestrator = impl.NewFlowOrchestrator
	// NewGenericFlowWorker = impl.NewGenericFlowWorker
	NewSimpleNode  = node.NewSimpleNode
	NewNodeBuilder = node.NewNodeBuilder

	// Observability factory methods
	NewObservabilityManager      = observability.NewObservabilityManager
	NewStdoutObserver            = observability.NewStdoutObserver
	NewStdoutObserverWithOptions = observability.NewStdoutObserverWithOptions
	NewStdoutFlowTracer          = observability.NewStdoutFlowTracer
)

// For backward compatibility, provide access to these types

// Load legacy nodes for backward compatibility

// NewQuestionNodeWorker creates a question node worker
func NewQuestionNodeWorker(publisher EventPublisher, stateStore semantic.StateStore, question string) NodeWorker {
	// Create a simple question handler
	handler := &questionHandler{
		defaultQuestion: question,
	}
	return NewSimpleNode("question", handler, publisher, stateStore)
}

// questionHandler implements SimpleNodeHandler for asking questions
type questionHandler struct {
	defaultQuestion string
}

func (h *questionHandler) Prep(ctx semantic.NodeContext) (interface{}, error) {
	// Get question from params or use default
	question := h.defaultQuestion
	if q, ok := ctx.Params["question"].(string); ok && q != "" {
		question = q
	}
	return question, nil
}

func (h *questionHandler) Exec(ctx semantic.NodeContext, prepResult interface{}) (interface{}, error) {
	// In a real implementation, this would prompt the user
	question := prepResult.(string)
	log.Info().Str("question", question).Msg("User asked")

	// For demo, return a mock answer
	answer := "How does PocketFlow work?"
	return answer, nil
}

func (h *questionHandler) Post(ctx semantic.NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
	userAnswer := execResult.(string)

	// Store the answer using semantic data access
	store := ctx.SemanticData.GetStore()
	if err := store.UpdateWithSemantic(ctx.FlowExecutionID, ctx.NodeID, userAnswer, []string{"user_input"}, []string{"user", "question"}); err != nil {
		return "", nil, fmt.Errorf("failed to store user input: %w", err)
	}

	return "default", userAnswer, nil
}

// DeclareOutputs makes questionHandler implement semantic.SemanticNodeHandler interface
func (h *questionHandler) DeclareOutputs() []semantic.SemanticOutput {
	return []semantic.SemanticOutput{
		{
			Key:         "user_input",
			Description: "User's response to the question",
			Tags:        []string{"user", "question"},
		},
	}
}

// NewAnswerNodeWorker creates an answer node worker
func NewAnswerNodeWorker(publisher EventPublisher, stateStore semantic.StateStore, llmClient interface{}) NodeWorker {
	// Create a simple answer handler
	handler := &answerHandler{
		llmClient: llmClient,
	}
	return NewSimpleNode("answer", handler, publisher, stateStore)
}

// answerHandler implements SimpleNodeHandler for generating answers
type answerHandler struct {
	llmClient interface{}
}

func (h *answerHandler) Prep(ctx semantic.NodeContext) (interface{}, error) {
	// Get user input using semantic data access
	userAnswer, err := ctx.SemanticData.UserInput()
	if err != nil {
		return nil, fmt.Errorf("user input not found: %w", err)
	}
	return userAnswer, nil
}

func (h *answerHandler) Exec(ctx semantic.NodeContext, prepResult interface{}) (interface{}, error) {
	userAnswer := prepResult.(string)

	// Create prompt
	prompt := fmt.Sprintf("Given the user's response: %s", userAnswer)
	log.Info().Str("prompt", prompt).Msg("Generating response")

	// Call LLM
	var response string
	_, ok := h.llmClient.(*MockLLMClient)
	if ok {
		response = "PocketFlow is an event-driven framework for building LLM applications using a graph-based workflow approach."
	} else {
		response = "This is a mock response because no LLM client was provided."
	}

	return response, nil
}

func (h *answerHandler) Post(ctx semantic.NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
	llmResponse := execResult.(string)

	// Store the response using semantic data access
	store := ctx.SemanticData.GetStore()
	if err := store.UpdateWithSemantic(ctx.FlowExecutionID, ctx.NodeID, llmResponse, []string{"response"}, []string{"llm", "answer"}); err != nil {
		return "", nil, fmt.Errorf("failed to store response: %w", err)
	}

	return "default", llmResponse, nil
}

// DeclareOutputs makes answerHandler implement semantic.SemanticNodeHandler interface
func (h *answerHandler) DeclareOutputs() []semantic.SemanticOutput {
	return []semantic.SemanticOutput{
		{
			Key:         "response",
			Description: "LLM generated response to the user's input",
			Tags:        []string{"llm", "answer"},
		},
	}
}

// NewMockLLMClient creates a mock LLM client
func NewMockLLMClient() *MockLLMClient {
	return &MockLLMClient{responses: make(map[string]string)}
}

// MockLLMClient implements a mock LLM client
type MockLLMClient struct {
	responses map[string]string
}

// AddResponse adds a mock response for a specific prompt
func (m *MockLLMClient) AddResponse(promptPart string, response string) {
	m.responses[promptPart] = response
}

// Generate generates a response for a prompt
func (m *MockLLMClient) Generate(prompt string) (string, error) {
	return "This is a mock response", nil
}
