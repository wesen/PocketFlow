// Package event provides the main entry point for the PocketFlow Go implementation
package event

import (
	"fmt"

	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/The-Pocket/PocketFlow/go/event/impl"
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
	EventSubscriber = core.EventSubscriber
	StateStore      = core.StateStore
	FlowRegistry    = core.FlowRegistry
	SimpleNodeHandler = core.SimpleNodeHandler
	NodeBuilder     = core.NodeBuilder
	NodeParams      = core.NodeParams
	
	// Messages
	BaseMessage = core.BaseMessage
	FlowStartRequestedMessage = core.FlowStartRequestedMessage
	FlowInitializedMessage = core.FlowInitializedMessage
	FlowCompletedMessage = core.FlowCompletedMessage
	FlowFailedMessage = core.FlowFailedMessage
	ExecRequestedMessage = core.ExecRequestedMessage
	NodeCompletedMessage = core.NodeCompletedMessage
	ExecFailedMessage = core.ExecFailedMessage
	ProgressUpdateMessage = core.ProgressUpdateMessage
)

// Constants
const (
	// Message types
	MessageTypeFlowStartRequested = core.MessageTypeFlowStartRequested
	MessageTypeFlowInitialized = core.MessageTypeFlowInitialized
	MessageTypeFlowPauseRequested = core.MessageTypeFlowPauseRequested
	MessageTypeFlowPaused = core.MessageTypeFlowPaused
	MessageTypeFlowResumeRequested = core.MessageTypeFlowResumeRequested
	MessageTypeFlowCancelRequested = core.MessageTypeFlowCancelRequested
	MessageTypeFlowCompleted = core.MessageTypeFlowCompleted
	MessageTypeFlowFailed = core.MessageTypeFlowFailed
	MessageTypeExecRequested = core.MessageTypeExecRequested
	MessageTypeNodeCompleted = core.MessageTypeNodeCompleted
	MessageTypeExecFailed = core.MessageTypeExecFailed
	MessageTypeProgressUpdate = core.MessageTypeProgressUpdate
)

// Factory functions - export from impl package

// Re-export implementation methods from impl package
var (
	// Flow factory methods
	NewFlowBuilder = impl.NewFlowBuilder
	
	// State and Registry factory methods
	NewSQLiteStateStore = impl.NewSQLiteStateStore
	NewInMemoryFlowRegistry = impl.NewInMemoryFlowRegistry
	
	// Worker factory methods
	// These are only accessible through the impl package to avoid redeclaration
	// NewFlowOrchestrator = impl.NewFlowOrchestrator
	// NewGenericFlowWorker = impl.NewGenericFlowWorker
	NewSimpleNode = impl.NewSimpleNode
	NewNodeBuilder = impl.NewNodeBuilder
)

// For backward compatibility, provide access to these types

// Load legacy nodes for backward compatibility

// NewQuestionNodeWorker creates a question node worker
func NewQuestionNodeWorker(publisher EventPublisher, stateStore StateStore, question string) NodeWorker {
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

func (h *questionHandler) Prep(ctx core.NodeContext) (interface{}, error) {
	// Get question from params or use default
	question := h.defaultQuestion
	if q, ok := ctx.Params["question"].(string); ok && q != "" {
		question = q
	}
	return question, nil
}

func (h *questionHandler) Exec(ctx core.NodeContext, prepResult interface{}) (interface{}, error) {
	// In a real implementation, this would prompt the user
	question := prepResult.(string)
	log.Info().Str("question", question).Msg("User asked")
	
	// For demo, return a mock answer
	answer := "How does PocketFlow work?"
	return answer, nil
}

func (h *questionHandler) Post(ctx core.NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
	userAnswer := execResult.(string)
	
	// Store the answer in shared data
	ctx.StateStore.UpdateSharedData(ctx.FlowExecutionID, "user_answer", userAnswer)
	
	return "default", userAnswer, nil
}

// NewAnswerNodeWorker creates an answer node worker
func NewAnswerNodeWorker(publisher EventPublisher, stateStore StateStore, llmClient interface{}) NodeWorker {
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

func (h *answerHandler) Prep(ctx core.NodeContext) (interface{}, error) {
	// Get user answer from shared data
	userAnswer, ok := ctx.SharedData["user_answer"].(string)
	if !ok {
		return nil, fmt.Errorf("user answer not found in shared data")
	}
	return userAnswer, nil
}

func (h *answerHandler) Exec(ctx core.NodeContext, prepResult interface{}) (interface{}, error) {
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

func (h *answerHandler) Post(ctx core.NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
	llmResponse := execResult.(string)
	
	// Store the response in shared data
	ctx.StateStore.UpdateSharedData(ctx.FlowExecutionID, "llm_response", llmResponse)
	
	return "default", llmResponse, nil
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