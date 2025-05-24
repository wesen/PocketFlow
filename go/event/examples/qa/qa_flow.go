// Package qa provides a simple question-answering flow example
package qa

import (
	"fmt"

	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/The-Pocket/PocketFlow/go/event/flow"
	"github.com/The-Pocket/PocketFlow/go/event/node"
	"github.com/The-Pocket/PocketFlow/go/semantic"
	"github.com/rs/zerolog/log"
)

// QuestionHandler implements SemanticNodeHandler for user interaction
type QuestionHandler struct{}

// Prep handles the preparation phase
func (h *QuestionHandler) Prep(ctx semantic.NodeContext) (interface{}, error) {
	// Get question prompt from params or use default
	prompt := "What would you like to know about?"
	if val, ok := ctx.Params["prompt"]; ok {
		if promptStr, ok := val.(string); ok && promptStr != "" {
			prompt = promptStr
		}
	}
	return prompt, nil
}

// Exec handles the actual processing
func (h *QuestionHandler) Exec(ctx semantic.NodeContext, prepResult interface{}) (interface{}, error) {
	// In a real implementation, this would prompt the user for input
	// For this example, we'll simulate user input
	prompt := prepResult.(string)
	log.Info().Str("prompt", prompt).Msg("Asking user question")

	// Simulate user response
	userAnswer := "How does PocketFlow work?"
	return userAnswer, nil
}

// Post handles the post-processing and determines next action
func (h *QuestionHandler) Post(ctx semantic.NodeContext, prepResult, execResult interface{}) (string, interface{},  error) {
	userAnswer := execResult.(string)
	return "default", userAnswer, nil
}

// DeclareOutputs implements SemanticNodeHandler interface
func (h *QuestionHandler) DeclareOutputs() []semantic.SemanticOutput {
	return []semantic.SemanticOutput{
		{
			Key:         "user_input",
			Description: "The user's question or input",
			Tags:        []string{"question", "user_interaction"},
		},
	}
}

// LLMInterface defines a simple interface for LLM clients
type LLMInterface interface {
	Generate(prompt string) (string, error)
}

// MockLLMClient implements LLMInterface for testing
type MockLLMClient struct {
	responses map[string]string
}

// NewMockLLMClient creates a new mock LLM client
func NewMockLLMClient() *MockLLMClient {
	return &MockLLMClient{
		responses: make(map[string]string),
	}
}

// AddResponse adds a mock response for a specific prompt
func (m *MockLLMClient) AddResponse(promptPart string, response string) {
	m.responses[promptPart] = response
}

// Generate generates a response for a prompt
func (m *MockLLMClient) Generate(prompt string) (string, error) {
	// Look for any matching part in the prompt
	for part, response := range m.responses {
		if part == "" || part == "*" { // Default fallback
			return response, nil
		}
		if prompt == part { // Exact match
			return response, nil
		}
	}

	// Default response
	return "I don't know how to respond to that.", nil
}

// AnswerHandler implements SemanticNodeHandler for LLM responses
type AnswerHandler struct {
	llm LLMInterface
}

// NewAnswerHandler creates a new AnswerHandler
func NewAnswerHandler(llm LLMInterface) *AnswerHandler {
	return &AnswerHandler{llm: llm}
}

// Prep handles the preparation phase
func (h *AnswerHandler) Prep(ctx semantic.NodeContext) (interface{}, error) {
	// Get the user's question using semantic data accessor
	userQuestion, err := ctx.SemanticData.GetString("result:node:question")
	if err != nil {
		return nil, fmt.Errorf("user question not found: %w", err)
	}
	return userQuestion, nil
}

// Exec handles the actual processing
func (h *AnswerHandler) Exec(ctx semantic.NodeContext, prepResult interface{}) (interface{}, error) {
	userQuestion := prepResult.(string)

	// Construct prompt
	prompt := fmt.Sprintf("Given the user's question: %s\nProvide a detailed explanation.", userQuestion)

	// Call LLM
	log.Info().Str("prompt", prompt).Msg("Calling LLM")

	llmResponse, err := h.llm.Generate(prompt)
	if err != nil {
		return nil, fmt.Errorf("LLM generation failed: %w", err)
	}

	return llmResponse, nil
}

// Post handles the post-processing and determines next action
func (h *AnswerHandler) Post(ctx semantic.NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
	llmResponse := execResult.(string)
	return "default", llmResponse, nil
}

// DeclareOutputs implements SemanticNodeHandler interface
func (h *AnswerHandler) DeclareOutputs() []semantic.SemanticOutput {
	return []semantic.SemanticOutput{
		{
			Key:         "response",
			Description: "The LLM's response to the user's question",
			Tags:        []string{"answer", "llm_output"},
		},
	}
}

// CreateQAFlow creates a simple question-answering flow
func CreateQAFlow() core.Flow {
	// Define node definitions
	questionNodeDef := 	node.NewNode("question", core.NodeParams{
		"prompt": "What would you like to know about?",
	})
	answerNodeDef := node.NewNode("answer", core.NodeParams{})

	// Define flow using builder pattern
	qaFlow := flow.NewFlowBuilder("qa_chain").
		Begin(questionNodeDef).
		Then(answerNodeDef).
		Build()

	return qaFlow
}
