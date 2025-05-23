package mocks

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
