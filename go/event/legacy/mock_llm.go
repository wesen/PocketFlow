package event

import (
	"fmt"
	"strings"
)

// MockLLMClient is a simple mock implementation of the LLMClient interface
type MockLLMClient struct {
	Responses map[string]string
}

func NewMockLLMClient() *MockLLMClient {
	return &MockLLMClient{
		Responses: make(map[string]string),
	}
}

// Add a predefined response for a specific prompt
func (m *MockLLMClient) AddResponse(prompt, response string) {
	m.Responses[prompt] = response
}

// Call implements the LLMClient interface
func (m *MockLLMClient) Call(prompt string) (string, error) {
	// Check if we have a predefined response for this exact prompt
	if response, ok := m.Responses[prompt]; ok {
		return response, nil
	}
	
	// If no exact match, look for a partial match
	for key, response := range m.Responses {
		if strings.Contains(prompt, key) {
			return response, nil
		}
	}
	
	// Default response
	return fmt.Sprintf("Mock response for: %s", prompt), nil
}