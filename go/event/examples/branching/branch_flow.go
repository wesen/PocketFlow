// Package branching provides an example of a branching workflow
package branching

import (
	"fmt"
	"strings"
	"time"

	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/The-Pocket/PocketFlow/go/event/impl"
)

// IntentClassifierHandler implements SimpleNodeHandler for intent classification
type IntentClassifierHandler struct{}

// Prep handles the preparation phase
func (h *IntentClassifierHandler) Prep(ctx core.NodeContext) (interface{}, error) {
	// Get user question from shared data by looking for string values (user input from user_input node)
	for key, value := range ctx.SharedData {
		if key == "started_at" { // Skip metadata
			continue
		}
		if userQuestion, ok := value.(string); ok && userQuestion != "" {
			// Check if this looks like user input (not an intent classification result)
			if !strings.Contains(userQuestion, "_intent") {
				return userQuestion, nil
			}
		}
	}
	
	// Fallback to params or default
	if userQuestion, ok := ctx.Params["default_question"].(string); ok {
		return userQuestion, nil
	}
	
	return "Default question", nil
}

// Exec handles the actual processing
func (h *IntentClassifierHandler) Exec(ctx core.NodeContext, prepResult interface{}) (interface{}, error) {
	userQuestion := prepResult.(string)

	// Simple rule-based classifier
	// In a real implementation, this would use an LLM or ML model
	question := strings.ToLower(userQuestion)

	if strings.Contains(question, "weather") {
		return "weather_intent", nil
	} else if strings.Contains(question, "time") {
		return "time_intent", nil
	} else if strings.Contains(question, "help") {
		return "help_intent", nil
	}

	return "general_intent", nil
}

// Post handles the post-processing and determines next action
func (h *IntentClassifierHandler) Post(ctx core.NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
	intent := execResult.(string)
	return intent, intent, nil
}

// WeatherHandler implements SimpleNodeHandler for weather information
type WeatherHandler struct{}

// Prep handles the preparation phase
func (h *WeatherHandler) Prep(ctx core.NodeContext) (interface{}, error) {
	// Get user query from shared data by looking for string values (user input from user_input node)
	var userQuery string
	for key, value := range ctx.SharedData {
		if key == "started_at" { // Skip metadata
			continue
		}
		if query, ok := value.(string); ok && query != "" {
			// Check if this looks like user input (not an intent classification result)
			if !strings.Contains(query, "_intent") {
				userQuery = query
				break
			}
		}
	}
	
	if userQuery == "" {
		return nil, fmt.Errorf("user input not found in shared data")
	}

	// Extract location from query (simplified)
	location := "default_location"
	if strings.Contains(strings.ToLower(userQuery), "weather in") {
		parts := strings.Split(strings.ToLower(userQuery), "weather in")
		if len(parts) > 1 {
			location = strings.TrimSpace(parts[1])
		}
	}

	return location, nil
}

// Exec handles the actual processing
func (h *WeatherHandler) Exec(ctx core.NodeContext, prepResult interface{}) (interface{}, error) {
	location := prepResult.(string)

	// In a real implementation, call an actual weather API
	// For this example, we'll return mock data
	weatherData := map[string]interface{}{
		"location":    location,
		"temperature": 72,
		"condition":   "sunny",
		"humidity":    45,
	}

	return weatherData, nil
}

// Post handles the post-processing and determines next action
func (h *WeatherHandler) Post(ctx core.NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
	weatherData := execResult.(map[string]interface{})

	// Form a readable response
	response := fmt.Sprintf(
		"The weather in %s is %s with a temperature of %d°F and humidity of %d%%.",
		weatherData["location"],
		weatherData["condition"],
		weatherData["temperature"],
		weatherData["humidity"],
	)

	return "default", response, nil
}

// TimeHandler implements SimpleNodeHandler for time information
type TimeHandler struct{}

// Prep handles the preparation phase
func (h *TimeHandler) Prep(ctx core.NodeContext) (interface{}, error) {
	// No special preparation needed
	return nil, nil
}

// Exec handles the actual processing
func (h *TimeHandler) Exec(ctx core.NodeContext, prepResult interface{}) (interface{}, error) {
	// Get current time
	currentTime := fmt.Sprintf("The current time is %s", time.Now().Format("15:04:05"))
	return currentTime, nil
}

// Post handles the post-processing and determines next action
func (h *TimeHandler) Post(ctx core.NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
	timeInfo := execResult.(string)
	return "default", timeInfo, nil
}

// HelpHandler implements SimpleNodeHandler for providing help
type HelpHandler struct{}

// Prep handles the preparation phase
func (h *HelpHandler) Prep(ctx core.NodeContext) (interface{}, error) {
	// No special preparation needed
	return nil, nil
}

// Exec handles the actual processing
func (h *HelpHandler) Exec(ctx core.NodeContext, prepResult interface{}) (interface{}, error) {
	helpMessage := "You can ask me about the weather, the time, or general questions."
	return helpMessage, nil
}

// Post handles the post-processing and determines next action
func (h *HelpHandler) Post(ctx core.NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
	helpInfo := execResult.(string)
	return "default", helpInfo, nil
}

// GeneralHandler implements SimpleNodeHandler for general responses
type GeneralHandler struct{}

// Prep handles the preparation phase
func (h *GeneralHandler) Prep(ctx core.NodeContext) (interface{}, error) {
	// Get user query from shared data by looking for string values (user input from user_input node)
	for key, value := range ctx.SharedData {
		if key == "started_at" { // Skip metadata
			continue
		}
		if userQuery, ok := value.(string); ok && userQuery != "" {
			// Check if this looks like user input (not an intent classification result)
			if !strings.Contains(userQuery, "_intent") {
				return userQuery, nil
			}
		}
	}
	return nil, fmt.Errorf("user input not found in shared data")
}

// Exec handles the actual processing
func (h *GeneralHandler) Exec(ctx core.NodeContext, prepResult interface{}) (interface{}, error) {
	userQuery := prepResult.(string)

	// In a real implementation, call an LLM
	// For this example, we'll return a fixed response
	response := fmt.Sprintf(
		"I received your query: '%s'. I would normally provide a detailed response using an LLM.",
		userQuery,
	)

	return response, nil
}

// Post handles the post-processing and determines next action
func (h *GeneralHandler) Post(ctx core.NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
	response := execResult.(string)
	return "default", response, nil
}

// CreateBranchingFlow creates a flow with branches based on intent
func CreateBranchingFlow() core.Flow {
	// Define node definitions
	inputNode := impl.NewNode("user_input", core.NodeParams{
		"prompt": "What would you like to know?",
	})
	intentClassifierNode := impl.NewNode("intent_classifier", core.NodeParams{})
	weatherNode := impl.NewNode("weather", core.NodeParams{})
	timeNode := impl.NewNode("time", core.NodeParams{})
	helpNode := impl.NewNode("help", core.NodeParams{})
	generalNode := impl.NewNode("general", core.NodeParams{})

	// Define flow using builder pattern with branching
	branchingFlow := impl.NewFlowBuilder("branching").
		Begin(inputNode).
		Then(intentClassifierNode).
		On("weather_intent", weatherNode).
		On("time_intent", timeNode).
		On("help_intent", helpNode).
		On("general_intent", generalNode).
		Build()

	return branchingFlow
}
