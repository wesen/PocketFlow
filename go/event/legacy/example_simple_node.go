package event

import (
	"fmt"
	"time"
)

// Example of creating custom nodes using the SimpleNodeHandler interface

// EchoNodeHandler implements a simple echo node
type EchoNodeHandler struct {
	Prefix string
}

// Prep handles the preparation phase
func (h *EchoNodeHandler) Prep(ctx NodeContext) (interface{}, error) {
	fmt.Printf("[Echo] Starting prep phase\n")
	
	// Get prefix from params or use default
	prefix := h.Prefix
	if val, ok := ctx.Params["prefix"]; ok {
		if p, ok := val.(string); ok {
			prefix = p
		}
	}
	
	return prefix, nil
}

// Exec handles the execution phase
func (h *EchoNodeHandler) Exec(ctx NodeContext, prepResult interface{}) (interface{}, error) {
	fmt.Printf("[Echo] Starting exec phase\n")
	
	// Get prefix from prep result
	prefix, _ := prepResult.(string)
	
	// Get input from shared data
	input, ok := ctx.SharedData["user_answer"].(string)
	if !ok {
		return nil, fmt.Errorf("user_answer not found in shared data")
	}
	
	// Simulate processing time
	time.Sleep(500 * time.Millisecond)
	
	// Return the echoed message
	result := fmt.Sprintf("%s %s", prefix, input)
	fmt.Printf("[Echo] Generated: %s\n", result)
	return result, nil
}

// Post handles the post-processing phase
func (h *EchoNodeHandler) Post(ctx NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
	fmt.Printf("[Echo] Starting post phase\n")
	
	// We'll always use the default transition
	result := execResult.(string)
	
	// Add a timestamp to the result
	resultWithTimestamp := fmt.Sprintf("%s (processed at %s)", 
		result, time.Now().Format(time.RFC3339))
	
	// Return the action and final result
	return "default", resultWithTimestamp, nil
}

// Example of creating a node using the NodeBuilder

// createDelayNode creates a node that simply adds a delay and passes through data
func createDelayNode(publisher EventPublisher, stateStore StateStore) NodeWorker {
	return NewNodeBuilder("delay", publisher, stateStore).
		WithName("Processing Delay").
		WithParam("delay_ms", 1000).
		WithParam("message", "Processing...").
		WithExec(func(ctx NodeContext, prepResult interface{}) (interface{}, error) {
			// Get delay duration from params
			delayMs := 1000
			if val, ok := ctx.Params["delay_ms"].(float64); ok {
				delayMs = int(val)
			}
			
			// Get message from params
			message := "Processing..."
			if val, ok := ctx.Params["message"].(string); ok && val != "" {
				message = val
			}
			
			// Log start of delay
			fmt.Printf("[Delay] %s (delay: %dms)\n", message, delayMs)
			
			// Simulate processing with delay
			time.Sleep(time.Duration(delayMs) * time.Millisecond)
			
			// Get input from shared data
			llmResponse, ok := ctx.SharedData["llm_response"].(string)
			if !ok {
				return "No input available", nil
			}
			
			return llmResponse, nil
		}).
		Build()
}

// Example of registering simple nodes

// RegisterExampleSimpleNodes registers the example nodes with the router
func RegisterExampleSimpleNodes(router *WatermillEventRouter) {
	// Create an echo node using the handler interface
	echoHandler := &EchoNodeHandler{Prefix: "Echo:"}
	echoNode := NewSimpleNode("echo", echoHandler, router.FlowOrchestrator.Publisher, router.FlowOrchestrator.StateStore)
	router.RegisterNodeWorker(echoNode)
	
	// Create a delay node using the builder
	delayNode := createDelayNode(router.FlowOrchestrator.Publisher, router.FlowOrchestrator.StateStore)
	router.RegisterNodeWorker(delayNode)
	
	fmt.Println("Registered example simple nodes: echo, delay")
}