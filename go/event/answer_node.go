package event

import (
	"fmt"
	"time"
)

// AnswerNodeWorker handles the answer node logic
type AnswerNodeWorker struct {
	Publisher  EventPublisher
	StateStore StateStore
	LLMClient  LLMClient
}

func NewAnswerNodeWorker(publisher EventPublisher, stateStore StateStore, llmClient LLMClient) *AnswerNodeWorker {
	return &AnswerNodeWorker{
		Publisher:  publisher,
		StateStore: stateStore,
		LLMClient:  llmClient,
	}
}

// NodeType returns the type of node this worker handles
func (w *AnswerNodeWorker) NodeType() string {
	return "answer"
}

// SupportedMessageTypes returns the list of message types this worker can handle
func (w *AnswerNodeWorker) SupportedMessageTypes() []string {
	return []string{MessageTypeExecRequested}
}

// HandleMessage handles the incoming messages
func (w *AnswerNodeWorker) HandleMessage(msgObj interface{}) error {
	// Implementation needed for new message pattern
	// This would extract the BaseMessage, check the message type,
	// and call the appropriate handler
	return fmt.Errorf("not implemented")
}

// Handle prep request
func (w *AnswerNodeWorker) HandlePrepRequested(event NodePrepRequested) {
	// Get shared data
	sharedData, err := w.StateStore.GetSharedData(event.FlowExecutionID)
	if err != nil {
		w.handleError(event, "Failed to get shared data", err)
		return
	}
	
	// Get the question from shared data
	userAnswer, ok := sharedData["user_answer"].(string)
	if !ok {
		w.handleError(event, "User answer not found in shared data", fmt.Errorf("user_answer not found"))
		return
	}
	
	// Store prep result
	prepResultRef, err := w.StateStore.StoreNodeResult(
		event.NodeExecutionID,
		"prep",
		userAnswer,
	)
	if err != nil {
		w.handleError(event, "Failed to store prep result", err)
		return
	}
	
	// Publish node completed event (simplified for the update)
	w.Publisher.Publish("node.completed", NodeCompletedMessage{
		BaseMessage: BaseMessage{
			MessageType:     MessageTypeNodeCompleted,
			FlowExecutionID: event.FlowExecutionID,
			NodeExecutionID: event.NodeExecutionID,
			Timestamp:       time.Now(),
		},
		NodeType: "answer",
		NodeID:   event.NodeExecutionID, // Using execution ID as node ID for simplicity
		Action:   "default",
		Result:   prepResultRef,
	})
}

// Handle exec request
func (w *AnswerNodeWorker) HandleExecRequested(event NodeExecRequested) {
	// This method should create a NodeCompletedMessage similar to above
	// Implementation simplified for the fix
	
	// Publish node completed message for the exec phase
	w.Publisher.Publish("node.completed", NodeCompletedMessage{
		BaseMessage: BaseMessage{
			MessageType:     MessageTypeNodeCompleted,
			FlowExecutionID: event.FlowExecutionID,
			NodeExecutionID: event.NodeExecutionID,
			Timestamp:       time.Now(),
		},
		NodeType: "answer",
		NodeID:   event.NodeExecutionID,
		Action:   "default",
		Result:   "LLM response would go here",
	})
}

// Handle post request
func (w *AnswerNodeWorker) HandlePostRequested(event NodePostRequested) {
	// This method should create a NodeCompletedMessage similar to above
	// Implementation simplified for the fix
	
	// Publish node completed message for the post phase
	w.Publisher.Publish("node.completed", NodeCompletedMessage{
		BaseMessage: BaseMessage{
			MessageType:     MessageTypeNodeCompleted,
			FlowExecutionID: event.FlowExecutionID,
			NodeExecutionID: event.NodeExecutionID,
			Timestamp:       time.Now(),
		},
		NodeType: "answer",
		NodeID:   event.NodeExecutionID,
		Action:   "default",
		Result:   "Post processing result",
	})
}

// Handle exec failures
func (w *AnswerNodeWorker) HandleExecFailed(event NodeExecFailed) {
	// In a real implementation, we might implement retry logic here
	// For simplicity, we'll just log the error and not retry
	fmt.Printf("Answer node execution failed: %s\n", event.ErrorMessage)
}

// Helper function to handle errors
func (w *AnswerNodeWorker) handleError(event interface{}, message string, err error) {
	fullMessage := fmt.Sprintf("%s: %v", message, err)
	fmt.Println(fullMessage)
	
	// Extract flow execution ID and node execution ID based on event type
	var flowExecID, nodeExecID string
	
	// Simple extraction for now - this would need to be improved
	flowExecID = ""
	nodeExecID = ""
	
	// Try to extract from different event types
	if prep, ok := event.(NodePrepRequested); ok {
		flowExecID = prep.FlowExecutionID
		nodeExecID = prep.NodeExecutionID
	} else if exec, ok := event.(NodeExecRequested); ok {
		flowExecID = exec.FlowExecutionID
		nodeExecID = exec.NodeExecutionID
	} else if post, ok := event.(NodePostRequested); ok {
		flowExecID = post.FlowExecutionID
		nodeExecID = post.NodeExecutionID
	}
	
	// Publish error message
	w.Publisher.Publish("node.exec.failed", ExecFailedMessage{
		BaseMessage: BaseMessage{
			MessageType:     MessageTypeExecFailed,
			FlowExecutionID: flowExecID,
			NodeExecutionID: nodeExecID,
			Timestamp:       time.Now(),
		},
		NodeType:     "answer",
		NodeID:       nodeExecID,
		ErrorMessage: fullMessage,
		RetryCount:   0,
		WillRetry:    false,
	})
}