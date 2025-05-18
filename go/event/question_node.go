package event

import (
	"fmt"
	"time"
)

// QuestionNodeWorker handles user interaction
type QuestionNodeWorker struct {
	Publisher  EventPublisher
	StateStore StateStore
	Question   string
}

func NewQuestionNodeWorker(publisher EventPublisher, stateStore StateStore, question string) *QuestionNodeWorker {
	return &QuestionNodeWorker{
		Publisher:  publisher,
		StateStore: stateStore,
		Question:   question,
	}
}

// NodeType returns the type of node this worker handles
func (w *QuestionNodeWorker) NodeType() string {
	return "question"
}

// SupportedMessageTypes returns the list of message types this worker can handle
func (w *QuestionNodeWorker) SupportedMessageTypes() []string {
	return []string{MessageTypeExecRequested}
}

// HandleMessage handles the incoming messages
func (w *QuestionNodeWorker) HandleMessage(msgObj interface{}) error {
	// Implementation needed for new message pattern
	return fmt.Errorf("not implemented")
}

// Handle prep request
func (w *QuestionNodeWorker) HandlePrepRequested(event NodePrepRequested) {
	// In a real implementation, this would get parameters from the node definition
	// and prepare for user interaction
	question := w.Question
	if question == "" {
		question = "What is your question?"
	}
	
	// For simplicity, we'll simulate user interaction with a fixed answer
	userAnswer := "How does PocketFlow work?"
	
	// Publish node completed event with the user's answer
	w.Publisher.Publish("node.completed", NodeCompletedMessage{
		BaseMessage: BaseMessage{
			MessageType:     MessageTypeNodeCompleted,
			FlowExecutionID: event.FlowExecutionID,
			NodeExecutionID: event.NodeExecutionID,
			Timestamp:       time.Now(),
		},
		NodeType: "question",
		NodeID:   event.NodeExecutionID,
		Action:   "default",
		Result:   userAnswer,
	})
	
	// Update shared data with the user's answer
	if err := w.StateStore.UpdateSharedData(event.FlowExecutionID, "user_answer", userAnswer); err != nil {
		fmt.Printf("Error updating shared data: %v\n", err)
	}
}

// HandleExecRequested implements the NodeWorker interface
func (w *QuestionNodeWorker) HandleExecRequested(event NodeExecRequested) {
	// For the simplified version, just treat this the same as prep
	w.HandlePrepRequested(event)
}

// HandlePostRequested implements the NodeWorker interface
func (w *QuestionNodeWorker) HandlePostRequested(event NodePostRequested) {
	// For the simplified version, just publish completion with default action
	w.Publisher.Publish("node.completed", NodeCompletedMessage{
		BaseMessage: BaseMessage{
			MessageType:     MessageTypeNodeCompleted,
			FlowExecutionID: event.FlowExecutionID,
			NodeExecutionID: event.NodeExecutionID,
			Timestamp:       time.Now(),
		},
		NodeType: "question",
		NodeID:   event.NodeExecutionID,
		Action:   "default",
		Result:   nil,
	})
}

// HandleExecFailed implements the NodeWorker interface
func (w *QuestionNodeWorker) HandleExecFailed(event NodeExecFailed) {
	// Just log the error for now
	fmt.Printf("Question node execution failed: %s\n", event.ErrorMessage)
}