package event

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
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
	// Extract the message from the message object
	msg, ok := msgObj.(*message.Message)
	if !ok {
		return fmt.Errorf("expected *message.Message but got %T", msgObj)
	}

	// Extract base message to determine type
	var base BaseMessage
	if err := json.Unmarshal(msg.Payload, &base); err != nil {
		return fmt.Errorf("failed to unmarshal base message: %v", err)
	}

	// Log incoming message
	fmt.Printf("Question node received message: %s\n", base.MessageType)

	switch base.MessageType {
	case MessageTypeExecRequested:
		var execReq ExecRequestedMessage
		if err := json.Unmarshal(msg.Payload, &execReq); err != nil {
			return fmt.Errorf("failed to unmarshal exec request: %v", err)
		}

		// Get question from params or use default
		question := "What would you like to know about PocketFlow?"
		if val, ok := execReq.Params["question"]; ok {
			if q, ok := val.(string); ok && q != "" {
				question = q
			}
		}

		// Simulate processing delay (1 second) 
		fmt.Printf("Question node asking: %s\n", question)
		time.Sleep(1 * time.Second)

		// Simulate user response
		userAnswer := "How does PocketFlow work?"
		fmt.Printf("User answered: %s\n", userAnswer)

		// Store in shared data
		if err := w.StateStore.UpdateSharedData(execReq.FlowExecutionID, "user_answer", userAnswer); err != nil {
			return fmt.Errorf("failed to update shared data: %v", err)
		}

		// Publish completion
		return w.Publisher.Publish("node.completed", NodeCompletedMessage{
			BaseMessage: BaseMessage{
				MessageType:     MessageTypeNodeCompleted,
				FlowExecutionID: execReq.FlowExecutionID,
				NodeExecutionID: execReq.NodeExecutionID,
				Timestamp:       time.Now(),
			},
			NodeType: "question",
			NodeID:   execReq.NodeID,
			Action:   "default",
			Result:   userAnswer,
		})

	default:
		return fmt.Errorf("unsupported message type: %s", base.MessageType)
	}
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