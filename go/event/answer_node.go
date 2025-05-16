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
	
	// Publish prep completed event
	w.Publisher.Publish("node.answer.prep.completed", NodePrepCompleted{
		BaseEvent: BaseEvent{
			EventID:         generateUUID(),
			FlowExecutionID: event.FlowExecutionID,
			NodeExecutionID: event.NodeExecutionID,
			NodeType:        event.NodeType,
			Timestamp:       time.Now(),
			CorrelationID:   event.CorrelationID,
		},
		PrepResultRef: prepResultRef,
	})
	
	// Automatically proceed to exec phase
	w.Publisher.Publish("node.answer.exec.requested", NodeExecRequested{
		BaseEvent: BaseEvent{
			EventID:         generateUUID(),
			FlowExecutionID: event.FlowExecutionID,
			NodeExecutionID: event.NodeExecutionID,
			NodeType:        event.NodeType,
			Timestamp:       time.Now(),
			CorrelationID:   event.CorrelationID,
		},
		PrepResultRef: prepResultRef,
	})
}

// Handle exec request
func (w *AnswerNodeWorker) HandleExecRequested(event NodeExecRequested) {
	// Get prep result (the user's answer)
	userAnswerObj, err := w.StateStore.GetNodeResult(event.PrepResultRef)
	if err != nil {
		w.handleError(event, "Failed to get prep result", err)
		return
	}
	
	userAnswer, ok := userAnswerObj.(string)
	if !ok {
		w.handleError(event, "Invalid user answer format", fmt.Errorf("expected string, got %T", userAnswerObj))
		return
	}
	
	// Create a prompt for the LLM
	prompt := fmt.Sprintf("Given the user's response: '%s', provide a detailed explanation.", userAnswer)
	
	// Call the LLM
	llmResponse, err := w.LLMClient.Call(prompt)
	if err != nil {
		w.handleError(event, "Failed to call LLM", err)
		return
	}
	
	// Store exec result
	execResultRef, err := w.StateStore.StoreNodeResult(
		event.NodeExecutionID,
		"exec",
		llmResponse,
	)
	if err != nil {
		w.handleError(event, "Failed to store exec result", err)
		return
	}
	
	// Publish exec completed event
	w.Publisher.Publish("node.answer.exec.completed", NodeExecCompleted{
		BaseEvent: BaseEvent{
			EventID:         generateUUID(),
			FlowExecutionID: event.FlowExecutionID,
			NodeExecutionID: event.NodeExecutionID,
			NodeType:        event.NodeType,
			Timestamp:       time.Now(),
			CorrelationID:   event.CorrelationID,
		},
		ExecResultRef: execResultRef,
		RetryCount:    0,
	})
	
	// Automatically proceed to post phase
	w.Publisher.Publish("node.answer.post.requested", NodePostRequested{
		BaseEvent: BaseEvent{
			EventID:         generateUUID(),
			FlowExecutionID: event.FlowExecutionID,
			NodeExecutionID: event.NodeExecutionID,
			NodeType:        event.NodeType,
			Timestamp:       time.Now(),
			CorrelationID:   event.CorrelationID,
		},
		PrepResultRef: event.PrepResultRef,
		ExecResultRef: execResultRef,
	})
}

// Handle post request
func (w *AnswerNodeWorker) HandlePostRequested(event NodePostRequested) {
	// Get shared data
	sharedData, err := w.StateStore.GetSharedData(event.FlowExecutionID)
	if err != nil {
		w.handleError(event, "Failed to get shared data", err)
		return
	}
	
	// Get llm response from exec result
	llmResponseObj, err := w.StateStore.GetNodeResult(event.ExecResultRef)
	if err != nil {
		w.handleError(event, "Failed to get exec result", err)
		return
	}
	
	// Update shared data
	sharedData["llm_response"] = llmResponseObj
	
	// Store updated shared data
	err = w.StateStore.StoreSharedData(event.FlowExecutionID, sharedData)
	if err != nil {
		w.handleError(event, "Failed to store shared data", err)
		return
	}
	
	// Determine action (always "default" in this simple example)
	action := "default"
	
	// Publish post completed event
	w.Publisher.Publish("node.answer.post.completed", NodePostCompleted{
		BaseEvent: BaseEvent{
			EventID:         generateUUID(),
			FlowExecutionID: event.FlowExecutionID,
			NodeExecutionID: event.NodeExecutionID,
			NodeType:        event.NodeType,
			Timestamp:       time.Now(),
			CorrelationID:   event.CorrelationID,
		},
		Action: action,
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
	
	// Extract BaseEvent fields based on event type
	var baseEvent BaseEvent
	switch e := event.(type) {
	case NodePrepRequested:
		baseEvent = e.BaseEvent
	case NodeExecRequested:
		baseEvent = e.BaseEvent
	case NodePostRequested:
		baseEvent = e.BaseEvent
	case BaseEvent:
		baseEvent = e
	default:
		// If we can't determine the event type, create a minimal base event
		baseEvent = BaseEvent{
			EventID: generateUUID(),
			Timestamp: time.Now(),
		}
	}
	
	w.Publisher.Publish("node.answer.exec.failed", NodeExecFailed{
		BaseEvent: BaseEvent{
			EventID:         generateUUID(),
			FlowExecutionID: baseEvent.FlowExecutionID,
			NodeExecutionID: baseEvent.NodeExecutionID,
			NodeType:        baseEvent.NodeType,
			Timestamp:       time.Now(),
			CorrelationID:   baseEvent.CorrelationID,
		},
		ErrorMessage: fullMessage,
		RetryCount:   0,
		WillRetry:    false,
	})
}