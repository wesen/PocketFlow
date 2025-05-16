package event

import (
	"fmt"
	"time"
)

// QuestionNodeWorker handles the question node logic
type QuestionNodeWorker struct {
	Publisher  EventPublisher
	StateStore StateStore
	// Instead of getting user input in a non-interactive environment, 
	// we'll configure a predetermined question
	Question string
}

func NewQuestionNodeWorker(publisher EventPublisher, stateStore StateStore, question string) *QuestionNodeWorker {
	return &QuestionNodeWorker{
		Publisher:  publisher,
		StateStore: stateStore,
		Question:   question,
	}
}

// Handle prep request
func (w *QuestionNodeWorker) HandlePrepRequested(event NodePrepRequested) {
	// Get shared data (just to validate it exists)
	_, err := w.StateStore.GetSharedData(event.FlowExecutionID)
	if err != nil {
		w.handleError(event, "Failed to get shared data", err)
		return
	}
	
	// In a real application, we might prepare the question here
	// based on shared data or node params
	question := w.Question
	if questionParam, ok := event.NodeParams["question"].(string); ok && questionParam != "" {
		question = questionParam
	}
	
	// Store prep result
	prepResultRef, err := w.StateStore.StoreNodeResult(
		event.NodeExecutionID,
		"prep",
		question,
	)
	if err != nil {
		w.handleError(event, "Failed to store prep result", err)
		return
	}
	
	// Publish prep completed event
	w.Publisher.Publish("node.question.prep.completed", NodePrepCompleted{
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
	w.Publisher.Publish("node.question.exec.requested", NodeExecRequested{
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
func (w *QuestionNodeWorker) HandleExecRequested(event NodeExecRequested) {
	// Get prep result (the question)
	questionObj, err := w.StateStore.GetNodeResult(event.PrepResultRef)
	if err != nil {
		w.handleError(event, "Failed to get prep result", err)
		return
	}
	
	question, ok := questionObj.(string)
	if !ok {
		w.handleError(event, "Invalid question format", fmt.Errorf("expected string, got %T", questionObj))
		return
	}
	
	// In a real application, we would display the question and get user input
	// For this example, we'll just use a predefined answer
	answer := fmt.Sprintf("This is the answer to: %s", question)
	
	// Store exec result
	execResultRef, err := w.StateStore.StoreNodeResult(
		event.NodeExecutionID,
		"exec",
		answer,
	)
	if err != nil {
		w.handleError(event, "Failed to store exec result", err)
		return
	}
	
	// Publish exec completed event
	w.Publisher.Publish("node.question.exec.completed", NodeExecCompleted{
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
	w.Publisher.Publish("node.question.post.requested", NodePostRequested{
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
func (w *QuestionNodeWorker) HandlePostRequested(event NodePostRequested) {
	// Get shared data
	sharedData, err := w.StateStore.GetSharedData(event.FlowExecutionID)
	if err != nil {
		w.handleError(event, "Failed to get shared data", err)
		return
	}
	
	// Get prep and exec results
	questionObj, err := w.StateStore.GetNodeResult(event.PrepResultRef)
	if err != nil {
		w.handleError(event, "Failed to get prep result", err)
		return
	}
	
	answerObj, err := w.StateStore.GetNodeResult(event.ExecResultRef)
	if err != nil {
		w.handleError(event, "Failed to get exec result", err)
		return
	}
	
	// Update shared data
	sharedData["question"] = questionObj
	sharedData["user_answer"] = answerObj
	
	// Store updated shared data
	err = w.StateStore.StoreSharedData(event.FlowExecutionID, sharedData)
	if err != nil {
		w.handleError(event, "Failed to store shared data", err)
		return
	}
	
	// Determine action (always "default" in this simple example)
	action := "default"
	
	// Publish post completed event
	w.Publisher.Publish("node.question.post.completed", NodePostCompleted{
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
func (w *QuestionNodeWorker) HandleExecFailed(event NodeExecFailed) {
	// In a real implementation, we might implement retry logic here
	// For simplicity, we'll just log the error and not retry
	fmt.Printf("Question node execution failed: %s\n", event.ErrorMessage)
}

// Helper function to handle errors
func (w *QuestionNodeWorker) handleError(event interface{}, message string, err error) {
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
	
	w.Publisher.Publish("node.question.exec.failed", NodeExecFailed{
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