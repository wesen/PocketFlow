package main

import (
	"fmt"
	"time"

	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/The-Pocket/PocketFlow/go/event/observability"
	"github.com/google/uuid"
)

func main() {
	fmt.Println("PocketFlow Observability Demo")
	fmt.Println("=============================")

	// Create observability manager
	obsManager := observability.NewObservabilityManager()

	// Create stdout observer with colors and verbose output
	stdoutObserver := observability.NewStdoutObserverWithOptions("stdout", true, true)
	obsManager.AddObserver(stdoutObserver)

	// Create flow tracer
	flowTracer := observability.NewStdoutFlowTracer("flow_tracer")
	obsManager.AddObserver(flowTracer)

	fmt.Println("\nDemonstrating observability events:")
	fmt.Println("-----------------------------------")

	// Simulate flow execution events
	flowExecutionID := uuid.New().String()
	nodeExecutionID := uuid.New().String()

	// Flow started event
	flowStartedMsg := &core.FlowStartRequestedMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     "flow.start.requested",
			FlowExecutionID: flowExecutionID,
			Timestamp:       time.Now(),
		},
		FlowType:         "qa_chain",
		FlowDefinitionID: "qa_def_001",
		InitialSharedData: map[string]interface{}{
			"user_question": "How does PocketFlow work?",
		},
	}

	event1, _ := obsManager.CreateEventFromMessage(flowStartedMsg)
	obsManager.NotifyObservers(event1)

	// Simulate some delay
	time.Sleep(100 * time.Millisecond)

	// Node started event
	nodeStartedMsg := &core.ExecRequestedMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     "node.exec.requested",
			FlowExecutionID: flowExecutionID,
			NodeExecutionID: nodeExecutionID,
			Timestamp:       time.Now(),
		},
		NodeType: "question",
		NodeID:   "question_node_001",
		Params: core.NodeParams{
			"prompt": "What is your question?",
		},
	}

	event2, _ := obsManager.CreateEventFromMessage(nodeStartedMsg)
	obsManager.NotifyObservers(event2)

	// Simulate processing delay
	time.Sleep(200 * time.Millisecond)

	// Node completed event
	nodeCompletedMsg := &core.NodeCompletedMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     "node.completed",
			FlowExecutionID: flowExecutionID,
			NodeExecutionID: nodeExecutionID,
			Timestamp:       time.Now(),
		},
		NodeType: "question",
		NodeID:   "question_node_001",
		Action:   "default",
		Result:   "How does PocketFlow work?",
	}

	event3, _ := obsManager.CreateEventFromMessage(nodeCompletedMsg)
	obsManager.NotifyObservers(event3)

	// Simulate another node
	time.Sleep(100 * time.Millisecond)
	answerNodeExecutionID := uuid.New().String()

	answerStartedMsg := &core.ExecRequestedMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     "node.exec.requested",
			FlowExecutionID: flowExecutionID,
			NodeExecutionID: answerNodeExecutionID,
			Timestamp:       time.Now(),
		},
		NodeType: "answer",
		NodeID:   "answer_node_001",
		Params: core.NodeParams{
			"llm_client": "mock",
		},
	}

	event4, _ := obsManager.CreateEventFromMessage(answerStartedMsg)
	obsManager.NotifyObservers(event4)

	time.Sleep(300 * time.Millisecond)

	answerCompletedMsg := &core.NodeCompletedMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     "node.completed",
			FlowExecutionID: flowExecutionID,
			NodeExecutionID: answerNodeExecutionID,
			Timestamp:       time.Now(),
		},
		NodeType: "answer",
		NodeID:   "answer_node_001",
		Action:   "default",
		Result:   "PocketFlow is an event-driven framework for building LLM applications.",
	}

	event5, _ := obsManager.CreateEventFromMessage(answerCompletedMsg)
	obsManager.NotifyObservers(event5)

	// Flow completed event
	time.Sleep(50 * time.Millisecond)

	flowCompletedMsg := &core.FlowCompletedMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     "flow.completed",
			FlowExecutionID: flowExecutionID,
			Timestamp:       time.Now(),
		},
		FlowType:    "qa_chain",
		FinalAction: "default",
		FinalResult: "PocketFlow is an event-driven framework for building LLM applications.",
		ExecutionMs: 650,
	}

	event6, _ := obsManager.CreateEventFromMessage(flowCompletedMsg)
	obsManager.NotifyObservers(event6)

	fmt.Println("\n-----------------------------------")

	// Test flow status
	if flowStatus, err := flowTracer.GetFlowStatus(flowExecutionID); err == nil {
		fmt.Printf("\nFlow Status for %s:\n", flowExecutionID[:8])
		fmt.Printf("  Type: %s\n", flowStatus.FlowType)
		fmt.Printf("  Status: %s\n", flowStatus.Status)
		fmt.Printf("  Duration: %v\n", flowStatus.Duration)
		fmt.Printf("  Nodes Executed: %d\n", flowStatus.NodesExecuted)
	}

	// Demonstrate error scenario
	fmt.Println("\nDemonstrating error scenario:")
	fmt.Println("-----------------------------")

	errorFlowID := uuid.New().String()
	errorNodeID := uuid.New().String()

	// Start error flow
	errorFlowStartedMsg := &core.FlowStartRequestedMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     "flow.start.requested",
			FlowExecutionID: errorFlowID,
			Timestamp:       time.Now(),
		},
		FlowType:         "error_demo",
		FlowDefinitionID: "error_def_001",
		InitialSharedData: map[string]interface{}{
			"input": "test",
		},
	}

	errorEvent1, _ := obsManager.CreateEventFromMessage(errorFlowStartedMsg)
	obsManager.NotifyObservers(errorEvent1)

	time.Sleep(100 * time.Millisecond)

	// Node failure
	nodeFailedMsg := &core.ExecFailedMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     "node.exec.failed",
			FlowExecutionID: errorFlowID,
			NodeExecutionID: errorNodeID,
			Timestamp:       time.Now(),
		},
		NodeType:     "error_prone",
		NodeID:       "error_node_001",
		ErrorMessage: "Connection timeout",
		ErrorDetails: "Failed to connect to external service after 3 attempts",
		RetryCount:   2,
		WillRetry:    false,
	}

	errorEvent2, _ := obsManager.CreateEventFromMessage(nodeFailedMsg)
	obsManager.NotifyObservers(errorEvent2)

	time.Sleep(50 * time.Millisecond)

	// Flow failure
	flowFailedMsg := &core.FlowFailedMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     "flow.failed",
			FlowExecutionID: errorFlowID,
			Timestamp:       time.Now(),
		},
		FlowType:     "error_demo",
		ErrorMessage: "Node execution failed",
		ErrorDetails: "Connection timeout in error_node_001",
		FailedNodeID: "error_node_001",
	}

	errorEvent3, _ := obsManager.CreateEventFromMessage(flowFailedMsg)
	obsManager.NotifyObservers(errorEvent3)

	fmt.Println("\n-----------------------------------")

	// Demonstrate observer management
	fmt.Println("\nDemonstrating observer management:")
	fmt.Println("----------------------------------")

	// List observers
	observers := obsManager.ListObservers()
	fmt.Printf("Active observers: %d\n", len(observers))
	for _, obs := range observers {
		fmt.Printf("  - %s (enabled: %v)\n", obs.GetName(), obs.IsEnabled())
	}

	// Disable stdout observer temporarily
	fmt.Println("\nDisabling stdout observer...")
	obsManager.DisableObserver("stdout")

	// Send an event (should only show in flow tracer)
	progressMsg := &core.ProgressUpdateMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     "progress.update",
			FlowExecutionID: flowExecutionID,
			Timestamp:       time.Now(),
		},
		Status:   "processing",
		Progress: 0.75,
		Message:  "Processing data...",
	}

	progressEvent, _ := obsManager.CreateEventFromMessage(progressMsg)
	obsManager.NotifyObservers(progressEvent)

	// Re-enable stdout observer
	fmt.Println("Re-enabling stdout observer...")
	obsManager.EnableObserver("stdout")

	// Send another event (should show in both observers)
	finalProgressMsg := &core.ProgressUpdateMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     "progress.update",
			FlowExecutionID: flowExecutionID,
			Timestamp:       time.Now(),
		},
		Status:   "completed",
		Progress: 1.0,
		Message:  "All done!",
	}

	finalProgressEvent, _ := obsManager.CreateEventFromMessage(finalProgressMsg)
	obsManager.NotifyObservers(finalProgressEvent)

	fmt.Println("\nDemo completed!")
}