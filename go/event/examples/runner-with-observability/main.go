package main

import (
	"fmt"
	"log"
	"time"

	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/The-Pocket/PocketFlow/go/event/observability"
)

func main() {
	fmt.Println("PocketFlow Runner with Observability")
	fmt.Println("====================================")

	// Create observability manager
	obsManager := observability.NewObservabilityManager()

	// Add stdout observer with colorized output
	stdoutObserver := observability.NewStdoutObserverWithOptions("console", true, false)
	err := obsManager.AddObserver(stdoutObserver)
	if err != nil {
		log.Fatalf("Failed to add stdout observer: %v", err)
	}

	// Add flow tracer for detailed flow tracking
	flowTracer := observability.NewStdoutFlowTracer("flow_monitor")
	err = obsManager.AddObserver(flowTracer)
	if err != nil {
		log.Fatalf("Failed to add flow tracer: %v", err)
	}

	fmt.Printf("✓ Added %d observers\n", len(obsManager.ListObservers()))

	// Example of how to integrate with the actual runner
	// This demonstrates the pattern for wrapping the publisher

	// Note: In a real implementation, you would get the publisher from the runner
	// publisher := runner.Publisher()
	// observablePublisher := observability.NewObservableEventPublisher(publisher, obsManager)

	// For demo purposes, we'll simulate with a mock publisher
	mockPublisher := &MockPublisher{}
	observablePublisher := observability.NewObservableEventPublisher(mockPublisher, obsManager)

	fmt.Println("\n🚀 Starting simulated flow execution with observability...")

	// Simulate publishing events through the observable publisher
	// These would normally be published by the flow and node workers

	testFlowExecution(observablePublisher)

	fmt.Println("\n✅ Flow execution completed!")

	// Show how to query flow status
	fmt.Println("\n📊 Flow Status Summary:")
	fmt.Println("----------------------")
	
	// In a real scenario, you would track flow IDs and query their status
	// For now, we'll just show that the functionality is available
	fmt.Println("Flow tracer is tracking flow executions and can provide:")
	fmt.Println("  • Real-time status updates")
	fmt.Println("  • Execution duration tracking")
	fmt.Println("  • Node execution counts")
	fmt.Println("  • Error details and failure points")
}

// MockPublisher implements core.EventPublisher for demonstration
type MockPublisher struct{}

func (p *MockPublisher) Publish(topic string, event interface{}) error {
	// In a real implementation, this would publish to the actual message bus
	fmt.Printf("📤 Published to topic '%s'\n", topic)
	return nil
}

func testFlowExecution(publisher *observability.ObservableEventPublisher) {
	// Simulate a typical flow execution sequence

	// 1. Flow start
	flowStartMsg := &core.FlowStartRequestedMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     "flow.start.requested",
			FlowExecutionID: "demo-flow-001",
			Timestamp:       time.Now(),
		},
		FlowType:         "qa_demo",
		FlowDefinitionID: "qa_demo_def",
		InitialSharedData: map[string]interface{}{
			"user_input": "Tell me about event-driven architectures",
		},
	}

	publisher.PublishWithObservability("flow.qa_demo", flowStartMsg)

	// 2. First node execution
	nodeExecMsg := &core.ExecRequestedMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     "node.exec.requested",
			FlowExecutionID: "demo-flow-001",
			NodeExecutionID: "node-exec-001",
			Timestamp:       time.Now(),
		},
		NodeType: "question",
		NodeID:   "question_processor",
		Params: core.NodeParams{
			"prompt": "Process user question",
		},
	}

	publisher.PublishWithObservability("node.question", nodeExecMsg)

	// 3. First node completion
	nodeCompletedMsg := &core.NodeCompletedMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     "node.completed",
			FlowExecutionID: "demo-flow-001",
			NodeExecutionID: "node-exec-001",
			Timestamp:       time.Now(),
		},
		NodeType: "question",
		NodeID:   "question_processor",
		Action:   "proceed_to_answer",
		Result:   "Processed question: Tell me about event-driven architectures",
	}

	publisher.PublishWithObservability("node.completed", nodeCompletedMsg)

	// 4. Second node execution
	answerExecMsg := &core.ExecRequestedMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     "node.exec.requested",
			FlowExecutionID: "demo-flow-001",
			NodeExecutionID: "node-exec-002",
			Timestamp:       time.Now(),
		},
		NodeType: "llm_answer",
		NodeID:   "answer_generator",
		Params: core.NodeParams{
			"model": "gpt-4",
			"max_tokens": 500,
		},
	}

	publisher.PublishWithObservability("node.llm_answer", answerExecMsg)

	// 5. Second node completion
	answerCompletedMsg := &core.NodeCompletedMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     "node.completed",
			FlowExecutionID: "demo-flow-001",
			NodeExecutionID: "node-exec-002",
			Timestamp:       time.Now(),
		},
		NodeType: "llm_answer",
		NodeID:   "answer_generator",
		Action:   "complete",
		Result:   "Event-driven architectures are design patterns where components communicate through events...",
	}

	publisher.PublishWithObservability("node.completed", answerCompletedMsg)

	// 6. Flow completion
	flowCompletedMsg := &core.FlowCompletedMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     "flow.completed",
			FlowExecutionID: "demo-flow-001",
			Timestamp:       time.Now(),
		},
		FlowType:    "qa_demo",
		FinalAction: "complete",
		FinalResult: "Event-driven architectures are design patterns where components communicate through events...",
		ExecutionMs: 1500, // 1.5 seconds
	}

	publisher.PublishWithObservability("flow.completed", flowCompletedMsg)
}