package observability

import (
	"strings"
	"testing"
	"time"

	"github.com/The-Pocket/PocketFlow/go/event/core"
)

func TestStdoutObserver(t *testing.T) {
	observer := NewStdoutObserver("test")

	// Test basic properties
	if observer.GetName() != "test" {
		t.Errorf("Expected name 'test', got '%s'", observer.GetName())
	}

	if !observer.IsEnabled() {
		t.Error("Expected observer to be enabled by default")
	}

	// Test enable/disable
	observer.SetEnabled(false)
	if observer.IsEnabled() {
		t.Error("Expected observer to be disabled")
	}

	observer.SetEnabled(true)
	if !observer.IsEnabled() {
		t.Error("Expected observer to be enabled")
	}
}

func TestStdoutObserverFormatting(t *testing.T) {
	observer := NewStdoutObserverWithOptions("test", false, false) // No colors, no verbose

	// Create a test event
	event := &FlowStartedEvent{
		BaseObservableEvent: BaseObservableEvent{
			EventType:       EventTypeFlowStarted,
			Timestamp:       time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
			FlowExecutionID: "test-flow-12345678",
		},
		FlowType:         "test_flow",
		FlowDefinitionID: "test_def_001",
	}

	formatted := observer.formatEvent(event)

	// Should contain timestamp, event type, and flow ID
	if !strings.Contains(formatted, "12:00:00.000") {
		t.Error("Expected formatted output to contain timestamp")
	}

	if !strings.Contains(formatted, "flow.started") {
		t.Error("Expected formatted output to contain event type")
	}

	if !strings.Contains(formatted, "[test-flo]") {
		t.Error("Expected formatted output to contain truncated flow ID")
	}

	if !strings.Contains(formatted, "test_flow") {
		t.Error("Expected formatted output to contain flow type")
	}
}

func TestStdoutFlowTracer(t *testing.T) {
	tracer := NewStdoutFlowTracer("test_tracer")

	flowID := "test-flow-12345678"

	// Test flow started
	startedEvent := FlowStartedEvent{
		BaseObservableEvent: BaseObservableEvent{
			EventType:       EventTypeFlowStarted,
			Timestamp:       time.Now(),
			FlowExecutionID: flowID,
		},
		FlowType:         "test_flow",
		FlowDefinitionID: "test_def_001",
	}

	err := tracer.OnFlowStarted(startedEvent)
	if err != nil {
		t.Errorf("Unexpected error in OnFlowStarted: %v", err)
	}

	// Check flow status
	status, err := tracer.GetFlowStatus(flowID)
	if err != nil {
		t.Errorf("Unexpected error getting flow status: %v", err)
	}

	if status.Status != "running" {
		t.Errorf("Expected status 'running', got '%s'", status.Status)
	}

	if status.FlowType != "test_flow" {
		t.Errorf("Expected flow type 'test_flow', got '%s'", status.FlowType)
	}

	// Test flow completed
	completedEvent := FlowCompletedEvent{
		BaseObservableEvent: BaseObservableEvent{
			EventType:       EventTypeFlowCompleted,
			Timestamp:       time.Now(),
			FlowExecutionID: flowID,
		},
		FlowType:      "test_flow",
		FinalAction:   "success",
		Duration:      500 * time.Millisecond,
		NodesExecuted: 3,
	}

	err = tracer.OnFlowCompleted(completedEvent)
	if err != nil {
		t.Errorf("Unexpected error in OnFlowCompleted: %v", err)
	}

	// Check updated status
	status, err = tracer.GetFlowStatus(flowID)
	if err != nil {
		t.Errorf("Unexpected error getting updated flow status: %v", err)
	}

	if status.Status != "completed" {
		t.Errorf("Expected status 'completed', got '%s'", status.Status)
	}

	if status.NodesExecuted != 3 {
		t.Errorf("Expected 3 nodes executed, got %d", status.NodesExecuted)
	}

	if status.FinalAction != "success" {
		t.Errorf("Expected final action 'success', got '%s'", status.FinalAction)
	}
}

func TestObservabilityManager(t *testing.T) {
	manager := NewObservabilityManager()

	// Test adding observers
	observer1 := NewStdoutObserver("obs1")
	observer2 := NewStdoutObserver("obs2")

	err := manager.AddObserver(observer1)
	if err != nil {
		t.Errorf("Unexpected error adding observer1: %v", err)
	}

	err = manager.AddObserver(observer2)
	if err != nil {
		t.Errorf("Unexpected error adding observer2: %v", err)
	}

	// Test duplicate name error
	observer3 := NewStdoutObserver("obs1") // Same name as observer1
	err = manager.AddObserver(observer3)
	if err == nil {
		t.Error("Expected error when adding observer with duplicate name")
	}

	// Test listing observers
	observers := manager.ListObservers()
	if len(observers) != 2 {
		t.Errorf("Expected 2 observers, got %d", len(observers))
	}

	// Test getting observer by name
	obs := manager.GetObserver("obs1")
	if obs == nil {
		t.Error("Expected to find observer 'obs1'")
	}

	if obs.GetName() != "obs1" {
		t.Errorf("Expected observer name 'obs1', got '%s'", obs.GetName())
	}

	// Test removing observer
	err = manager.RemoveObserver("obs1")
	if err != nil {
		t.Errorf("Unexpected error removing observer: %v", err)
	}

	obs = manager.GetObserver("obs1")
	if obs != nil {
		t.Error("Expected observer 'obs1' to be removed")
	}

	// Test removing non-existent observer
	err = manager.RemoveObserver("nonexistent")
	if err == nil {
		t.Error("Expected error when removing non-existent observer")
	}
}

func TestCreateEventFromMessage(t *testing.T) {
	manager := NewObservabilityManager()

	// Test FlowStartRequestedMessage
	flowMsg := &core.FlowStartRequestedMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     "flow.start.requested",
			FlowExecutionID: "test-flow",
			Timestamp:       time.Now(),
		},
		FlowType:         "test_flow",
		FlowDefinitionID: "test_def",
	}

	event, err := manager.CreateEventFromMessage(flowMsg)
	if err != nil {
		t.Errorf("Unexpected error creating event: %v", err)
	}

	flowStartedEvent, ok := event.(*FlowStartedEvent)
	if !ok {
		t.Error("Expected FlowStartedEvent")
	}

	if flowStartedEvent.FlowType != "test_flow" {
		t.Errorf("Expected flow type 'test_flow', got '%s'", flowStartedEvent.FlowType)
	}

	// Test unsupported message type
	unsupportedMsg := "not a message"
	_, err = manager.CreateEventFromMessage(unsupportedMsg)
	if err == nil {
		t.Error("Expected error for unsupported message type")
	}
}