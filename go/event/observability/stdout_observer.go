package observability

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/The-Pocket/PocketFlow/go/event/observability/events"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/rs/zerolog/log"
)

// StdoutObserver implements Observer and outputs events to stdout
type StdoutObserver struct {
	name      string
	enabled   bool
	colorized bool
	verbose   bool
}

var _ Observer = (*StdoutObserver)(nil)

// NewStdoutObserver creates a new stdout observer
func NewStdoutObserver(name string) *StdoutObserver {
	return &StdoutObserver{
		name:      name,
		enabled:   true,
		colorized: true, // Default to colorized output
		verbose:   false,
	}
}

// NewStdoutObserverWithOptions creates a new stdout observer with custom options
func NewStdoutObserverWithOptions(name string, colorized, verbose bool) *StdoutObserver {
	return &StdoutObserver{
		name:      name,
		enabled:   true,
		colorized: colorized,
		verbose:   verbose,
	}
}

var _ Observer = (*StdoutObserver)(nil)

// GetName returns the observer's identifier
func (o *StdoutObserver) GetName() string {
	return o.name
}

// IsEnabled returns whether this observer is active
func (o *StdoutObserver) IsEnabled() bool {
	return o.enabled
}

// SetEnabled enables or disables this observer
func (o *StdoutObserver) SetEnabled(enabled bool) {
	o.enabled = enabled
}

// SetColorized enables or disables colored output
func (o *StdoutObserver) SetColorized(colorized bool) {
	o.colorized = colorized
}

// SetVerbose enables or disables verbose output
func (o *StdoutObserver) SetVerbose(verbose bool) {
	o.verbose = verbose
}

// HandleMessage processes a message from a subscribed topic
func (o *StdoutObserver) HandleMessage(msg *message.Message) error {
	if !o.enabled {
		return nil
	}

	// Parse the message based on topic and message type
	event, err := o.parseMessage(msg.Payload)
	if err != nil {
		log.Debug().Err(err).Msg("Failed to parse message for observability")
		return nil // Don't fail on parse errors
	}

	if event != nil {
		output := o.formatEvent(event)
		_, err := fmt.Fprintln(os.Stdout, output)
		return err
	}

	return nil
}

// parseMessage converts a raw message to an ObservableEvent
func (o *StdoutObserver) parseMessage(message []byte) (ObservableEvent, error) {
	// Try to parse as a base message first to get the message type
	var baseMsg core.BaseMessage
	if err := json.Unmarshal(message, &baseMsg); err != nil {
		return nil, fmt.Errorf("failed to parse base message: %w", err)
	}

	// Create the appropriate observable event based on message type
	switch baseMsg.MessageType {
	case core.MessageTypeFlowStartRequested:
		var msg core.FlowStartRequestedMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			return nil, err
		}
		return events.CreateFlowStartedEvent(&msg), nil

	case core.MessageTypeFlowCompleted:
		var msg core.FlowCompletedMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			return nil, err
		}
		return events.CreateFlowCompletedEvent(&msg), nil

	case core.MessageTypeFlowFailed:
		var msg core.FlowFailedMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			return nil, err
		}
		return events.CreateFlowFailedEvent(&msg), nil

	case core.MessageTypeExecRequested:
		var msg core.ExecRequestedMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			return nil, err
		}
		return events.CreateNodeStartedEvent(&msg), nil

	case core.MessageTypeNodeCompleted:
		var msg core.NodeCompletedMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			return nil, err
		}
		return events.CreateNodeCompletedEvent(&msg), nil

	case core.MessageTypeExecFailed:
		var msg core.ExecFailedMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			return nil, err
		}
		return events.CreateNodeFailedEvent(&msg), nil

	case core.MessageTypeProgressUpdate:
		var msg core.ProgressUpdateMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			return nil, err
		}
		return events.CreateProgressUpdateEvent(&msg), nil

	default:
		// Unknown message type, skip silently
		return nil, nil
	}
}

// formatEvent formats an observable event for display
func (o *StdoutObserver) formatEvent(event ObservableEvent) string {
	timestamp := event.GetTimestamp().Format("15:04:05.000")
	eventType := event.GetEventType()
	flowID := o.truncateID(event.GetFlowExecutionID())
	nodeID := o.truncateID(event.GetNodeExecutionID())

	var output strings.Builder

	// Add timestamp
	if o.colorized {
		output.WriteString(o.colorize("90", timestamp)) // Gray
	} else {
		output.WriteString(timestamp)
	}
	output.WriteString(" ")

	// Add event type with color
	output.WriteString(o.formatEventType(eventType))
	output.WriteString(" ")

	// Add flow ID
	if flowID != "" {
		if o.colorized {
			output.WriteString(o.colorize("36", fmt.Sprintf("[%s]", flowID))) // Cyan
		} else {
			output.WriteString(fmt.Sprintf("[%s]", flowID))
		}
		output.WriteString(" ")
	}

	// Add node ID if present
	if nodeID != "" {
		if o.colorized {
			output.WriteString(o.colorize("35", fmt.Sprintf("(%s)", nodeID))) // Magenta
		} else {
			output.WriteString(fmt.Sprintf("(%s)", nodeID))
		}
		output.WriteString(" ")
	}

	// Add event-specific details
	output.WriteString(o.formatEventDetails(event))

	return output.String()
}

// formatEventType formats the event type with appropriate colors
func (o *StdoutObserver) formatEventType(eventType string) string {
	if !o.colorized {
		return fmt.Sprintf("%-15s", eventType)
	}

	var color string
	switch eventType {
	case events.EventTypeFlowStarted:
		color = "32" // Green
	case events.EventTypeFlowCompleted:
		color = "32" // Green
	case events.EventTypeFlowFailed:
		color = "31" // Red
	case events.EventTypeNodeStarted:
		color = "34" // Blue
	case events.EventTypeNodeCompleted:
		color = "34" // Blue
	case events.EventTypeNodeFailed:
		color = "31" // Red
	case events.EventTypeProgressUpdate:
		color = "33" // Yellow
	default:
		color = "37" // White
	}

	return o.colorize(color, fmt.Sprintf("%-15s", eventType))
}

// formatEventDetails formats event-specific details
func (o *StdoutObserver) formatEventDetails(event ObservableEvent) string {
	switch e := event.(type) {
	case *events.FlowStartedEvent:
		return o.formatFlowStarted(e)
	case *events.FlowCompletedEvent:
		return o.formatFlowCompleted(e)
	case *events.FlowFailedEvent:
		return o.formatFlowFailed(e)
	case *events.NodeStartedEvent:
		return o.formatNodeStarted(e)
	case *events.NodeCompletedEvent:
		return o.formatNodeCompleted(e)
	case *events.NodeFailedEvent:
		return o.formatNodeFailed(e)
	case *events.ProgressUpdateEvent:
		return o.formatProgressUpdate(e)
	default:
		return "Unknown event type"
	}
}

// formatFlowStarted formats a flow started event
func (o *StdoutObserver) formatFlowStarted(event *events.FlowStartedEvent) string {
	output := fmt.Sprintf("Flow '%s' started", event.FlowType)
	if event.FlowDefinitionID != "" {
		output += fmt.Sprintf(" (def: %s)", o.truncateID(event.FlowDefinitionID))
	}

	if o.verbose && len(event.InitialData) > 0 {
		output += fmt.Sprintf(" with data: %v", event.InitialData)
	}

	return output
}

// formatFlowCompleted formats a flow completed event
func (o *StdoutObserver) formatFlowCompleted(event *events.FlowCompletedEvent) string {
	output := fmt.Sprintf("Flow '%s' completed", event.FlowType)
	if event.Duration > 0 {
		output += fmt.Sprintf(" in %v", event.Duration.Round(time.Millisecond))
	}
	if event.NodesExecuted > 0 {
		output += fmt.Sprintf(" (%d nodes)", event.NodesExecuted)
	}
	if event.FinalAction != "" {
		output += fmt.Sprintf(" with action '%s'", event.FinalAction)
	}

	if o.verbose && event.FinalResult != nil {
		output += fmt.Sprintf(" result: %v", event.FinalResult)
	}

	return output
}

// formatFlowFailed formats a flow failed event
func (o *StdoutObserver) formatFlowFailed(event *events.FlowFailedEvent) string {
	output := fmt.Sprintf("Flow '%s' failed: %s", event.FlowType, event.ErrorMessage)
	if event.FailedNodeID != "" {
		output += fmt.Sprintf(" (node: %s)", o.truncateID(event.FailedNodeID))
	}
	if event.Duration > 0 {
		output += fmt.Sprintf(" after %v", event.Duration.Round(time.Millisecond))
	}

	if o.verbose && event.ErrorDetails != "" {
		output += fmt.Sprintf(" details: %s", event.ErrorDetails)
	}

	return output
}

// formatNodeStarted formats a node started event
func (o *StdoutObserver) formatNodeStarted(event *events.NodeStartedEvent) string {
	output := fmt.Sprintf("Node '%s' (%s) started", event.NodeID, event.NodeType)

	if o.verbose && len(event.Params) > 0 {
		output += fmt.Sprintf(" with params: %v", event.Params)
	}

	return output
}

// formatNodeCompleted formats a node completed event
func (o *StdoutObserver) formatNodeCompleted(event *events.NodeCompletedEvent) string {
	output := fmt.Sprintf("Node '%s' (%s) completed", event.NodeID, event.NodeType)
	if event.Duration > 0 {
		output += fmt.Sprintf(" in %v", event.Duration.Round(time.Millisecond))
	}
	if event.Action != "" {
		output += fmt.Sprintf(" with action '%s'", event.Action)
	}

	if o.verbose && event.Result != nil {
		output += fmt.Sprintf(" result: %v", event.Result)
	}

	return output
}

// formatNodeFailed formats a node failed event
func (o *StdoutObserver) formatNodeFailed(event *events.NodeFailedEvent) string {
	output := fmt.Sprintf("Node '%s' (%s) failed: %s", event.NodeID, event.NodeType, event.ErrorMessage)
	if event.RetryCount > 0 {
		output += fmt.Sprintf(" (retry %d)", event.RetryCount)
	}
	if event.WillRetry {
		output += " - will retry"
	}
	if event.Duration > 0 {
		output += fmt.Sprintf(" after %v", event.Duration.Round(time.Millisecond))
	}

	if o.verbose && event.ErrorDetails != "" {
		output += fmt.Sprintf(" details: %s", event.ErrorDetails)
	}

	return output
}

// formatProgressUpdate formats a progress update event
func (o *StdoutObserver) formatProgressUpdate(event *events.ProgressUpdateEvent) string {
	output := fmt.Sprintf("Progress: %s (%.1f%%)", event.Status, event.Progress*100)
	if event.Message != "" {
		output += fmt.Sprintf(" - %s", event.Message)
	}
	return output
}

// colorize applies ANSI color codes to text
func (o *StdoutObserver) colorize(colorCode, text string) string {
	if !o.colorized {
		return text
	}
	return fmt.Sprintf("\033[%sm%s\033[0m", colorCode, text)
}

// truncateID truncates long IDs for display
func (o *StdoutObserver) truncateID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

// Ensure StdoutObserver implements Observer interface
var _ Observer = (*StdoutObserver)(nil)

// StdoutFlowTracer extends StdoutObserver to implement FlowTracer
type StdoutFlowTracer struct {
	*StdoutObserver
	flowStatuses map[string]*FlowStatus
}

// NewStdoutFlowTracer creates a new stdout flow tracer
func NewStdoutFlowTracer(name string) *StdoutFlowTracer {
	return &StdoutFlowTracer{
		StdoutObserver: NewStdoutObserver(name),
		flowStatuses:   make(map[string]*FlowStatus),
	}
}

// OnFlowStarted handles flow start events
func (t *StdoutFlowTracer) OnFlowStarted(event events.FlowStartedEvent) error {
	// Track flow status
	t.flowStatuses[event.FlowExecutionID] = &FlowStatus{
		FlowExecutionID:  event.FlowExecutionID,
		FlowType:         event.FlowType,
		FlowDefinitionID: event.FlowDefinitionID,
		Status:           "running",
		StartTime:        event.Timestamp,
		NodesExecuted:    0,
	}

	// Log to stdout via the base observer
	if t.StdoutObserver.enabled {
		output := t.StdoutObserver.formatEvent(&event)
		_, err := fmt.Fprintln(os.Stdout, output)
		if err != nil {
			log.Error().Err(err).Msg("Failed to output flow started event")
		}
		return err
	}

	return nil
}

// OnFlowCompleted handles flow completion events
func (t *StdoutFlowTracer) OnFlowCompleted(event events.FlowCompletedEvent) error {
	// Update flow status
	if status, exists := t.flowStatuses[event.FlowExecutionID]; exists {
		status.Status = "completed"
		endTime := event.Timestamp
		status.EndTime = &endTime
		status.Duration = event.Duration
		status.NodesExecuted = event.NodesExecuted
		status.FinalAction = event.FinalAction
		status.FinalResult = event.FinalResult
	}

	// Log to stdout via the base observer
	if t.StdoutObserver.enabled {
		output := t.StdoutObserver.formatEvent(&event)
		_, err := fmt.Fprintln(os.Stdout, output)
		if err != nil {
			log.Error().Err(err).Msg("Failed to output flow completed event")
		}
		return err
	}

	return nil
}

// OnFlowFailed handles flow failure events
func (t *StdoutFlowTracer) OnFlowFailed(event events.FlowFailedEvent) error {
	// Update flow status
	if status, exists := t.flowStatuses[event.FlowExecutionID]; exists {
		status.Status = "failed"
		endTime := event.Timestamp
		status.EndTime = &endTime
		status.Duration = event.Duration
		status.ErrorMessage = event.ErrorMessage
	}

	// Log to stdout via the base observer
	if t.StdoutObserver.enabled {
		output := t.StdoutObserver.formatEvent(&event)
		_, err := fmt.Fprintln(os.Stdout, output)
		if err != nil {
			log.Error().Err(err).Msg("Failed to output flow failed event")
		}
		return err
	}

	return nil
}

// GetFlowStatus returns the current status of a flow
func (t *StdoutFlowTracer) GetFlowStatus(flowExecutionID string) (*FlowStatus, error) {
	status, exists := t.flowStatuses[flowExecutionID]
	if !exists {
		return nil, fmt.Errorf("flow status not found for execution ID: %s", flowExecutionID)
	}

	// Create a copy to avoid concurrent modifications
	statusCopy := *status
	return &statusCopy, nil
}

// Ensure StdoutFlowTracer implements FlowTracer interface
var _ FlowTracer = (*StdoutFlowTracer)(nil)
