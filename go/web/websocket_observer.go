package web

import (
	"encoding/json"
	"time"

	"github.com/The-Pocket/PocketFlow/go/event/observability"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/rs/zerolog/log"
)

// WebSocketObserver implements the Observer interface to broadcast events via WebSocket
type WebSocketObserver struct {
	name    string
	enabled bool
	server  *Server
}

var _ observability.Observer = &WebSocketObserver{}

// NewWebSocketObserver creates a new WebSocket observer
func NewWebSocketObserver(server *Server) observability.Observer {
	return 	&WebSocketObserver{
		name:    "websocket",
		enabled: true,
		server:  server,
	}
}

// GetName returns the observer's identifier
func (w *WebSocketObserver) GetName() string {
	return w.name
}

// IsEnabled returns whether this observer is active
func (w *WebSocketObserver) IsEnabled() bool {
	return w.enabled
}

// SetEnabled enables or disables this observer
func (w *WebSocketObserver) SetEnabled(enabled bool) {
	w.enabled = enabled
}

// GetSubscribedTopics returns the list of topics this observer wants to subscribe to
func (w *WebSocketObserver) GetSubscribedTopics() []string {
	return []string{
		"flow.start.requested",
		"flow.completed",
		"flow.failed",
		"node.exec.requested",
		"node.completed",
		"node.exec.failed",
		"progress.update",
	}
}

// HandleMessage processes a message from a subscribed topic
func (w *WebSocketObserver) HandleMessage(topic string, msg *message.Message) error {
	if !w.enabled {
		return nil
	}

	// Parse the base message to get the message type
	var baseMsg map[string]interface{}
	if err := json.Unmarshal(msg.Payload, &baseMsg); err != nil {
		log.Error().Err(err).Msg("Failed to parse message in WebSocket observer")
		return err
	}

	// Create event for WebSocket broadcast
	event := createWebSocketEvent(topic, baseMsg)
	
	// Broadcast to all connected clients
	if err := w.server.BroadcastEvent(event); err != nil {
		log.Error().Err(err).Msg("Failed to broadcast event via WebSocket")
		return err
	}

	log.Debug().
		Str("topic", topic).
		Str("event_type", event.EventType).
		Msg("Broadcasted event via WebSocket")

	return nil
}

// WebSocketEvent represents an event that will be sent to WebSocket clients
type WebSocketEvent struct {
	EventType       string      `json:"event_type"`
	Timestamp       time.Time   `json:"timestamp"`
	FlowExecutionID string      `json:"flow_execution_id,omitempty"`
	NodeExecutionID string      `json:"node_execution_id,omitempty"`
	FlowType        string      `json:"flow_type,omitempty"`
	NodeType        string      `json:"node_type,omitempty"`
	NodeID          string      `json:"node_id,omitempty"`
	Action          string      `json:"action,omitempty"`
	Duration        interface{} `json:"duration,omitempty"`
	ErrorMessage    string      `json:"error_message,omitempty"`
	Result          interface{} `json:"result,omitempty"`
	Progress        float64     `json:"progress,omitempty"`
	Message         string      `json:"message,omitempty"`
	Status          string      `json:"status,omitempty"`
}

// createWebSocketEvent creates a WebSocketEvent from a raw message
func createWebSocketEvent(topic string, rawMsg map[string]interface{}) WebSocketEvent {
	event := WebSocketEvent{
		EventType: getStringValue(rawMsg, "message_type"),
		Timestamp: time.Now(),
	}

	// Parse timestamp if available
	if timestampStr, ok := rawMsg["timestamp"].(string); ok {
		if parsedTime, err := time.Parse(time.RFC3339, timestampStr); err == nil {
			event.Timestamp = parsedTime
		}
	}

	// Extract common fields
	event.FlowExecutionID = getStringValue(rawMsg, "flow_execution_id")
	event.NodeExecutionID = getStringValue(rawMsg, "node_execution_id")
	event.FlowType = getStringValue(rawMsg, "flow_type")
	event.NodeType = getStringValue(rawMsg, "node_type")
	event.NodeID = getStringValue(rawMsg, "node_id")
	event.Action = getStringValue(rawMsg, "action")
	event.ErrorMessage = getStringValue(rawMsg, "error_message")
	event.Message = getStringValue(rawMsg, "message")
	event.Status = getStringValue(rawMsg, "status")

	// Extract duration if present
	if duration, ok := rawMsg["duration"]; ok {
		event.Duration = duration
	}

	// Extract result if present
	if result, ok := rawMsg["result"]; ok {
		event.Result = result
	}

	// Extract progress if present
	if progress, ok := rawMsg["progress"]; ok {
		if progFloat, ok := progress.(float64); ok {
			event.Progress = progFloat
		}
	}

	return event
}

// getStringValue safely extracts a string value from a map
func getStringValue(m map[string]interface{}, key string) string {
	if value, ok := m[key].(string); ok {
		return value
	}
	return ""
}