package web

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/The-Pocket/PocketFlow/go/semantic"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// DelayNode represents a node that introduces a delay
type DelayNode struct {
	id     string
	params core.NodeParams
}

// ID returns the node's unique identifier
func (n *DelayNode) ID() string {
	return n.id
}

// Type returns the node type
func (n *DelayNode) Type() string {
	return "delay"
}

// Name returns the node's display name
func (n *DelayNode) Name() string {
	if name, ok := n.params["name"].(string); ok {
		return name
	}
	return fmt.Sprintf("Delay Node (%s)", n.id[:8])
}

// Params returns the node's parameters
func (n *DelayNode) Params() core.NodeParams {
	return n.params
}

// DelayNodeWorker implements NodeWorker for delay nodes
type DelayNodeWorker struct {
	publisher  core.EventPublisher
	stateStore semantic.StateStore
}

// NewDelayNodeWorker creates a new delay node worker
func NewDelayNodeWorker(publisher core.EventPublisher, stateStore semantic.StateStore) core.NodeWorker {
	return &DelayNodeWorker{
		publisher:  publisher,
		stateStore: stateStore,
	}
}

// NodeType returns the type of nodes this worker handles
func (w *DelayNodeWorker) NodeType() string {
	return "delay"
}

// SupportedMessageTypes returns the message types this worker supports
func (w *DelayNodeWorker) SupportedMessageTypes() []string {
	return []string{core.MessageTypeExecRequested}
}

// HandleMessage processes messages for delay nodes
func (w *DelayNodeWorker) HandleMessage(msgObj interface{}) error {
	msg, ok := msgObj.(core.ExecRequestedMessage)
	if !ok {
		return fmt.Errorf("unsupported message type for delay node worker")
	}

	log.Info().
		Str("nodeID", msg.NodeID).
		Str("flowExecutionID", msg.FlowExecutionID).
		Msg("Starting delay node execution")

	// Get delay duration from params
	delaySeconds := 2 // default
	if delay, ok := msg.Params["delay_seconds"]; ok {
		if delayFloat, ok := delay.(float64); ok {
			delaySeconds = int(delayFloat)
		} else if delayInt, ok := delay.(int); ok {
			delaySeconds = delayInt
		}
	}

	// Get message to display
	message := "Delay completed"
	if msg, ok := msg.Params["message"].(string); ok {
		message = msg
	}

	// Publish progress update
	progressMsg := core.ProgressUpdateMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     core.MessageTypeProgressUpdate,
			FlowExecutionID: msg.FlowExecutionID,
			NodeExecutionID: msg.NodeExecutionID,
			Timestamp:       time.Now(),
		},
		Status:   "delaying",
		Progress: 0.0,
		Message:  fmt.Sprintf("Starting %d second delay...", delaySeconds),
	}

	progressJSON, _ := json.Marshal(progressMsg)
	w.publisher.Publish("progress", progressJSON)

	// Perform the delay with progress updates
	startTime := time.Now()
	
	for i := 0; i < delaySeconds; i++ {
		time.Sleep(1 * time.Second)
		
		progress := float64(i+1) / float64(delaySeconds)
		progressMsg.Progress = progress
		progressMsg.Message = fmt.Sprintf("Delay progress: %d/%d seconds", i+1, delaySeconds)
		progressMsg.Timestamp = time.Now()
		
		progressJSON, _ := json.Marshal(progressMsg)
		w.publisher.Publish("progress", progressJSON)
	}

	// Calculate actual duration
	actualDuration := time.Since(startTime)

	// Store result in shared state
	sharedData, err := w.stateStore.GetSharedData(msg.FlowExecutionID)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get shared data")
		return err
	}

	// Add delay result to shared data
	delayKey := fmt.Sprintf("delay_%s", msg.NodeID[:8])
	sharedData[delayKey] = map[string]interface{}{
		"message":          message,
		"delay_seconds":    delaySeconds,
		"actual_duration":  actualDuration.String(),
		"completed_at":     time.Now().Format(time.RFC3339),
	}

	if err := w.stateStore.UpdateSharedData(msg.FlowExecutionID, delayKey, sharedData[delayKey]); err != nil {
		log.Error().Err(err).Msg("Failed to update shared data")
		return err
	}

	// Publish completion message
	completedMsg := core.NodeCompletedMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     core.MessageTypeNodeCompleted,
			FlowExecutionID: msg.FlowExecutionID,
			NodeExecutionID: msg.NodeExecutionID,
			Timestamp:       time.Now(),
		},
		NodeType: "delay",
		NodeID:   msg.NodeID,
		Action:   "default",
		Result: map[string]interface{}{
			"message":         message,
			"delay_seconds":   delaySeconds,
			"actual_duration": actualDuration.Milliseconds(),
		},
	}

	completedJSON, err := json.Marshal(completedMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal completed message: %w", err)
	}

	if err := w.publisher.Publish("node.completed", completedJSON); err != nil {
		return fmt.Errorf("failed to publish completed message: %w", err)
	}

	log.Info().
		Str("nodeID", msg.NodeID).
		Dur("duration", actualDuration).
		Str("message", message).
		Msg("Delay node execution completed")

	return nil
}

// Legacy methods (required by NodeWorker interface)
func (w *DelayNodeWorker) HandlePrepRequested(event core.NodePrepRequested)   {}
func (w *DelayNodeWorker) HandleExecRequested(event core.NodeExecRequested)   {}
func (w *DelayNodeWorker) HandlePostRequested(event core.NodePostRequested)   {}
func (w *DelayNodeWorker) HandleExecFailed(event core.NodeExecFailed)         {}

// NewNode creates a new delay node with the given parameters
func (w *DelayNodeWorker) NewNode(params core.NodeParams) core.Node {
	return &DelayNode{
		id:     uuid.New().String(),
		params: params,
	}
}

// GetTopicSubscriptions returns topics this worker should subscribe to
func (w *DelayNodeWorker) GetTopicSubscriptions() []string {
	return []string{
		fmt.Sprintf("node.%s", w.NodeType()),
	}
}