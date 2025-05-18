package event

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// GenericFlowWorker implements the FlowWorker interface
type GenericFlowWorker struct {
	FlowTypeName string
	Publisher    EventPublisher
	StateStore   StateStore
	Registry     FlowRegistry
	Orchestrator *FlowOrchestrator
}

// NewGenericFlowWorker creates a new GenericFlowWorker instance
func NewGenericFlowWorker(
	flowType string,
	publisher EventPublisher,
	stateStore StateStore,
	registry FlowRegistry,
	orchestrator *FlowOrchestrator,
) *GenericFlowWorker {
	return &GenericFlowWorker{
		FlowTypeName: flowType,
		Publisher:    publisher,
		StateStore:   stateStore,
		Registry:     registry,
		Orchestrator: orchestrator,
	}
}

// FlowType returns the type of flow this worker handles
func (w *GenericFlowWorker) FlowType() string {
	return w.FlowTypeName
}

// SupportedMessageTypes returns the list of message types this worker can handle
func (w *GenericFlowWorker) SupportedMessageTypes() []string {
	return []string{
		MessageTypeFlowStartRequested,
		MessageTypeFlowInitialized,
		MessageTypeFlowPauseRequested,
		MessageTypeFlowResumeRequested,
		MessageTypeFlowCancelRequested,
	}
}

// HandleMessage handles a message
func (w *GenericFlowWorker) HandleMessage(msgObj interface{}) error {
	msg, ok := msgObj.(*message.Message)
	if !ok {
		return fmt.Errorf("invalid message type: %T", msgObj)
	}

	// Extract base message to determine message type
	var base BaseMessage
	if err := json.Unmarshal(msg.Payload, &base); err != nil {
		return err
	}

	log.Debug().
		Str("messageType", base.MessageType).
		Str("flowType", w.FlowTypeName).
		Msg("Flow worker received message")

	switch base.MessageType {
	case MessageTypeFlowStartRequested:
		return w.handleFlowStartRequested(msg)
	case MessageTypeFlowInitialized:
		return w.handleFlowInitialized(msg)
	case MessageTypeFlowPauseRequested:
		return w.handleFlowPauseRequested(msg)
	case MessageTypeFlowResumeRequested:
		return w.handleFlowResumeRequested(msg)
	case MessageTypeFlowCancelRequested:
		return w.handleFlowCancelRequested(msg)
	default:
		return fmt.Errorf("unsupported message type: %s", base.MessageType)
	}
}

// HandleNodeCompletedMessage processes node completion events to advance the flow
func (w *GenericFlowWorker) HandleNodeCompletedMessage(msgObj interface{}) error {
	msg, ok := msgObj.(*message.Message)
	if !ok {
		return fmt.Errorf("invalid message type: %T", msgObj)
	}

	var completed NodeCompletedMessage
	if err := json.Unmarshal(msg.Payload, &completed); err != nil {
		return err
	}

	log.Debug().
		Str("nodeType", completed.NodeType).
		Str("action", completed.Action).
		Str("flowExecutionID", completed.FlowExecutionID).
		Msg("Flow worker received node completion")

	// Get flow definition from registry
	flow, err := w.Registry.GetFlowByExecutionID(completed.FlowExecutionID)
	if err != nil {
		return err
	}

	// Only handle if flow type matches
	if flow.Type() != w.FlowTypeName {
		log.Debug().
			Str("thisFlowType", w.FlowTypeName).
			Str("msgFlowType", flow.Type()).
			Msg("Ignoring node completion for different flow type")
		return nil
	}

	// Update shared data with node result
	if err := w.StateStore.UpdateSharedData(completed.FlowExecutionID, completed.NodeID, completed.Result); err != nil {
		return err
	}

	// Find next node based on the action using the flow definition
	nextNode, exists := flow.GetNextNode(completed.NodeID, completed.Action)
	if !exists || nextNode == nil {
		// Flow is complete
		w.publishFlowCompletion(completed.FlowExecutionID, flow.Type(), completed.Action)
		return nil
	}

	// Start the next node
	nodeExecID := uuid.New().String()
	w.Publisher.Publish(
		fmt.Sprintf("node.%s", nextNode.Type()),
		ExecRequestedMessage{
			BaseMessage: BaseMessage{
				MessageType:     MessageTypeExecRequested,
				FlowExecutionID: completed.FlowExecutionID,
				NodeExecutionID: nodeExecID,
				Timestamp:       time.Now(),
			},
			NodeType: nextNode.Type(),
			NodeID:   nextNode.ID(),
			Params:   nextNode.Params(),
		},
	)

	// Publish progress update
	w.publishProgressUpdate(completed.FlowExecutionID, "node_transition", 0.5,
		fmt.Sprintf("Moving from %s to %s", completed.NodeType, nextNode.Type()))

	return nil
}

// Handle flow start requested message
func (w *GenericFlowWorker) handleFlowStartRequested(msg *message.Message) error {
	var event FlowStartRequestedMessage
	if err := json.Unmarshal(msg.Payload, &event); err != nil {
		return err
	}

	// Verify flow type matches this worker
	if event.FlowType != w.FlowTypeName && event.FlowType != "" {
		log.Debug().
			Str("thisFlowType", w.FlowTypeName).
			Str("msgFlowType", event.FlowType).
			Msg("Ignoring flow start for different flow type")
		return nil
	}

	log.Info().
		Str("flowExecutionID", event.FlowExecutionID).
		Str("flowDefinitionID", event.FlowDefinitionID).
		Msg("Handling flow start request")

	// Store initial shared data
	if err := w.StateStore.StoreSharedData(event.FlowExecutionID, event.InitialSharedData); err != nil {
		w.publishFlowFailed(event.FlowExecutionID, "Failed to store initial shared data", err.Error(), "")
		return err
	}

	// Get flow definition
	flow, err := w.Registry.GetFlow(event.FlowDefinitionID)
	if err != nil {
		w.publishFlowFailed(event.FlowExecutionID, "Failed to get flow definition", err.Error(), "")
		return err
	}

	// Store the mapping between execution ID and definition ID
	if err := w.StateStore.StoreFlowExecution(event.FlowExecutionID, event.FlowDefinitionID); err != nil {
		w.publishFlowFailed(event.FlowExecutionID, "Failed to store flow execution mapping", err.Error(), "")
		return err
	}

	// Publish flow initialized event
	w.Publisher.Publish(
		fmt.Sprintf("flow.%s", w.FlowTypeName),
		FlowInitializedMessage{
			BaseMessage: BaseMessage{
				MessageType:     MessageTypeFlowInitialized,
				FlowExecutionID: event.FlowExecutionID,
				Timestamp:       time.Now(),
			},
			FlowType:         flow.Type(),
			FlowDefinitionID: flow.ID(),
		},
	)

	// Publish progress update
	w.publishProgressUpdate(event.FlowExecutionID, "flow_started", 0.0, "Flow started")

	// Start the first node
	startNode := flow.StartNode()
	if startNode == nil {
		w.publishFlowFailed(event.FlowExecutionID, "Flow has no start node", "", "")
		return fmt.Errorf("flow has no start node")
	}

	nodeExecID := uuid.New().String()
	w.Publisher.Publish(
		fmt.Sprintf("node.%s", startNode.Type()),
		ExecRequestedMessage{
			BaseMessage: BaseMessage{
				MessageType:     MessageTypeExecRequested,
				FlowExecutionID: event.FlowExecutionID,
				NodeExecutionID: nodeExecID,
				Timestamp:       time.Now(),
			},
			NodeType: startNode.Type(),
			NodeID:   startNode.ID(),
			Params:   startNode.Params(),
		},
	)

	return nil
}

// Handle flow initialized message
func (w *GenericFlowWorker) handleFlowInitialized(msg *message.Message) error {
	var event FlowInitializedMessage
	if err := json.Unmarshal(msg.Payload, &event); err != nil {
		return err
	}

	log.Info().
		Str("flowExecutionID", event.FlowExecutionID).
		Str("flowType", event.FlowType).
		Msg("Flow initialized")

	return nil
}

// Handle flow pause requested message
func (w *GenericFlowWorker) handleFlowPauseRequested(msg *message.Message) error {
	var event FlowPauseRequestedMessage
	if err := json.Unmarshal(msg.Payload, &event); err != nil {
		return err
	}

	log.Info().
		Str("flowExecutionID", event.FlowExecutionID).
		Str("reason", event.Reason).
		Msg("Flow pause requested")

	// Publish flow paused event
	w.Publisher.Publish(
		fmt.Sprintf("flow.%s", w.FlowTypeName),
		FlowPausedMessage{
			BaseMessage: BaseMessage{
				MessageType:     MessageTypeFlowPaused,
				FlowExecutionID: event.FlowExecutionID,
				Timestamp:       time.Now(),
			},
			FlowType: w.FlowTypeName,
		},
	)

	return nil
}

// Handle flow resume requested message
func (w *GenericFlowWorker) handleFlowResumeRequested(msg *message.Message) error {
	var event FlowResumeRequestedMessage
	if err := json.Unmarshal(msg.Payload, &event); err != nil {
		return err
	}

	log.Info().
		Str("flowExecutionID", event.FlowExecutionID).
		Msg("Flow resume requested")

	// Implementation would depend on how pause/resume is tracked
	// For now, we'll just log the event

	return nil
}

// Handle flow cancel requested message
func (w *GenericFlowWorker) handleFlowCancelRequested(msg *message.Message) error {
	var event FlowCancelRequestedMessage
	if err := json.Unmarshal(msg.Payload, &event); err != nil {
		return err
	}

	log.Info().
		Str("flowExecutionID", event.FlowExecutionID).
		Str("reason", event.Reason).
		Msg("Flow cancel requested")

	// Implementation would depend on how cancellation is handled
	// For now, we'll just log the event

	return nil
}

// Publish flow completion event
func (w *GenericFlowWorker) publishFlowCompletion(flowExecID, flowType, finalAction string) {
	log.Info().
		Str("flowExecutionID", flowExecID).
		Str("flowType", flowType).
		Str("finalAction", finalAction).
		Msg("Flow completed")

	// Publish to flow-specific topic
	w.Publisher.Publish(
		fmt.Sprintf("flow.%s", w.FlowTypeName),
		FlowCompletedMessage{
			BaseMessage: BaseMessage{
				MessageType:     MessageTypeFlowCompleted,
				FlowExecutionID: flowExecID,
				Timestamp:       time.Now(),
			},
			FlowType:    flowType,
			FinalAction: finalAction,
			ExecutionMs: 0, // Could calculate this if needed
		},
	)

	// Publish to common flow.completed topic
	w.Publisher.Publish(
		"flow.completed",
		FlowCompletedMessage{
			BaseMessage: BaseMessage{
				MessageType:     MessageTypeFlowCompleted,
				FlowExecutionID: flowExecID,
				Timestamp:       time.Now(),
			},
			FlowType:    flowType,
			FinalAction: finalAction,
			ExecutionMs: 0,
		},
	)

	// Publish final progress update
	w.publishProgressUpdate(flowExecID, "flow_completed", 1.0, "Flow completed successfully")
}

// Publish flow failed event
func (w *GenericFlowWorker) publishFlowFailed(flowExecID, errMsg, errDetails, failedNodeID string) {
	log.Error().
		Str("flowExecutionID", flowExecID).
		Str("errorMessage", errMsg).
		Str("errorDetails", errDetails).
		Str("failedNodeID", failedNodeID).
		Msg("Flow failed")

	// Publish to flow-specific topic
	w.Publisher.Publish(
		fmt.Sprintf("flow.%s", w.FlowTypeName),
		FlowFailedMessage{
			BaseMessage: BaseMessage{
				MessageType:     MessageTypeFlowFailed,
				FlowExecutionID: flowExecID,
				Timestamp:       time.Now(),
			},
			FlowType:     w.FlowTypeName,
			ErrorMessage: errMsg,
			ErrorDetails: errDetails,
			FailedNodeID: failedNodeID,
		},
	)

	// Publish to common flow.failed topic
	w.Publisher.Publish(
		"flow.failed",
		FlowFailedMessage{
			BaseMessage: BaseMessage{
				MessageType:     MessageTypeFlowFailed,
				FlowExecutionID: flowExecID,
				Timestamp:       time.Now(),
			},
			FlowType:     w.FlowTypeName,
			ErrorMessage: errMsg,
			ErrorDetails: errDetails,
			FailedNodeID: failedNodeID,
		},
	)

	// Publish progress update
	w.publishProgressUpdate(flowExecID, "flow_failed", 1.0, "Flow failed: "+errMsg)
}

// Publish progress update event
func (w *GenericFlowWorker) publishProgressUpdate(flowExecID, status string, progress float64, message string) {
	w.Publisher.Publish(
		"progress",
		ProgressUpdateMessage{
			BaseMessage: BaseMessage{
				MessageType:     MessageTypeProgressUpdate,
				FlowExecutionID: flowExecID,
				Timestamp:       time.Now(),
			},
			Status:   status,
			Progress: progress,
			Message:  message,
		},
	)
}
