package impl

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// GenericFlowWorker implements the FlowWorker interface
type GenericFlowWorker struct {
	FlowTypeName string
	Publisher    core.EventPublisher
	StateStore   core.StateStore
	Registry     core.FlowRegistry
	Orchestrator *FlowOrchestrator
}

func NewGenericFlowWorker(
	flowType string,
	publisher core.EventPublisher,
	stateStore core.StateStore,
	registry core.FlowRegistry,
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

func (w *GenericFlowWorker) FlowType() string {
	return w.FlowTypeName
}

func (w *GenericFlowWorker) SupportedMessageTypes() []string {
	return []string{
		core.MessageTypeFlowStartRequested,
		core.MessageTypeFlowInitialized,
		core.MessageTypeFlowPauseRequested,
		core.MessageTypeFlowResumeRequested,
		core.MessageTypeFlowCancelRequested,
	}
}

func (w *GenericFlowWorker) HandleMessage(msgObj interface{}) error {
	msg, ok := msgObj.(*message.Message)
	if !ok {
		return fmt.Errorf("invalid message type")
	}
	
	var base core.BaseMessage
	if err := json.Unmarshal(msg.Payload, &base); err != nil {
		return err
	}
	
	switch base.MessageType {
	case core.MessageTypeFlowStartRequested:
		return w.handleFlowStartRequested(msg)
	case core.MessageTypeFlowInitialized:
		return w.handleFlowInitialized(msg)
	case core.MessageTypeFlowPauseRequested:
		return w.handleFlowPauseRequested(msg)
	case core.MessageTypeFlowResumeRequested:
		return w.handleFlowResumeRequested(msg)
	case core.MessageTypeFlowCancelRequested:
		return w.handleFlowCancelRequested(msg)
	default:
		return fmt.Errorf("unsupported message type: %s", base.MessageType)
	}
}

func (w *GenericFlowWorker) HandleNodeCompletedMessage(msgObj interface{}) error {
	msg, ok := msgObj.(*message.Message)
	if !ok {
		return fmt.Errorf("invalid message type")
	}
	
	var completed core.NodeCompletedMessage
	if err := json.Unmarshal(msg.Payload, &completed); err != nil {
		return err
	}
	
	// Get flow definition from registry
	flow, err := w.Registry.GetFlowByExecutionID(completed.FlowExecutionID)
	if err != nil {
		return err
	}
	
	// Only handle if flow type matches
	if flow.Type() != w.FlowTypeName {
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
		log.Info().Str("flowExecutionID", completed.FlowExecutionID).Msg("Flow completed")
		return nil
	}
	
	// Start the next node
	nodeExecID := uuid.New().String()
	w.Publisher.Publish(
		fmt.Sprintf("node.%s", nextNode.Type()),
		core.ExecRequestedMessage{
			BaseMessage: core.BaseMessage{
				MessageType:     core.MessageTypeExecRequested,
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

func (w *GenericFlowWorker) handleFlowStartRequested(msg *message.Message) error {
	var flowStart core.FlowStartRequestedMessage
	if err := json.Unmarshal(msg.Payload, &flowStart); err != nil {
		return err
	}
	
	// Only handle if flow type matches
	if flowStart.FlowType != w.FlowTypeName {
		return nil
	}
	
	// Store the initial shared data
	if err := w.StateStore.StoreSharedData(flowStart.FlowExecutionID, flowStart.InitialSharedData); err != nil {
		return w.handleFlowInitializationError(flowStart, err)
	}
	
	// Get flow definition
	flow, err := w.Registry.GetFlow(flowStart.FlowDefinitionID)
	if err != nil {
		return w.handleFlowInitializationError(flowStart, err)
	}
	
	// Map execution ID to flow definition ID
	if mapExec, ok := w.Registry.(interface{
		MapExecutionToFlow(string, string)
	}); ok {
		mapExec.MapExecutionToFlow(flowStart.FlowExecutionID, flowStart.FlowDefinitionID)
	}
	
	// Publish flow initialized event
	w.Publisher.Publish(
		fmt.Sprintf("flow.%s", w.FlowTypeName),
		core.FlowInitializedMessage{
			BaseMessage: core.BaseMessage{
				MessageType:     core.MessageTypeFlowInitialized,
				FlowExecutionID: flowStart.FlowExecutionID,
				Timestamp:       time.Now(),
			},
			FlowType:         w.FlowTypeName,
			FlowDefinitionID: flowStart.FlowDefinitionID,
		},
	)
	
	// Publish initial progress update
	w.publishProgressUpdate(flowStart.FlowExecutionID, "flow_started", 0.0,
		fmt.Sprintf("Starting flow of type %s", w.FlowTypeName))
	
	// Get the start node
	startNode := flow.StartNode()
	if startNode == nil {
		return w.handleFlowInitializationError(flowStart, fmt.Errorf("flow has no start node"))
	}
	
	// Start the first node
	nodeExecID := uuid.New().String()
	w.Publisher.Publish(
		fmt.Sprintf("node.%s", startNode.Type()),
		core.ExecRequestedMessage{
			BaseMessage: core.BaseMessage{
				MessageType:     core.MessageTypeExecRequested,
				FlowExecutionID: flowStart.FlowExecutionID,
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

func (w *GenericFlowWorker) handleFlowInitializationError(flowStart core.FlowStartRequestedMessage, err error) error {
	// Log the error
	log.Error().
		Err(err).
		Str("flowExecutionID", flowStart.FlowExecutionID).
		Str("flowType", w.FlowTypeName).
		Msg("Failed to initialize flow")
	
	// Publish flow failed event
	w.Publisher.Publish(
		fmt.Sprintf("flow.%s", w.FlowTypeName),
		core.FlowFailedMessage{
			BaseMessage: core.BaseMessage{
				MessageType:     core.MessageTypeFlowFailed,
				FlowExecutionID: flowStart.FlowExecutionID,
				Timestamp:       time.Now(),
			},
			FlowType:     w.FlowTypeName,
			ErrorMessage: "Failed to initialize flow",
			ErrorDetails: err.Error(),
		},
	)
	
	// Also publish to universal failed topic
	w.Publisher.Publish(
		core.TopicFlowFailed,
		core.FlowFailedMessage{
			BaseMessage: core.BaseMessage{
				MessageType:     core.MessageTypeFlowFailed,
				FlowExecutionID: flowStart.FlowExecutionID,
				Timestamp:       time.Now(),
			},
			FlowType:     w.FlowTypeName,
			ErrorMessage: "Failed to initialize flow",
			ErrorDetails: err.Error(),
		},
	)
	
	// Publish error progress update
	w.publishProgressUpdate(flowStart.FlowExecutionID, "flow_failed", 0.0,
		"Failed to initialize flow: "+err.Error())
	
	return nil
}

func (w *GenericFlowWorker) handleFlowInitialized(msg *message.Message) error {
	// This is mostly a signal event, nothing to do
	return nil
}

func (w *GenericFlowWorker) handleFlowPauseRequested(msg *message.Message) error {
	// TODO: Implement flow pause handling
	return fmt.Errorf("pause not yet implemented")
}

func (w *GenericFlowWorker) handleFlowResumeRequested(msg *message.Message) error {
	// TODO: Implement flow resume handling
	return fmt.Errorf("resume not yet implemented")
}

func (w *GenericFlowWorker) handleFlowCancelRequested(msg *message.Message) error {
	// TODO: Implement flow cancellation handling
	return fmt.Errorf("cancellation not yet implemented")
}

func (w *GenericFlowWorker) publishFlowCompletion(flowExecutionID, flowType, finalAction string) {
	// Get the final shared data to include as result
	sharedData, err := w.StateStore.GetSharedData(flowExecutionID)
	if err != nil {
		log.Error().Err(err).Str("flowExecutionID", flowExecutionID).Msg("Failed to get shared data for completion")
	}
	
	// Calculate execution time
	var executionMs int64 = 0
	if startTime, ok := sharedData["started_at"].(string); ok {
		if t, err := time.Parse(time.RFC3339, startTime); err == nil {
			executionMs = time.Since(t).Milliseconds()
		}
	}
	
	// Publish to specific flow type topic
	w.Publisher.Publish(
		fmt.Sprintf("flow.%s", flowType),
		core.FlowCompletedMessage{
			BaseMessage: core.BaseMessage{
				MessageType:     core.MessageTypeFlowCompleted,
				FlowExecutionID: flowExecutionID,
				Timestamp:       time.Now(),
			},
			FlowType:    flowType,
			FinalAction: finalAction,
			FinalResult: sharedData,
			ExecutionMs: executionMs,
		},
	)
	
	// Also publish to universal completed topic
	w.Publisher.Publish(
		core.TopicFlowCompleted,
		core.FlowCompletedMessage{
			BaseMessage: core.BaseMessage{
				MessageType:     core.MessageTypeFlowCompleted,
				FlowExecutionID: flowExecutionID,
				Timestamp:       time.Now(),
			},
			FlowType:    flowType,
			FinalAction: finalAction,
			FinalResult: sharedData,
			ExecutionMs: executionMs,
		},
	)
	
	// Publish final progress update
	w.publishProgressUpdate(flowExecutionID, "flow_completed", 1.0,
		fmt.Sprintf("Flow completed with action: %s", finalAction))
}

func (w *GenericFlowWorker) publishProgressUpdate(flowExecutionID, status string, progress float64, message string) {
	w.Publisher.Publish(
		core.TopicProgress,
		core.ProgressUpdateMessage{
			BaseMessage: core.BaseMessage{
				MessageType:     core.MessageTypeProgressUpdate,
				FlowExecutionID: flowExecutionID,
				Timestamp:       time.Now(),
			},
			Status:   status,
			Progress: progress,
			Message:  message,
		},
	)
}