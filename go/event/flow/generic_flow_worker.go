package flow

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/The-Pocket/PocketFlow/go/semantic"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// GenericFlowWorker implements the FlowWorker interface
type GenericFlowWorker struct {
	FlowTypeName string
	Publisher    core.EventPublisher
	StateStore   semantic.StateStore
	Registry     core.FlowRegistry
}

func NewGenericFlowWorker(
	flowType string,
	publisher core.EventPublisher,
	stateStore semantic.StateStore,
	registry core.FlowRegistry,
) *GenericFlowWorker {
	return &GenericFlowWorker{
		FlowTypeName: flowType,
		Publisher:    publisher,
		StateStore:   stateStore,
		Registry:     registry,
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
		log.Error().Msg("FlowWorker received invalid message type")
		return fmt.Errorf("invalid message type")
	}
	
	var base core.BaseMessage
	if err := json.Unmarshal(msg.Payload, &base); err != nil {
		log.Error().Err(err).Str("messageID", msg.UUID).Msg("FlowWorker failed to unmarshal base message")
		return err
	}
	
	log.Debug().
		Str("flowType", w.FlowTypeName).
		Str("messageType", base.MessageType).
		Str("flowExecutionID", base.FlowExecutionID).
		Str("messageID", msg.UUID).
		Msg("FlowWorker handling message")
	
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
	case core.MessageTypeFlowCompleted, core.MessageTypeFlowFailed:
		// Ignore flow completion and failure messages since we probably emitted it
		log.Debug().
			Str("flowType", w.FlowTypeName).
			Str("messageType", base.MessageType).
			Str("flowExecutionID", base.FlowExecutionID).
			Msg("FlowWorker ignoring own completion/failure message")
		return nil
	default:
		log.Warn().
			Str("flowType", w.FlowTypeName).
			Str("messageType", base.MessageType).
			Str("flowExecutionID", base.FlowExecutionID).
			Msg("FlowWorker received unsupported message type")
		return fmt.Errorf("unsupported message type: %s", base.MessageType)
	}
}

func (w *GenericFlowWorker) HandleNodeCompletedMessage(msgObj interface{}) error {
	msg, ok := msgObj.(*message.Message)
	if !ok {
		log.Error().Msg("FlowWorker received invalid message type for node completion")
		return fmt.Errorf("invalid message type")
	}
	
	var completed core.NodeCompletedMessage
	if err := json.Unmarshal(msg.Payload, &completed); err != nil {
		log.Error().Err(err).Str("messageID", msg.UUID).Msg("FlowWorker failed to unmarshal node completed message")
		return err
	}
	
	log.Debug().
		Str("flowType", w.FlowTypeName).
		Str("flowExecutionID", completed.FlowExecutionID).
		Str("nodeExecutionID", completed.NodeExecutionID).
		Str("nodeType", completed.NodeType).
		Str("nodeID", completed.NodeID).
		Str("action", completed.Action).
		Str("messageID", msg.UUID).
		Msg("FlowWorker handling node completion")
	
	// Get flow definition from registry
	flow, err := w.Registry.GetFlowByExecutionID(completed.FlowExecutionID)
	if err != nil {
		log.Error().
			Err(err).
			Str("flowType", w.FlowTypeName).
			Str("flowExecutionID", completed.FlowExecutionID).
			Msg("FlowWorker failed to get flow definition from registry")
		return err
	}
	
	// Only handle if flow type matches
	if flow.Type() != w.FlowTypeName {
		log.Debug().
			Str("flowType", w.FlowTypeName).
			Str("actualFlowType", flow.Type()).
			Str("flowExecutionID", completed.FlowExecutionID).
			Msg("FlowWorker ignoring node completion for different flow type")
		return nil
	}
	
	// Update shared data with node result
	if err := w.StateStore.UpdateSharedData(completed.FlowExecutionID, completed.NodeID, completed.Result); err != nil {
		log.Error().
			Err(err).
			Str("flowType", w.FlowTypeName).
			Str("flowExecutionID", completed.FlowExecutionID).
			Str("nodeID", completed.NodeID).
			Msg("FlowWorker failed to update shared data with node result")
		return err
	}
	
	log.Debug().
		Str("flowType", w.FlowTypeName).
		Str("flowExecutionID", completed.FlowExecutionID).
		Str("nodeID", completed.NodeID).
		Interface("result", completed.Result).
		Msg("FlowWorker updated shared data with node result")
	
	// Find next node based on the action using the flow definition
	nextNode, exists := flow.GetNextNode(completed.NodeID, completed.Action)
	if !exists || nextNode == nil {
		// Flow is complete
		log.Debug().
			Str("flowType", w.FlowTypeName).
			Str("flowExecutionID", completed.FlowExecutionID).
			Str("currentNodeID", completed.NodeID).
			Str("action", completed.Action).
			Bool("exists", exists).
			Msg("FlowWorker determined flow is complete - no next node found")
		w.publishFlowCompletion(completed.FlowExecutionID, flow.Type(), completed.Action)
		log.Info().Str("flowExecutionID", completed.FlowExecutionID).Msg("Flow completed")
		return nil
	}
	
	// Start the next node
	nodeExecID := uuid.New().String()
	
	log.Debug().
		Str("flowType", w.FlowTypeName).
		Str("flowExecutionID", completed.FlowExecutionID).
		Str("currentNodeID", completed.NodeID).
		Str("currentNodeType", completed.NodeType).
		Str("action", completed.Action).
		Str("nextNodeID", nextNode.ID()).
		Str("nextNodeType", nextNode.Type()).
		Str("nodeExecutionID", nodeExecID).
		Interface("nextNodeParams", nextNode.Params()).
		Msg("FlowWorker starting next node execution")
	
	topic := fmt.Sprintf("node.%s", nextNode.Type())
	
	err = w.Publisher.Publish(
		topic,
		core.ExecRequestedMessage{
			BaseMessage: core.BaseMessage{
				MessageType:     core.MessageTypeExecRequested,
				FlowExecutionID: completed.FlowExecutionID,
				NodeExecutionID: nodeExecID,
				Timestamp:       time.Now(),
				FlowType:        w.FlowTypeName,
			},
			NodeType: nextNode.Type(),
			NodeID:   nextNode.ID(),
			Params:   nextNode.Params(),
		},
	)
	
	if err != nil {
		log.Error().
			Err(err).
			Str("flowType", w.FlowTypeName).
			Str("topic", topic).
			Str("nextNodeType", nextNode.Type()).
			Str("flowExecutionID", completed.FlowExecutionID).
			Msg("FlowWorker failed to publish exec request for next node")
		return err
	}
	
	log.Debug().
		Str("flowType", w.FlowTypeName).
		Str("topic", topic).
		Str("nextNodeType", nextNode.Type()).
		Str("flowExecutionID", completed.FlowExecutionID).
		Str("nodeExecutionID", nodeExecID).
		Msg("FlowWorker successfully published exec request for next node")
	
	// Publish progress update
	w.publishProgressUpdate(completed.FlowExecutionID, "node_transition", 0.5,
		fmt.Sprintf("Moving from %s to %s", completed.NodeType, nextNode.Type()))
	
	return nil
}

func (w *GenericFlowWorker) handleFlowStartRequested(msg *message.Message) error {
	var flowStart core.FlowStartRequestedMessage
	if err := json.Unmarshal(msg.Payload, &flowStart); err != nil {
		log.Error().Err(err).Str("messageID", msg.UUID).Msg("FlowWorker failed to unmarshal flow start message")
		return err
	}
	
	log.Debug().
		Str("flowType", w.FlowTypeName).
		Str("requestedFlowType", flowStart.FlowType).
		Str("flowExecutionID", flowStart.FlowExecutionID).
		Str("flowDefinitionID", flowStart.FlowDefinitionID).
		Interface("initialSharedData", flowStart.InitialSharedData).
		Str("messageID", msg.UUID).
		Msg("FlowWorker received flow start request")
	
	// Only handle if flow type matches
	if flowStart.FlowType != w.FlowTypeName {
		log.Debug().
			Str("flowType", w.FlowTypeName).
			Str("requestedFlowType", flowStart.FlowType).
			Str("flowExecutionID", flowStart.FlowExecutionID).
			Msg("FlowWorker ignoring flow start for different flow type")
		return nil
	}
	
	log.Debug().
		Str("flowType", w.FlowTypeName).
		Str("flowExecutionID", flowStart.FlowExecutionID).
		Msg("FlowWorker processing flow start request")
	
	// Store the initial shared data
	for key, value := range flowStart.InitialSharedData {
		if err := w.StateStore.UpdateSharedData(flowStart.FlowExecutionID, key, value); err != nil {
			log.Error().
				Err(err).
				Str("flowType", w.FlowTypeName).
				Str("flowExecutionID", flowStart.FlowExecutionID).
				Str("key", key).
				Msg("FlowWorker failed to store initial shared data")
			return w.handleFlowInitializationError(flowStart, err)
		}
	}
	
	log.Debug().
		Str("flowType", w.FlowTypeName).
		Str("flowExecutionID", flowStart.FlowExecutionID).
		Interface("initialSharedData", flowStart.InitialSharedData).
		Msg("FlowWorker stored initial shared data")
	
	// Get flow definition
	flow, err := w.Registry.GetFlow(flowStart.FlowDefinitionID)
	if err != nil {
		log.Error().
			Err(err).
			Str("flowType", w.FlowTypeName).
			Str("flowExecutionID", flowStart.FlowExecutionID).
			Str("flowDefinitionID", flowStart.FlowDefinitionID).
			Msg("FlowWorker failed to get flow definition from registry")
		return w.handleFlowInitializationError(flowStart, err)
	}
	
	log.Debug().
		Str("flowType", w.FlowTypeName).
		Str("flowExecutionID", flowStart.FlowExecutionID).
		Str("flowDefinitionID", flowStart.FlowDefinitionID).
		Str("flowName", flow.Name()).
		Msg("FlowWorker retrieved flow definition")
	
	// Map execution ID to flow definition ID
	if mapExec, ok := w.Registry.(interface{
		MapExecutionToFlow(string, string)
	}); ok {
		mapExec.MapExecutionToFlow(flowStart.FlowExecutionID, flowStart.FlowDefinitionID)
		log.Debug().
			Str("flowType", w.FlowTypeName).
			Str("flowExecutionID", flowStart.FlowExecutionID).
			Str("flowDefinitionID", flowStart.FlowDefinitionID).
			Msg("FlowWorker mapped execution ID to flow definition ID")
	}
	
	// Publish flow initialized event
	initTopic := fmt.Sprintf("flow.%s", w.FlowTypeName)
	err = w.Publisher.Publish(
		initTopic,
		core.FlowInitializedMessage{
			BaseMessage: core.BaseMessage{
				MessageType:     core.MessageTypeFlowInitialized,
				FlowExecutionID: flowStart.FlowExecutionID,
				Timestamp:       time.Now(),
				FlowType:        w.FlowTypeName,
			},
			FlowDefinitionID: flowStart.FlowDefinitionID,
		},
	)
	
	if err != nil {
		log.Error().
			Err(err).
			Str("flowType", w.FlowTypeName).
			Str("topic", initTopic).
			Str("flowExecutionID", flowStart.FlowExecutionID).
			Msg("FlowWorker failed to publish flow initialized message")
		return w.handleFlowInitializationError(flowStart, err)
	}
	
	log.Debug().
		Str("flowType", w.FlowTypeName).
		Str("topic", initTopic).
		Str("flowExecutionID", flowStart.FlowExecutionID).
		Msg("FlowWorker published flow initialized message")
	
	// Publish initial progress update
	w.publishProgressUpdate(flowStart.FlowExecutionID, "flow_started", 0.0,
		fmt.Sprintf("Starting flow of type %s", w.FlowTypeName))
	
	// Get the start node
	startNode := flow.StartNode()
	if startNode == nil {
		log.Error().
			Str("flowType", w.FlowTypeName).
			Str("flowExecutionID", flowStart.FlowExecutionID).
			Str("flowDefinitionID", flowStart.FlowDefinitionID).
			Msg("FlowWorker found flow has no start node")
		return w.handleFlowInitializationError(flowStart, fmt.Errorf("flow has no start node"))
	}
	
	log.Debug().
		Str("flowType", w.FlowTypeName).
		Str("flowExecutionID", flowStart.FlowExecutionID).
		Str("startNodeID", startNode.ID()).
		Str("startNodeType", startNode.Type()).
		Interface("startNodeParams", startNode.Params()).
		Msg("FlowWorker found start node")
	
	// Start the first node
	nodeExecID := uuid.New().String()
	startNodeTopic := fmt.Sprintf("node.%s", startNode.Type())
	
	log.Debug().
		Str("flowType", w.FlowTypeName).
		Str("flowExecutionID", flowStart.FlowExecutionID).
		Str("startNodeID", startNode.ID()).
		Str("startNodeType", startNode.Type()).
		Str("nodeExecutionID", nodeExecID).
		Str("topic", startNodeTopic).
		Msg("FlowWorker starting first node execution")
	
	err = w.Publisher.Publish(
		startNodeTopic,
		core.ExecRequestedMessage{
			BaseMessage: core.BaseMessage{
				MessageType:     core.MessageTypeExecRequested,
				FlowExecutionID: flowStart.FlowExecutionID,
				NodeExecutionID: nodeExecID,
				Timestamp:       time.Now(),
				FlowType:        w.FlowTypeName,
			},
			NodeType: startNode.Type(),
			NodeID:   startNode.ID(),
			Params:   startNode.Params(),
		},
	)
	
	if err != nil {
		log.Error().
			Err(err).
			Str("flowType", w.FlowTypeName).
			Str("topic", startNodeTopic).
			Str("startNodeType", startNode.Type()).
			Str("flowExecutionID", flowStart.FlowExecutionID).
			Msg("FlowWorker failed to publish exec request for start node")
		return w.handleFlowInitializationError(flowStart, err)
	}
	
	log.Debug().
		Str("flowType", w.FlowTypeName).
		Str("topic", startNodeTopic).
		Str("startNodeType", startNode.Type()).
		Str("flowExecutionID", flowStart.FlowExecutionID).
		Str("nodeExecutionID", nodeExecID).
		Msg("FlowWorker successfully published exec request for start node")
	
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
				FlowType:        w.FlowTypeName,
			},
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
				FlowType:        w.FlowTypeName,
			},
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
				FlowType:        w.FlowTypeName,
			},
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
				FlowType:        w.FlowTypeName,
			},		
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
				FlowType:        w.FlowTypeName,
			},
			Status:   status,
			Progress: progress,
			Message:  message,
		},
	)
}