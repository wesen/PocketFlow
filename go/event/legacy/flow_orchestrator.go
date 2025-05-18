package event

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type FlowOrchestrator struct {
	Publisher  EventPublisher
	StateStore StateStore
	Registry   FlowRegistry
}

func NewFlowOrchestrator(publisher EventPublisher, stateStore StateStore, registry FlowRegistry) *FlowOrchestrator {
	return &FlowOrchestrator{
		Publisher:  publisher,
		StateStore: stateStore,
		Registry:   registry,
	}
}

// RegisterFlow registers a flow definition with the orchestrator
func (o *FlowOrchestrator) RegisterFlow(flow Flow) error {
	return o.Registry.RegisterFlow(flow.ID(), flow)
}

// GetNextNode determines the next node to execute based on completed node and action
func (o *FlowOrchestrator) GetNextNode(flow Flow, currentNodeID, action string) (Node, bool) {
	return flow.GetNextNode(currentNodeID, action)
}

// StartFlow initiates flow execution with the flow definition from registry
func (o *FlowOrchestrator) StartFlow(flowType, flowID string, initialData map[string]interface{}) string {
	flowExecutionID := uuid.New().String()

	o.Publisher.Publish(
		fmt.Sprintf("flow.%s", flowType),
		FlowStartRequestedMessage{
			BaseMessage: BaseMessage{
				MessageType:     MessageTypeFlowStartRequested,
				FlowExecutionID: flowExecutionID,
				Timestamp:       time.Now(),
			},
			FlowType:          flowType,
			FlowDefinitionID:  flowID,
			InitialSharedData: initialData,
		},
	)

	return flowExecutionID
}

// For backward compatibility, these methods are kept but delegate to the new methods

// HandleFlowStartRequested is kept for backward compatibility
func (o *FlowOrchestrator) HandleFlowStartRequested(event FlowStartRequested) {
	log.Info().
		Str("flowExecutionID", event.FlowExecutionID).
		Str("flowDefinitionID", event.FlowDefinitionID).
		Msg("Handling flow start request (deprecated method)")

	// Get flow definition
	flowDef, err := o.Registry.GetFlowDefinition(event.FlowDefinitionID)
	if err != nil {
		log.Error().
			Err(err).
			Str("flowExecutionID", event.FlowExecutionID).
			Str("flowDefinitionID", event.FlowDefinitionID).
			Msg("Failed to get flow definition")

		o.Publisher.Publish("flow.failed", FlowFailedMessage{
			BaseMessage: BaseMessage{
				MessageType:     MessageTypeFlowFailed,
				FlowExecutionID: event.FlowExecutionID,
				Timestamp:       time.Now(),
			},
			FlowType:     "unknown", // We don't know the flow type here
			ErrorMessage: "Failed to get flow definition",
			ErrorDetails: err.Error(),
		})
		return
	}

	// Determine the flow type
	flowType := "default"
	if flowDef != nil {
		// Use the start node type as the flow type for backward compatibility
		flowType = flowDef.StartNodeType
	}

	// Forward to the new StartFlow method
	o.StartFlow(flowType, event.FlowDefinitionID, event.InitialSharedData)
}

// HandleNodePostCompleted is kept for backward compatibility
func (o *FlowOrchestrator) HandleNodePostCompleted(event NodePostCompleted) {
	log.Info().
		Str("flowExecutionID", event.FlowExecutionID).
		Str("nodeType", event.NodeType).
		Str("action", event.Action).
		Msg("Handling node post completion (deprecated method)")

	// Get flow definition for this execution
	_, err := o.Registry.GetFlowByExecutionID(event.FlowExecutionID)
	if err != nil {
		log.Error().
			Err(err).
			Str("flowExecutionID", event.FlowExecutionID).
			Str("nodeType", event.NodeType).
			Msg("Failed to get flow definition")

		o.Publisher.Publish("flow.failed", FlowFailedMessage{
			BaseMessage: BaseMessage{
				MessageType:     MessageTypeFlowFailed,
				FlowExecutionID: event.FlowExecutionID,
				Timestamp:       time.Now(),
			},
			FlowType:     "unknown", // We don't know the flow type here
			ErrorMessage: "Failed to get flow definition",
			ErrorDetails: err.Error(),
		})
		return
	}

	// Convert the old-style NodePostCompleted to new-style NodeCompletedMessage
	o.Publisher.Publish(
		"node.completed",
		NodeCompletedMessage{
			BaseMessage: BaseMessage{
				MessageType:     MessageTypeNodeCompleted,
				FlowExecutionID: event.FlowExecutionID,
				NodeExecutionID: event.NodeExecutionID,
				Timestamp:       time.Now(),
			},
			NodeType: event.NodeType,
			NodeID:   event.NodeExecutionID, // Best guess for the node ID
			Action:   event.Action,
			Result:   nil, // We don't have the result in the old event
		},
	)
}
