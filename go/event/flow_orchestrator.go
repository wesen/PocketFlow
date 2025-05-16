package event

import (
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"time"
)

type FlowOrchestrator struct {
	Publisher    EventPublisher
	StateStore   StateStore
	FlowRegistry FlowRegistry
}

func NewFlowOrchestrator(publisher EventPublisher, stateStore StateStore, registry FlowRegistry) *FlowOrchestrator {
	return &FlowOrchestrator{
		Publisher:    publisher,
		StateStore:   stateStore,
		FlowRegistry: registry,
	}
}

// Generate a new UUID string
func generateUUID() string {
	return uuid.New().String()
}

// Handle flow start requests
func (o *FlowOrchestrator) HandleFlowStartRequested(event FlowStartRequested) {
	log.Info().
		Str("flowExecutionID", event.FlowExecutionID).
		Str("flowDefinitionID", event.FlowDefinitionID).
		Str("correlationID", event.CorrelationID).
		Msg("Handling flow start request")

	// Store initial shared data
	o.StateStore.StoreSharedData(event.FlowExecutionID, event.InitialSharedData)
	log.Debug().
		Str("flowExecutionID", event.FlowExecutionID).
		Interface("sharedData", event.InitialSharedData).
		Msg("Stored initial shared data")
	
	// Get flow definition
	flowDef, err := o.FlowRegistry.GetFlowDefinition(event.FlowDefinitionID)
	if err != nil {
		log.Error().
			Err(err).
			Str("flowExecutionID", event.FlowExecutionID).
			Str("flowDefinitionID", event.FlowDefinitionID).
			Msg("Failed to get flow definition")

		o.Publisher.Publish("flow.failed", FlowFailed{
			BaseEvent: BaseEvent{
				EventID:         generateUUID(),
				FlowExecutionID: event.FlowExecutionID,
				Timestamp:       time.Now(),
				CorrelationID:   event.CorrelationID,
			},
			ErrorMessage:   "Failed to get flow definition",
			ErrorDetails:   err.Error(),
			FailedNodeType: "",
		})
		return
	}
	
	// Store the mapping between execution ID and definition ID
	err = o.StateStore.StoreFlowExecution(event.FlowExecutionID, event.FlowDefinitionID)
	if err != nil {
		log.Error().
			Err(err).
			Str("flowExecutionID", event.FlowExecutionID).
			Str("flowDefinitionID", event.FlowDefinitionID).
			Msg("Failed to store flow execution mapping")

		o.Publisher.Publish("flow.failed", FlowFailed{
			BaseEvent: BaseEvent{
				EventID:         generateUUID(),
				FlowExecutionID: event.FlowExecutionID,
				Timestamp:       time.Now(),
				CorrelationID:   event.CorrelationID,
			},
			ErrorMessage:   "Failed to store flow execution mapping",
			ErrorDetails:   err.Error(),
			FailedNodeType: "",
		})
		return
	}
	
	// Create node execution ID for start node
	nodeExecID := generateUUID()
	
	log.Info().
		Str("flowExecutionID", event.FlowExecutionID).
		Str("nodeExecutionID", nodeExecID).
		Str("nodeType", flowDef.StartNodeType).
		Msg("Requesting start node preparation")

	// Request start node prep
	o.Publisher.Publish("node."+flowDef.StartNodeType+".prep.requested", NodePrepRequested{
		BaseEvent: BaseEvent{
			EventID:         generateUUID(),
			FlowExecutionID: event.FlowExecutionID,
			NodeExecutionID: nodeExecID,
			NodeType:        flowDef.StartNodeType,
			Timestamp:       time.Now(),
			CorrelationID:   event.CorrelationID,
		},
		NodeParams: flowDef.StartNodeParams,
	})
}

// Handle node post completion to determine next node
func (o *FlowOrchestrator) HandleNodePostCompleted(event NodePostCompleted) {
	log.Info().
		Str("flowExecutionID", event.FlowExecutionID).
		Str("nodeType", event.NodeType).
		Str("action", event.Action).
		Msg("Handling node post completion")

	// Get flow definition for this execution
	flowDef, err := o.FlowRegistry.GetFlowDefinitionByExecutionID(event.FlowExecutionID)
	if err != nil {
		log.Error().
			Err(err).
			Str("flowExecutionID", event.FlowExecutionID).
			Str("nodeType", event.NodeType).
			Msg("Failed to get flow definition")

		o.Publisher.Publish("flow.failed", FlowFailed{
			BaseEvent: BaseEvent{
				EventID:         generateUUID(),
				FlowExecutionID: event.FlowExecutionID,
				Timestamp:       time.Now(),
				CorrelationID:   event.CorrelationID,
			},
			ErrorMessage:   "Failed to get flow definition",
			ErrorDetails:   err.Error(),
			FailedNodeType: event.NodeType,
		})
		return
	}
	
	// Find next node based on current node type and action
	nextNode, exists := flowDef.GetNextNode(event.NodeType, event.Action)
	
	if !exists {
		log.Info().
			Str("flowExecutionID", event.FlowExecutionID).
			Str("nodeType", event.NodeType).
			Str("action", event.Action).
			Msg("Flow completed - no next node found")

		// Flow is complete, no next node
		o.Publisher.Publish("flow.completed", FlowCompleted{
			BaseEvent: BaseEvent{
				EventID:         generateUUID(),
				FlowExecutionID: event.FlowExecutionID,
				NodeExecutionID: "",
				NodeType:        "",
				Timestamp:       time.Now(),
				CorrelationID:   event.CorrelationID,
			},
			FinalAction:     event.Action,
			ExecutionTimeMs: 0, // Could calculate this if needed
		})
		return
	}
	
	// Create new node execution ID
	nextNodeExecID := generateUUID()

	log.Info().
		Str("flowExecutionID", event.FlowExecutionID).
		Str("fromNodeType", event.NodeType).
		Str("toNodeType", nextNode.Type).
		Str("action", event.Action).
		Str("nextNodeExecutionID", nextNodeExecID).
		Msg("Transitioning to next node")
	
	// Request transition to next node
	o.Publisher.Publish("node.transition.requested", NodeTransitionRequested{
		BaseEvent: BaseEvent{
			EventID:         generateUUID(),
			FlowExecutionID: event.FlowExecutionID,
			NodeExecutionID: nextNodeExecID,
			NodeType:        nextNode.Type,
			Timestamp:       time.Now(),
			CorrelationID:   event.CorrelationID,
		},
		FromNodeType: event.NodeType,
		Action:       event.Action,
		ToNodeType:   nextNode.Type,
		ToNodeID:     nextNode.ID,
	})
	
	log.Debug().
		Str("flowExecutionID", event.FlowExecutionID).
		Str("nodeType", nextNode.Type).
		Str("nodeExecutionID", nextNodeExecID).
		Interface("nodeParams", nextNode.Params).
		Msg("Requesting next node preparation")

	// Request next node prep
	o.Publisher.Publish("node."+nextNode.Type+".prep.requested", NodePrepRequested{
		BaseEvent: BaseEvent{
			EventID:         generateUUID(),
			FlowExecutionID: event.FlowExecutionID,
			NodeExecutionID: nextNodeExecID,
			NodeType:        nextNode.Type,
			Timestamp:       time.Now(),
			CorrelationID:   event.CorrelationID,
		},
		NodeParams: nextNode.Params,
	})
}