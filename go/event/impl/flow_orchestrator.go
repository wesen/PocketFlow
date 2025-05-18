package impl

import (
	"fmt"
	"time"

	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type FlowOrchestrator struct {
	Publisher  core.EventPublisher
	StateStore core.StateStore
	Registry   core.FlowRegistry
}

func NewFlowOrchestrator(publisher core.EventPublisher, stateStore core.StateStore, registry core.FlowRegistry) *FlowOrchestrator {
	return &FlowOrchestrator{
		Publisher:  publisher,
		StateStore: stateStore,
		Registry:   registry,
	}
}

// RegisterFlow registers a flow definition with the orchestrator
func (o *FlowOrchestrator) RegisterFlow(flow core.Flow) error {
	return o.Registry.RegisterFlow(flow.ID(), flow)
}

// GetNextNode determines the next node to execute based on completed node and action
func (o *FlowOrchestrator) GetNextNode(flow core.Flow, currentNodeID, action string) (core.Node, bool) {
	return flow.GetNextNode(currentNodeID, action)
}

// StartFlow initiates flow execution with the flow definition from registry
func (o *FlowOrchestrator) StartFlow(flowType, flowID string, initialData map[string]interface{}) string {
	flowExecutionID := uuid.New().String()

	o.Publisher.Publish(
		fmt.Sprintf("flow.%s", flowType),
		core.FlowStartRequestedMessage{
			BaseMessage: core.BaseMessage{
				MessageType:     core.MessageTypeFlowStartRequested,
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

// Legacy types for backward compatibility
type FlowStartRequested struct {
	FlowExecutionID  string
	FlowDefinitionID string
	InitialSharedData map[string]interface{}
}

type NodePostCompleted struct {
	FlowExecutionID string
	NodeExecutionID string
	NodeType        string
	Action          string
}

// HandleFlowStartRequested is kept for backward compatibility
func (o *FlowOrchestrator) HandleFlowStartRequested(event FlowStartRequested) {
	log.Info().
		Str("flowExecutionID", event.FlowExecutionID).
		Str("flowDefinitionID", event.FlowDefinitionID).
		Msg("Handling flow start request (deprecated method)")

	// Forward to the new StartFlow method
	o.StartFlow("default", event.FlowDefinitionID, event.InitialSharedData)
}

// HandleNodePostCompleted is kept for backward compatibility
func (o *FlowOrchestrator) HandleNodePostCompleted(event NodePostCompleted) {
	log.Info().
		Str("flowExecutionID", event.FlowExecutionID).
		Str("nodeType", event.NodeType).
		Str("action", event.Action).
		Msg("Handling node post completion (deprecated method)")

	// Convert the old-style NodePostCompleted to new-style NodeCompletedMessage
	o.Publisher.Publish(
		"node.completed",
		core.NodeCompletedMessage{
			BaseMessage: core.BaseMessage{
				MessageType:     core.MessageTypeNodeCompleted,
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