package impl

import (
	"fmt"
	"time"

	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/google/uuid"
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
