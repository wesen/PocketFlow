package event

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/The-Pocket/PocketFlow/go/semantic"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/rs/zerolog/log"
)

// FlowEventRouter handles events for a single flow type
// Each flow type gets its own router instance, can handle multiple executions
type FlowEventRouter struct {
	flowType    string
	flow        core.Flow
	stateStore  semantic.StateStore
	publisher   core.EventPublisher
	router      *message.Router
	subscriber  message.Subscriber
	nodeWorkers map[string]core.NodeWorker
	isRunning   bool
	mu          sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewFlowEventRouter creates a new event router for a specific flow type
func NewFlowEventRouter(
	flow core.Flow,
	stateStore semantic.StateStore,
	publisher core.EventPublisher,
	subscriber message.Subscriber,
	nodeWorkers map[string]core.NodeWorker,
	logger watermill.LoggerAdapter,
) (*FlowEventRouter, error) {
	if logger == nil {
		logger = watermill.NewStdLogger(false, false)
	}

	// Create a new router for this flow type
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create router for flow type %s: %w", flow.Type(), err)
	}

	// Set up middlewares
		SetupRouterMiddlewares(router, nil, logger)

	ctx, cancel := context.WithCancel(context.Background())

	fer := &FlowEventRouter{
		flowType:    flow.Type(),
		flow:        flow,
		stateStore:  stateStore,
		publisher:   publisher,
		router:      router,
		subscriber:  subscriber,
		nodeWorkers: make(map[string]core.NodeWorker),
		ctx:         ctx,
		cancel:      cancel,
	}

	// Copy node workers for this flow
	for nodeType, worker := range nodeWorkers {
		fer.nodeWorkers[nodeType] = worker
	}

	// Set up handlers for this flow
	if err := fer.setupHandlers(); err != nil {
		return nil, fmt.Errorf("failed to setup handlers for flow type %s: %w", flow.Type(), err)
	}

	return fer, nil
}

// setupHandlers configures all message handlers for this flow execution
func (fer *FlowEventRouter) setupHandlers() error {
	// Set up flow start handler for this flow type
	flowStartTopic := fmt.Sprintf("flow.%s", fer.flowType)
	flowStartHandlerName := fmt.Sprintf("flow_%s_start", fer.flowType)
	
	fer.router.AddNoPublisherHandler(
		flowStartHandlerName,
		flowStartTopic,
		fer.subscriber,
		fer.createFlowStartHandler(),
	)

	log.Debug().
		Str("handlerName", flowStartHandlerName).
		Str("topic", flowStartTopic).
		Str("flowType", fer.flowType).
		Msg("Registered flow start handler")

	// Set up node execution handlers for each node type used in this flow
	for nodeID, node := range fer.flow.Nodes() {
		nodeType := node.Type()
		worker, exists := fer.nodeWorkers[nodeType]
		if !exists {
			log.Warn().
				Str("nodeType", nodeType).
				Str("nodeID", nodeID).
				Str("flowType", fer.flowType).
				Msg("No worker found for node type")
			continue
		}

		// Subscribe to node execution requests for this flow type
		topic := fmt.Sprintf("node.%s.exec.requested", nodeType)
		handlerName := fmt.Sprintf("flow_%s_node_%s_exec", fer.flowType, nodeType)

		fer.router.AddNoPublisherHandler(
			handlerName,
			topic,
			fer.subscriber,
			fer.createNodeExecHandler(worker),
		)

		log.Debug().
			Str("handlerName", handlerName).
			Str("topic", topic).
			Str("nodeType", nodeType).
			Str("flowType", fer.flowType).
			Msg("Registered node execution handler for flow")
	}

	// Set up node completion handler for this flow
	nodeCompletedHandlerName := fmt.Sprintf("flow_%s_node_completed", fer.flowType)
	fer.router.AddNoPublisherHandler(
		nodeCompletedHandlerName,
		"node.completed",
		fer.subscriber,
		fer.createNodeCompletedHandler(),
	)

	log.Info().
		Str("flowType", fer.flowType).
		Int("nodeTypes", len(fer.nodeWorkers)).
		Msg("Set up flow event router handlers")

	return nil
}

// createFlowStartHandler creates a handler for flow start requests
func (fer *FlowEventRouter) createFlowStartHandler() message.NoPublishHandlerFunc {
	return func(msg *message.Message) error {
		var event core.FlowStartRequestedMessage
		if err := fer.unmarshalMessage(msg, &event); err != nil {
			return err
		}

		// Only handle if flow type matches
		if event.FlowType != fer.flowType {
			log.Debug().
				Str("flowType", fer.flowType).
				Str("requestedFlowType", event.FlowType).
				Str("flowExecutionID", event.FlowExecutionID).
				Msg("Flow router ignoring start request for different flow type")
			return nil
		}

		log.Debug().
			Str("flowType", fer.flowType).
			Str("flowExecutionID", event.FlowExecutionID).
			Str("flowDefinitionID", event.FlowDefinitionID).
			Interface("initialSharedData", event.InitialSharedData).
			Str("messageID", msg.UUID).
			Msg("Flow router processing flow start request")

		if err := fer.handleFlowStartRequested(event); err != nil {
			log.Error().
				Err(err).
				Str("flowType", fer.flowType).
				Str("flowExecutionID", event.FlowExecutionID).
				Str("messageID", msg.UUID).
				Msg("Error handling flow start request")
			return err
		}

		return nil
	}
}

// createNodeExecHandler creates a handler for node execution requests
func (fer *FlowEventRouter) createNodeExecHandler(worker core.NodeWorker) message.NoPublishHandlerFunc {
	return func(msg *message.Message) error {
		// Process all messages for this flow type
		var event core.NodeExecRequestedMessage
		if err := fer.unmarshalMessage(msg, &event); err != nil {
			return err
		}

		log.Debug().
			Str("flowType", fer.flowType).
			Str("flowExecutionID", event.FlowExecutionID).
			Str("nodeType", worker.NodeType()).
			Str("messageID", msg.UUID).
			Msg("Processing node execution request for flow")

		if err := worker.HandleMessage(msg); err != nil {
			log.Error().
				Err(err).
				Str("flowType", fer.flowType).
				Str("flowExecutionID", event.FlowExecutionID).
				Str("nodeType", worker.NodeType()).
				Str("messageID", msg.UUID).
				Msg("Error handling node execution for flow")
			return err
		}

		return nil
	}
}

// createNodeCompletedHandler creates a handler for node completion events
func (fer *FlowEventRouter) createNodeCompletedHandler() message.NoPublishHandlerFunc {
	return func(msg *message.Message) error {
		var event core.NodeCompletedMessage
		if err := fer.unmarshalMessage(msg, &event); err != nil {
			return err
		}

		log.Debug().
			Str("flowType", fer.flowType).
			Str("flowExecutionID", event.FlowExecutionID).
			Str("nodeID", event.NodeID).
			Bool("success", event.Success).
			Str("action", event.Action).
			Msg("Processing node completion for flow")

		if err := fer.handleNodeCompleted(event); err != nil {
			log.Error().
				Err(err).
				Str("flowType", fer.flowType).
				Str("flowExecutionID", event.FlowExecutionID).
				Str("nodeID", event.NodeID).
				Msg("Error handling node completion for flow")
				return err
		}

		return nil
	}
}

// handleNodeCompleted processes a node completion event for this flow
func (fer *FlowEventRouter) handleNodeCompleted(event core.NodeCompletedMessage) error {
	if event.Success {
		// Update shared state with node result
		if err := fer.stateStore.UpdateSharedData(event.FlowExecutionID, event.NodeID, event.Result); err != nil {
			return fmt.Errorf("failed to update shared data: %w", err)
		}

		// Publish progress event
		if err := fer.publisher.Publish("flow.progress", core.ProgressUpdateMessage{
			BaseMessage: core.BaseMessage{
				MessageType:     core.MessageTypeProgressUpdate,
				FlowExecutionID: event.FlowExecutionID,
				Timestamp:       time.Now(),
			},
			Status:   "node_completed",
			Progress: 0, // Calculate based on flow structure
			Message:  fmt.Sprintf("Node %s completed with action: %s", event.NodeID, event.Action),
		}); err != nil {
			log.Error().Err(err).Msg("Failed to publish progress update")
		}

		// Find next node based on the action
		nextNode, hasNext := fer.flow.GetNextNode(event.NodeID, event.Action)
		if hasNext {
			// Execute next node
			return fer.executeNode(event.FlowExecutionID, nextNode)
		}

		// No next node, flow is complete
		return fer.completeFlow(event.FlowExecutionID, event.Action)
	}

	// Node failed, fail the flow
	return fer.failFlow(event.FlowExecutionID, event.NodeID, event.ErrorMessage)
}

// handleFlowStartRequested processes a flow start request
func (fer *FlowEventRouter) handleFlowStartRequested(event core.FlowStartRequestedMessage) error {
	// Store the initial shared data
	for key, value := range event.InitialSharedData {
		if err := fer.stateStore.UpdateSharedData(event.FlowExecutionID, key, value); err != nil {
			log.Error().
				Err(err).
				Str("flowType", fer.flowType).
				Str("flowExecutionID", event.FlowExecutionID).
				Str("key", key).
				Msg("Flow router failed to store initial shared data")
			return fer.handleFlowInitializationError(event, err)
		}
	}

	log.Debug().
		Str("flowType", fer.flowType).
		Str("flowExecutionID", event.FlowExecutionID).
		Interface("initialSharedData", event.InitialSharedData).
		Msg("Flow router stored initial shared data")

	// Publish initial progress update
	if err := fer.publisher.Publish("flow.progress", core.ProgressUpdateMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     core.MessageTypeProgressUpdate,
			FlowExecutionID: event.FlowExecutionID,
			Timestamp:       time.Now(),
		},
		Status:   "flow_started",
		Progress: 0.0,
		Message:  fmt.Sprintf("Starting flow of type %s", fer.flowType),
	}); err != nil {
		log.Error().Err(err).Msg("Failed to publish initial progress update")
	}

	// Get the start node
	startNode := fer.flow.StartNode()
	if startNode == nil {
		log.Error().
			Str("flowType", fer.flowType).
			Str("flowExecutionID", event.FlowExecutionID).
			Str("flowDefinitionID", event.FlowDefinitionID).
			Msg("Flow router found flow has no start node")
		return fer.handleFlowInitializationError(event, fmt.Errorf("flow has no start node"))
	}

	log.Debug().
		Str("flowType", fer.flowType).
		Str("flowExecutionID", event.FlowExecutionID).
		Str("startNodeID", startNode.ID()).
		Str("startNodeType", startNode.Type()).
		Interface("startNodeParams", startNode.Params()).
		Msg("Flow router found start node")

	// Start the first node
	return fer.executeNode(event.FlowExecutionID, startNode)
}

// handleFlowInitializationError handles errors during flow initialization
func (fer *FlowEventRouter) handleFlowInitializationError(event core.FlowStartRequestedMessage, err error) error {
	// Log the error
	log.Error().
		Err(err).
		Str("flowExecutionID", event.FlowExecutionID).
		Str("flowType", fer.flowType).
		Msg("Failed to initialize flow")

	// Publish flow failed event
	return fer.failFlow(event.FlowExecutionID, "", err.Error())
}

// executeNode publishes a node execution request
func (fer *FlowEventRouter) executeNode(flowExecutionID string, node core.Node) error {
	topic := fmt.Sprintf("node.%s.exec.requested", node.Type())
	
	event := core.NodeExecRequestedMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     core.MessageTypeNodeExecRequested,
			FlowExecutionID: flowExecutionID,
		},
		NodeID:   node.ID(),
		NodeType: node.Type(),
		Params:   node.Params(),
	}

	if err := fer.publisher.Publish(topic, event); err != nil {
		return fmt.Errorf("failed to publish node execution request: %w", err)
	}

	log.Debug().
		Str("flowExecutionID", flowExecutionID).
		Str("nodeID", node.ID()).
		Str("nodeType", node.Type()).
		Str("topic", topic).
		Msg("Published node execution request")

	return nil
}

// completeFlow publishes a flow completion event
func (fer *FlowEventRouter) completeFlow(flowExecutionID, finalAction string) error {
	event := core.FlowCompletedMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     core.MessageTypeFlowCompleted,
			FlowExecutionID: flowExecutionID,
		},
		FinalAction: finalAction,
	}

	if err := fer.publisher.Publish("flow.completed", event); err != nil {
		return fmt.Errorf("failed to publish flow completion: %w", err)
	}

	// Publish final progress event
	if err := fer.publisher.Publish("flow.progress", core.ProgressUpdateMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     core.MessageTypeProgressUpdate,
			FlowExecutionID: flowExecutionID,
			Timestamp:       time.Now(),
		},
		Status:   "completed",
		Progress: 100.0,
		Message:  "Flow completed successfully",
	}); err != nil {
		log.Error().Err(err).Msg("Failed to publish final progress update")
	}

	log.Info().
		Str("flowExecutionID", flowExecutionID).
		Str("finalAction", finalAction).
		Msg("Flow completed successfully")

	return nil
}

// failFlow publishes a flow failure event
func (fer *FlowEventRouter) failFlow(flowExecutionID, nodeID, errorMessage string) error {
	event := core.FlowFailedMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     core.MessageTypeFlowFailed,
			FlowExecutionID: flowExecutionID,
		},
		NodeID:       nodeID,
		ErrorMessage: errorMessage,
	}

	if err := fer.publisher.Publish("flow.failed", event); err != nil {
		return fmt.Errorf("failed to publish flow failure: %w", err)
	}

	// Publish failure progress event
	if err := fer.publisher.Publish("flow.progress", core.ProgressUpdateMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     core.MessageTypeProgressUpdate,
			FlowExecutionID: flowExecutionID,
			Timestamp:       time.Now(),
		},
		Status:   "failed",
		Progress: 0,
		Message:  fmt.Sprintf("Flow failed at node %s: %s", nodeID, errorMessage),
	}); err != nil {
		log.Error().Err(err).Msg("Failed to publish failure progress update")
	}

	log.Error().
		Str("flowExecutionID", flowExecutionID).
		Str("nodeID", nodeID).
		Str("error", errorMessage).
		Msg("Flow failed")

	return nil
}

// Start starts the flow event router
func (fer *FlowEventRouter) Start() error {
	fer.mu.Lock()
	defer fer.mu.Unlock()

	if fer.isRunning {
		return nil
	}

	go func() {
		if err := fer.router.Run(fer.ctx); err != nil && err != context.Canceled {
			log.Error().
				Err(err).
				Str("flowType", fer.flowType).
				Msg("Flow router error")
		}
	}()

	// Wait for router to start
	<-fer.router.Running()
	fer.isRunning = true

	log.Info().
		Str("flowType", fer.flowType).
		Msg("Flow event router started")

	return nil
}

// Stop stops the flow event router
func (fer *FlowEventRouter) Stop() error {
	fer.mu.Lock()
	defer fer.mu.Unlock()

	if !fer.isRunning {
		return nil
	}

	fer.cancel()
	if err := fer.router.Close(); err != nil {
		log.Error().
			Err(err).
			Str("flowType", fer.flowType).
			Msg("Error closing flow router")
	}

	fer.isRunning = false

	log.Info().
		Str("flowType", fer.flowType).
		Msg("Flow event router stopped")

	return nil
}

// IsRunning returns true if the router is running
func (fer *FlowEventRouter) IsRunning() bool {
	fer.mu.RLock()
	defer fer.mu.RUnlock()
	return fer.isRunning
}

// unmarshalMessage unmarshals a Watermill message payload into a struct
func (fer *FlowEventRouter) unmarshalMessage(msg *message.Message, event interface{}) error {
	return json.Unmarshal(msg.Payload, event)
}