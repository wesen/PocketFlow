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
	"github.com/google/uuid"
	"github.com/rs/zerolog"
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
	logger      zerolog.Logger
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
	flowLogger := log.With().Str("flowType", flow.Type()).Logger()
	
	if logger == nil {
		logger = watermill.NewStdLogger(false, false)
	}

	flowLogger.Debug().Msg("Creating new flow event router")

	// Create a new router for this flow type
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		flowLogger.Error().Err(err).Msg("Failed to create router for flow type")
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
		logger:      flowLogger,
	}

	// Copy node workers for this flow
	for nodeType, worker := range nodeWorkers {
		fer.nodeWorkers[nodeType] = worker
	}

	flowLogger.Debug().Int("nodeWorkerCount", len(fer.nodeWorkers)).Msg("Copied node workers for flow")

	// Set up handlers for this flow
	if err := fer.setupHandlers(); err != nil {
		flowLogger.Error().Err(err).Msg("Failed to setup handlers for flow type")
		return nil, fmt.Errorf("failed to setup handlers for flow type %s: %w", flow.Type(), err)
	}

	flowLogger.Info().Msg("Successfully created flow event router")
	return fer, nil
}

// setupHandlers configures all message handlers for this flow execution
func (fer *FlowEventRouter) setupHandlers() error {
	fer.logger.Debug().Msg("Setting up handlers for flow")

	// Set up flow start handler for this flow type
	flowStartTopic := fmt.Sprintf("flow.%s", fer.flowType)
	flowStartHandlerName := fmt.Sprintf("flow_%s_start", fer.flowType)
	
	fer.router.AddNoPublisherHandler(
		flowStartHandlerName,
		flowStartTopic,
		fer.subscriber,
		fer.createFlowStartHandler(),
	)

	handlerLogger := fer.logger.With().
		Str("handlerName", flowStartHandlerName).
		Str("topic", flowStartTopic).
		Logger()
	handlerLogger.Debug().Msg("Registered flow start handler")

	// Set up node execution handlers for each node type used in this flow
	nodeHandlerCount := 0
	for nodeID, node := range fer.flow.Nodes() {
		nodeType := node.Type()
		nodeLogger := fer.logger.With().
			Str("nodeType", nodeType).
			Str("nodeID", nodeID).
			Logger()

		worker, exists := fer.nodeWorkers[nodeType]
		if !exists {
			nodeLogger.Warn().Msg("No worker found for node type")
			continue
		}

		// Subscribe to flow-scoped node execution requests 
		topic := fmt.Sprintf("%s.node.%s", fer.flowType, nodeType)
		handlerName := fmt.Sprintf("flow_%s_node_%s_exec", fer.flowType, nodeType)

		fer.router.AddNoPublisherHandler(
			handlerName,
			topic,
			fer.subscriber,
			fer.createNodeExecHandler(worker),
		)

		nodeLogger.Debug().
			Str("handlerName", handlerName).
			Str("topic", topic).
			Msg("Registered node execution handler for flow")
		
		nodeHandlerCount++
	}

	// Set up flow-scoped node completion handler
	nodeCompletedTopic := fmt.Sprintf("%s.node.completed", fer.flowType)
	nodeCompletedHandlerName := fmt.Sprintf("flow_%s_node_completed", fer.flowType)
	fer.router.AddNoPublisherHandler(
		nodeCompletedHandlerName,
		nodeCompletedTopic,
		fer.subscriber,
		fer.createNodeCompletedHandler(),
	)

	fer.logger.Debug().
		Str("nodeCompletedTopic", nodeCompletedTopic).
		Str("nodeCompletedHandlerName", nodeCompletedHandlerName).
		Msg("Registered node completion handler")

	fer.logger.Info().
		Int("nodeHandlerCount", nodeHandlerCount).
		Msg("Set up flow event router handlers")

	return nil
}

// createFlowStartHandler creates a handler for flow start requests
func (fer *FlowEventRouter) createFlowStartHandler() message.NoPublishHandlerFunc {
	return func(msg *message.Message) error {
		msgLogger := fer.logger.With().Str("messageID", msg.UUID).Logger()
		
		var event core.FlowStartRequestedMessage
		if err := fer.unmarshalMessage(msg, &event); err != nil {
			msgLogger.Error().Err(err).Msg("Failed to unmarshal flow start message")
			return err
		}

		execLogger := msgLogger.With().
			Str("requestedFlowType", event.FlowType).
			Str("flowExecutionID", event.FlowExecutionID).
			Str("flowDefinitionID", event.FlowDefinitionID).
			Logger()

		// Only handle if flow type matches
		if event.FlowType != fer.flowType {
			execLogger.Debug().Msg("Flow router ignoring start request for different flow type")
			return nil
		}

		execLogger.Debug().Interface("initialSharedData", event.InitialSharedData).Msg("Flow router processing flow start request")

		if err := fer.handleFlowStartRequested(event); err != nil {
			execLogger.Error().Err(err).Msg("Error handling flow start request")
			return err
		}

		execLogger.Info().Msg("Successfully processed flow start request")
		return nil
	}
}

// createNodeExecHandler creates a handler for node execution requests
func (fer *FlowEventRouter) createNodeExecHandler(worker core.NodeWorker) message.NoPublishHandlerFunc {
	return func(msg *message.Message) error {
		msgLogger := fer.logger.With().
			Str("messageID", msg.UUID).
			Str("workerNodeType", worker.NodeType()).
			Logger()

		// Process all messages for this flow type
		var event core.ExecRequestedMessage
		if err := fer.unmarshalMessage(msg, &event); err != nil {
			msgLogger.Error().Err(err).Msg("Failed to unmarshal node exec message")
			return err
		}

		execLogger := msgLogger.With().
			Str("flowExecutionID", event.FlowExecutionID).
			Str("nodeExecutionID", event.NodeExecutionID).
			Str("nodeID", event.NodeID).
			Logger()

		execLogger.Debug().Msg("Processing node execution request for flow")

		if err := worker.HandleMessage(msg); err != nil {
			execLogger.Error().Err(err).Msg("Error handling node execution for flow")
			return err
		}

		execLogger.Debug().Msg("Successfully processed node execution request")
		return nil
	}
}

// createNodeCompletedHandler creates a handler for node completion events
func (fer *FlowEventRouter) createNodeCompletedHandler() message.NoPublishHandlerFunc {
	return func(msg *message.Message) error {
		msgLogger := fer.logger.With().Str("messageID", msg.UUID).Logger()
		
		var event core.NodeCompletedMessage
		if err := fer.unmarshalMessage(msg, &event); err != nil {
			msgLogger.Error().Err(err).Msg("Failed to unmarshal node completed message")
			return err
		}

		execLogger := msgLogger.With().
			Str("flowExecutionID", event.FlowExecutionID).
			Str("nodeExecutionID", event.NodeExecutionID).
			Str("nodeID", event.NodeID).
			Str("nodeType", event.NodeType).
			Bool("success", event.Success).
			Str("action", event.Action).
			Logger()

		execLogger.Debug().Msg("Processing node completion for flow")

		if err := fer.handleNodeCompleted(event); err != nil {
			execLogger.Error().Err(err).Msg("Error handling node completion for flow")
			return err
		}

		execLogger.Debug().Msg("Successfully processed node completion")
		return nil
	}
}

// handleNodeCompleted processes a node completion event for this flow
func (fer *FlowEventRouter) handleNodeCompleted(event core.NodeCompletedMessage) error {
	execLogger := fer.logger.With().
		Str("flowExecutionID", event.FlowExecutionID).
		Str("nodeID", event.NodeID).
		Str("nodeType", event.NodeType).
		Bool("success", event.Success).
		Logger()

	if event.Success {
		execLogger.Debug().Str("action", event.Action).Msg("Processing successful node completion")

		// Update shared state with node result
		if err := fer.stateStore.UpdateSharedData(event.FlowExecutionID, event.NodeID, event.Result); err != nil {
			execLogger.Error().Err(err).Msg("Failed to update shared data")
			return fmt.Errorf("failed to update shared data: %w", err)
		}

		execLogger.Debug().Msg("Updated shared data with node result")

		// Publish progress event
		if err := fer.publisher.Publish("flow.progress", core.ProgressUpdateMessage{
			BaseMessage: core.BaseMessage{
				MessageType:     core.MessageTypeProgressUpdate,
				FlowExecutionID: event.FlowExecutionID,
				Timestamp:       time.Now(),
				FlowType:        fer.flowType,
			},
			Status:   "node_completed",
			Progress: 0, // Calculate based on flow structure
			Message:  fmt.Sprintf("Node %s completed with action: %s", event.NodeID, event.Action),
		}); err != nil {
			execLogger.Error().Err(err).Msg("Failed to publish progress update")
		}

		// Find next node based on the action
		nextNode, hasNext := fer.flow.GetNextNode(event.NodeID, event.Action)
		if hasNext {
			execLogger.Debug().
				Str("nextNodeID", nextNode.ID()).
				Str("nextNodeType", nextNode.Type()).
				Str("action", event.Action).
				Msg("Found next node, executing")
			// Execute next node
			return fer.executeNode(event.FlowExecutionID, nextNode)
		}

		execLogger.Info().Str("finalAction", event.Action).Msg("No next node found, completing flow")
		// No next node, flow is complete
		return fer.completeFlow(event.FlowExecutionID, event.Action)
	}

	// Node failed, fail the flow
	execLogger.Error().Str("errorMessage", event.ErrorMessage).Msg("Node failed, failing flow")
	return fer.failFlow(event.FlowExecutionID, event.NodeID, event.ErrorMessage)
}

// handleFlowStartRequested processes a flow start request
func (fer *FlowEventRouter) handleFlowStartRequested(event core.FlowStartRequestedMessage) error {
	execLogger := fer.logger.With().
		Str("flowExecutionID", event.FlowExecutionID).
		Str("flowDefinitionID", event.FlowDefinitionID).
		Logger()

	execLogger.Debug().Interface("initialSharedData", event.InitialSharedData).Msg("Processing flow start request")

	// Store the initial shared data
	for key, value := range event.InitialSharedData {
		if err := fer.stateStore.UpdateSharedData(event.FlowExecutionID, key, value); err != nil {
			execLogger.Error().
				Err(err).
				Str("key", key).
				Msg("Flow router failed to store initial shared data")
			return fer.handleFlowInitializationError(event, err)
		}
	}

	execLogger.Debug().Int("dataKeysStored", len(event.InitialSharedData)).Msg("Flow router stored initial shared data")

	// Publish initial progress update
	if err := fer.publisher.Publish("flow.progress", core.ProgressUpdateMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     core.MessageTypeProgressUpdate,
			FlowExecutionID: event.FlowExecutionID,
			Timestamp:       time.Now(),
			FlowType:        fer.flowType,
		},
		Status:   "flow_started",
		Progress: 0.0,
		Message:  fmt.Sprintf("Starting flow of type %s", fer.flowType),
	}); err != nil {
		execLogger.Error().Err(err).Msg("Failed to publish initial progress update")
	}

	// Get the start node
	startNode := fer.flow.StartNode()
	if startNode == nil {
		execLogger.Error().Msg("Flow router found flow has no start node")
		return fer.handleFlowInitializationError(event, fmt.Errorf("flow has no start node"))
	}

	execLogger.Debug().
		Str("startNodeID", startNode.ID()).
		Str("startNodeType", startNode.Type()).
		Interface("startNodeParams", startNode.Params()).
		Msg("Flow router found start node")

	// Start the first node
	return fer.executeNode(event.FlowExecutionID, startNode)
}

// handleFlowInitializationError handles errors during flow initialization
func (fer *FlowEventRouter) handleFlowInitializationError(event core.FlowStartRequestedMessage, err error) error {
	execLogger := fer.logger.With().
		Str("flowExecutionID", event.FlowExecutionID).
		Logger()

	// Log the error
	execLogger.Error().Err(err).Msg("Failed to initialize flow")

	// Publish flow failed event
	return fer.failFlow(event.FlowExecutionID, "", err.Error())
}

// executeNode publishes a node execution request
func (fer *FlowEventRouter) executeNode(flowExecutionID string, node core.Node) error {
	execLogger := fer.logger.With().
		Str("flowExecutionID", flowExecutionID).
		Str("nodeID", node.ID()).
		Str("nodeType", node.Type()).
		Logger()

	topic := fmt.Sprintf("%s.node.%s", fer.flowType, node.Type())
	nodeExecutionID := uuid.New().String()

	execLogger.Debug().
		Str("nodeExecutionID", nodeExecutionID).
		Str("topic", topic).
		Interface("nodeParams", node.Params()).
		Msg("Preparing to execute node")

	event := core.ExecRequestedMessage{
		BaseMessage: core.BaseMessage{
			NodeExecutionID: nodeExecutionID,
			MessageType:     core.MessageTypeExecRequested,
			FlowExecutionID: flowExecutionID,
			FlowType:        fer.flowType,	
			Timestamp:       time.Now(),
		},
		NodeID:   node.ID(),
		NodeType: node.Type(),
		Params:   node.Params(),
	}

	if err := fer.publisher.Publish(topic, event); err != nil {
		execLogger.Error().
			Err(err).
			Str("nodeExecutionID", nodeExecutionID).
			Str("topic", topic).
			Msg("Failed to publish node execution request")
		return fmt.Errorf("failed to publish node execution request: %w", err)
	}

	execLogger.Debug().
		Str("nodeExecutionID", nodeExecutionID).
		Str("topic", topic).
		Msg("Published node execution request")

	return nil
}

// completeFlow publishes a flow completion event
func (fer *FlowEventRouter) completeFlow(flowExecutionID, finalAction string) error {
	execLogger := fer.logger.With().
		Str("flowExecutionID", flowExecutionID).
		Str("finalAction", finalAction).
		Logger()

	execLogger.Debug().Msg("Completing flow")

	event := core.FlowCompletedMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     core.MessageTypeFlowCompleted,
			FlowExecutionID: flowExecutionID,
			FlowType:        fer.flowType,
			Timestamp:       time.Now(),
		},
		FinalAction: finalAction,
	}

	if err := fer.publisher.Publish("flow.completed", event); err != nil {
		execLogger.Error().Err(err).Msg("Failed to publish flow completion")
		return fmt.Errorf("failed to publish flow completion: %w", err)
	}

	// Publish final progress event
	if err := fer.publisher.Publish("flow.progress", core.ProgressUpdateMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     core.MessageTypeProgressUpdate,
			FlowExecutionID: flowExecutionID,
			Timestamp:       time.Now(),
			FlowType:        fer.flowType,
		},
		Status:   "completed",
		Progress: 100.0,
		Message:  "Flow completed successfully",
	}); err != nil {
		execLogger.Error().Err(err).Msg("Failed to publish final progress update")
	}

	execLogger.Info().Msg("Flow completed successfully")
	return nil
}

// failFlow publishes a flow failure event
func (fer *FlowEventRouter) failFlow(flowExecutionID, nodeID, errorMessage string) error {
	execLogger := fer.logger.With().
		Str("flowExecutionID", flowExecutionID).
		Str("nodeID", nodeID).
		Str("errorMessage", errorMessage).
		Logger()

	execLogger.Debug().Msg("Failing flow")

	event := core.FlowFailedMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     core.MessageTypeFlowFailed,
			FlowExecutionID: flowExecutionID,
			FlowType:        fer.flowType,
			Timestamp:       time.Now(),
		},
		NodeID:       nodeID,
		ErrorMessage: errorMessage,
	}

	if err := fer.publisher.Publish("flow.failed", event); err != nil {
		execLogger.Error().Err(err).Msg("Failed to publish flow failure")
		return fmt.Errorf("failed to publish flow failure: %w", err)
	}

	// Publish failure progress event
	if err := fer.publisher.Publish("flow.progress", core.ProgressUpdateMessage{
		BaseMessage: core.BaseMessage{
			MessageType:     core.MessageTypeProgressUpdate,
			FlowExecutionID: flowExecutionID,
			Timestamp:       time.Now(),
			FlowType:        fer.flowType,	
		},
		Status:   "failed",
		Progress: 0,
		Message:  fmt.Sprintf("Flow failed at node %s: %s", nodeID, errorMessage),
	}); err != nil {
		execLogger.Error().Err(err).Msg("Failed to publish failure progress update")
	}

	execLogger.Error().Msg("Flow failed")
	return nil
}

// Start starts the flow event router
func (fer *FlowEventRouter) Start() error {
	fer.mu.Lock()
	defer fer.mu.Unlock()

	if fer.isRunning {
		fer.logger.Debug().Msg("Flow event router already running")
		return nil
	}

	fer.logger.Debug().Msg("Starting flow event router")

	go func() {
		if err := fer.router.Run(fer.ctx); err != nil && err != context.Canceled {
			fer.logger.Error().Err(err).Msg("Flow router error")
		}
	}()

	// Wait for router to start
	<-fer.router.Running()
	fer.isRunning = true

	fer.logger.Info().Msg("Flow event router started")
	return nil
}

// Stop stops the flow event router
func (fer *FlowEventRouter) Stop() error {
	fer.mu.Lock()
	defer fer.mu.Unlock()

	if !fer.isRunning {
		fer.logger.Debug().Msg("Flow event router already stopped")
		return nil
	}

	fer.logger.Debug().Msg("Stopping flow event router")

	fer.cancel()
	if err := fer.router.Close(); err != nil {
		fer.logger.Error().Err(err).Msg("Error closing flow router")
	}

	fer.isRunning = false
	fer.logger.Info().Msg("Flow event router stopped")
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