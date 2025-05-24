package node

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

type SimpleNode struct {
	id       string
	nodeType string
	params   core.NodeParams
}

var _ core.Node = &SimpleNode{}

func (n *SimpleNode) Name() string {
	return n.nodeType
}

func (n *SimpleNode) Params() core.NodeParams {
	return n.params
}

func (n *SimpleNode) ID() string {
	return n.id
}

func (n *SimpleNode) Type() string {
	return n.nodeType
}

// SimpleNodeWorker is a wrapper that implements NodeWorker using a SimpleNodeHandler
type SimpleNodeWorker struct {
	nodeType  string
	handler   core.SimpleNodeHandler
	publisher core.EventPublisher
	store     semantic.StateStore
}

var _ core.NodeWorker = &SimpleNodeWorker{}

// NewSimpleNodeWorker creates a new SimpleNodeWorker
func NewSimpleNodeWorker(
	nodeType string,
	handler core.SimpleNodeHandler,
	publisher core.EventPublisher,
	store semantic.StateStore,
) *SimpleNodeWorker {
	return &SimpleNodeWorker{
		nodeType:  nodeType,
		handler:   handler,
		publisher: publisher,
		store:     store,
	}
}

// NodeType returns the type of node this worker handles
func (n *SimpleNodeWorker) NodeType() string {
	return n.nodeType
}

// SupportedMessageTypes returns the list of message types this worker can handle
func (n *SimpleNodeWorker) SupportedMessageTypes() []string {
	return []string{core.MessageTypeExecRequested}
}

// HandleMessage handles a message
func (n *SimpleNodeWorker) HandleMessage(msgObj interface{}) error {
	nodeLogger := log.With().Str("nodeType", n.nodeType).Logger()
	
	msg, ok := msgObj.(*message.Message)
	if !ok {
		nodeLogger.Error().Msg("NodeWorker received invalid message type")
		return fmt.Errorf("invalid message type")
	}

	msgLogger := nodeLogger.With().Str("messageID", msg.UUID).Logger()

	var base core.BaseMessage
	if err := json.Unmarshal(msg.Payload, &base); err != nil {
		msgLogger.Error().Err(err).Msg("NodeWorker failed to unmarshal base message")
		return err
	}

	execLogger := msgLogger.With().
		Str("messageType", base.MessageType).
		Str("flowExecutionID", base.FlowExecutionID).
		Str("nodeExecutionID", base.NodeExecutionID).
		Logger()

	execLogger.Debug().Msg("NodeWorker handling message")

	switch base.MessageType {
	case core.MessageTypeExecRequested:
		var execReq core.ExecRequestedMessage
		if err := json.Unmarshal(msg.Payload, &execReq); err != nil {
			msgLogger.Error().Err(err).Msg("NodeWorker failed to unmarshal exec request message")
			return err
		}
		return n.handleExecRequested(execReq)
	default:
		execLogger.Warn().Msg("NodeWorker received unsupported message type")
		return fmt.Errorf("unsupported message type: %s", base.MessageType)
	}
}

func (n *SimpleNodeWorker) handleExecError(event core.ExecRequestedMessage, err error, retryCount int, willRetry bool) error {
	execLogger := log.With().
		Str("nodeType", n.nodeType).
		Str("flowExecutionID", event.FlowExecutionID).
		Str("nodeExecutionID", event.NodeExecutionID).
		Logger()

	// Log the error
	execLogger.Error().Err(err).Msg("Node execution failed")

	// Publish error message to node.failed topic instead of node's own topic
	n.publisher.Publish(
		"node.failed",
		core.ExecFailedMessage{
			BaseMessage: core.BaseMessage{
				MessageType:     core.MessageTypeExecFailed,
				FlowType:        event.FlowType,
				FlowExecutionID: event.FlowExecutionID,
				NodeExecutionID: event.NodeExecutionID,
				Timestamp:       time.Now(),
			},
			NodeType:     n.nodeType,
			NodeID:       event.NodeID,
			ErrorMessage: err.Error(),
			RetryCount:   retryCount,
			WillRetry:    willRetry,
		},
	)

	// Publish error progress update
	n.publishProgressUpdate(event.FlowType, event.FlowExecutionID, event.NodeExecutionID, "node_failed", 0.0,
		fmt.Sprintf("%s node failed: %s", n.nodeType, err.Error()))

	return nil
}

func (n *SimpleNodeWorker) handleExecRequested(event core.ExecRequestedMessage) error {
	execLogger := log.With().
		Str("nodeType", n.nodeType).
		Str("flowExecutionID", event.FlowExecutionID).
		Str("nodeExecutionID", event.NodeExecutionID).
		Str("nodeID", event.NodeID).
		Str("flowType", event.FlowType).
		Logger()

	execLogger.Debug().Interface("params", event.Params).Msg("NodeWorker starting node execution request processing")

	// Publish starting progress update
	n.publishProgressUpdate(event.FlowType, event.FlowExecutionID, event.NodeExecutionID, "node_started", 0.0,
		fmt.Sprintf("%s node started processing", n.nodeType))

	// Get shared data from the store
	sharedData, err := n.store.GetSharedData(event.FlowExecutionID)
	if err != nil {
		execLogger.Error().Err(err).Msg("NodeWorker failed to get shared data")
		return n.handleNodeExecError(event, err, 0, false)
	}

	execLogger.Debug().Interface("sharedData", sharedData).Msg("NodeWorker retrieved shared data")

	// Create semantic node context
	ctx := semantic.NodeContext{
		FlowExecutionID: event.FlowExecutionID,
		NodeID:          event.NodeID,
		NodeType:        event.NodeType,
		Params:          event.Params,
		SemanticData:    semantic.NewSemanticDataAccessor(n.store, event.FlowExecutionID),
	}

	// Publish progress for prep phase
	n.publishProgressUpdate(event.FlowType, event.FlowExecutionID, event.NodeExecutionID, "node_prep", 0.25,
		fmt.Sprintf("%s node preparation phase", n.nodeType))

	// Prep phase
	phaseLogger := execLogger.With().Str("phase", "prep").Logger()
	phaseLogger.Debug().Msg("NodeWorker starting prep phase")

	prepResult, err := n.handler.Prep(ctx)
	if err != nil {
		phaseLogger.Error().Err(err).Msg("NodeWorker prep phase failed")
		return n.handleNodeExecError(event, err, 0, false)
	}

	phaseLogger.Debug().Interface("prepResult", prepResult).Msg("NodeWorker prep phase completed")

	// Publish progress for exec phase
	n.publishProgressUpdate(event.FlowType, event.FlowExecutionID, event.NodeExecutionID, "node_exec", 0.5,
		fmt.Sprintf("%s node execution phase", n.nodeType))

	// Exec phase
	phaseLogger = execLogger.With().Str("phase", "exec").Logger()
	phaseLogger.Debug().Msg("NodeWorker starting exec phase")

	execResult, err := n.handler.Exec(ctx, prepResult)
	if err != nil {
		phaseLogger.Error().Err(err).Msg("NodeWorker exec phase failed")
		return n.handleNodeExecError(event, err, 0, false)
	}

	phaseLogger.Debug().Interface("execResult", execResult).Msg("NodeWorker exec phase completed")

	// Publish progress for post phase
	n.publishProgressUpdate(event.FlowType, event.FlowExecutionID, event.NodeExecutionID, "node_post", 0.75,
		fmt.Sprintf("%s node post-processing phase", n.nodeType))

	// Post phase
	phaseLogger = execLogger.With().Str("phase", "post").Logger()
	phaseLogger.Debug().Msg("NodeWorker starting post phase")

	action, result, err := n.handler.Post(ctx, prepResult, execResult)
	if err != nil {
		phaseLogger.Error().Err(err).Msg("NodeWorker post phase failed")
		return n.handleNodeExecError(event, err, 0, false)
	}

	phaseLogger.Debug().Str("action", action).Interface("result", result).Msg("NodeWorker post phase completed")

	// Publish completion to flow-scoped topic
	completionTopic := fmt.Sprintf("%s.node.completed", event.FlowType)
	completionLogger := execLogger.With().
		Str("action", action).
		Str("topic", completionTopic).
		Logger()

	completionLogger.Debug().Msg("NodeWorker publishing completion message")

	err = n.publisher.Publish(
		completionTopic,
		core.NodeCompletedMessage{
			BaseMessage: core.BaseMessage{
				MessageType:     core.MessageTypeNodeCompleted,
				FlowType:        event.FlowType,
				FlowExecutionID: event.FlowExecutionID,
				NodeExecutionID: event.NodeExecutionID,
				Timestamp:       time.Now(),
			},
			NodeType: n.nodeType,
			NodeID:   event.NodeID,
			Action:   action,
			Result:   result,
			Success:  true,
		},
	)

	if err != nil {
		completionLogger.Error().Err(err).Msg("NodeWorker failed to publish completion message")
		return err
	}

	completionLogger.Debug().Msg("NodeWorker successfully published completion message")

	// Publish final progress update
	n.publishProgressUpdate(event.FlowType, event.FlowExecutionID, event.NodeExecutionID, "node_completed", 1.0,
		fmt.Sprintf("%s node completed with action: %s", n.nodeType, action))

	return nil
}

func (n *SimpleNodeWorker) handleNodeExecError(event core.ExecRequestedMessage, err error, retryCount int, willRetry bool) error {
	execLogger := log.With().
		Str("nodeType", n.nodeType).
		Str("flowExecutionID", event.FlowExecutionID).
		Str("nodeExecutionID", event.NodeExecutionID).
		Logger()

	// Log the error
	execLogger.Error().Err(err).Msg("Node execution failed")

	// Publish error message to flow-scoped failed topic
	failedTopic := fmt.Sprintf("%s.node.failed", event.FlowType)
	n.publisher.Publish(
		failedTopic,
		core.ExecFailedMessage{
			BaseMessage: core.BaseMessage{
				MessageType:     core.MessageTypeExecFailed,
				FlowType:        event.FlowType,
				FlowExecutionID: event.FlowExecutionID,
				NodeExecutionID: event.NodeExecutionID,
				Timestamp:       time.Now(),
			},
			NodeType:     n.nodeType,
			NodeID:       event.NodeID,
			ErrorMessage: err.Error(),
			RetryCount:   retryCount,
			WillRetry:    willRetry,
		},
	)

	// Publish failure completion message to flow-scoped topic
	completionTopic := fmt.Sprintf("%s.node.completed", event.FlowType)
	n.publisher.Publish(
		completionTopic,
		core.NodeCompletedMessage{
			BaseMessage: core.BaseMessage{
				MessageType:     core.MessageTypeNodeCompleted,
				FlowType:        event.FlowType,
				FlowExecutionID: event.FlowExecutionID,
				NodeExecutionID: event.NodeExecutionID,
				Timestamp:       time.Now(),
			},
			NodeType:     n.nodeType,
			NodeID:       event.NodeID,
			Action:       "",
			Result:       nil,
			Success:      false,
			ErrorMessage: err.Error(),
		},
	)

	return err
}

func (n *SimpleNodeWorker) publishProgressUpdate(flowType, flowExecutionID, nodeExecutionID, status string, progress float64, message string) {
	n.publisher.Publish(
		core.TopicProgress,
		core.ProgressUpdateMessage{
			BaseMessage: core.BaseMessage{
				MessageType:     core.MessageTypeProgressUpdate,
				FlowType:        flowType,
				FlowExecutionID: flowExecutionID,
				NodeExecutionID: nodeExecutionID,
				Timestamp:       time.Now(),
			},
			Status:   status,
			Progress: progress,
			Message:  message,
		},
	)
}

// NewNode creates a new Node instance for this node worker
func (n *SimpleNodeWorker) NewNode(params core.NodeParams) core.Node {
	return &SimpleNode{
		id:       uuid.New().String(),
		nodeType: n.nodeType,
		params:   params,
	}
}

// Legacy methods (simplified implementations)
func (n *SimpleNodeWorker) HandlePrepRequested(event core.NodePrepRequested) {}
func (n *SimpleNodeWorker) HandleExecRequested(event core.NodeExecRequested) {}
func (n *SimpleNodeWorker) HandlePostRequested(event core.NodePostRequested) {}
func (n *SimpleNodeWorker) HandleExecFailed(event core.NodeExecFailed)       {}
