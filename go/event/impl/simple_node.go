package impl

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/rs/zerolog/log"
)

// SimpleNode is a wrapper that implements NodeWorker using a SimpleNodeHandler
type SimpleNode struct {
	nodeType  string
	handler   core.SimpleNodeHandler
	publisher core.EventPublisher
	store     core.StateStore
}

// NewSimpleNode creates a new SimpleNode
func NewSimpleNode(
	nodeType string,
	handler core.SimpleNodeHandler,
	publisher core.EventPublisher,
	store core.StateStore,
) *SimpleNode {
	return &SimpleNode{
		nodeType:  nodeType,
		handler:   handler,
		publisher: publisher,
		store:     store,
	}
}

// NodeType returns the type of node this worker handles
func (n *SimpleNode) NodeType() string {
	return n.nodeType
}

// SupportedMessageTypes returns the list of message types this worker can handle
func (n *SimpleNode) SupportedMessageTypes() []string {
	return []string{core.MessageTypeExecRequested}
}

// HandleMessage handles a message
func (n *SimpleNode) HandleMessage(msgObj interface{}) error {
	msg, ok := msgObj.(*message.Message)
	if !ok {
		log.Error().Str("nodeType", n.nodeType).Msg("NodeWorker received invalid message type")
		return fmt.Errorf("invalid message type")
	}
	
	var base core.BaseMessage
	if err := json.Unmarshal(msg.Payload, &base); err != nil {
		log.Error().
			Err(err).
			Str("nodeType", n.nodeType).
			Str("messageID", msg.UUID).
			Msg("NodeWorker failed to unmarshal base message")
		return err
	}
	
	log.Debug().
		Str("nodeType", n.nodeType).
		Str("messageType", base.MessageType).
		Str("flowExecutionID", base.FlowExecutionID).
		Str("nodeExecutionID", base.NodeExecutionID).
		Str("messageID", msg.UUID).
		Msg("NodeWorker handling message")
	
	switch base.MessageType {
	case core.MessageTypeExecRequested:
		var execReq core.ExecRequestedMessage
		if err := json.Unmarshal(msg.Payload, &execReq); err != nil {
			log.Error().
				Err(err).
				Str("nodeType", n.nodeType).
				Str("messageID", msg.UUID).
				Msg("NodeWorker failed to unmarshal exec request message")
			return err
		}
		return n.handleExecRequested(execReq)
	default:
		log.Warn().
			Str("nodeType", n.nodeType).
			Str("messageType", base.MessageType).
			Str("flowExecutionID", base.FlowExecutionID).
			Msg("NodeWorker received unsupported message type")
		return fmt.Errorf("unsupported message type: %s", base.MessageType)
	}
}

func (n *SimpleNode) handleExecRequested(event core.ExecRequestedMessage) error {
	log.Debug().
		Str("nodeType", n.nodeType).
		Str("flowExecutionID", event.FlowExecutionID).
		Str("nodeExecutionID", event.NodeExecutionID).
		Str("nodeID", event.NodeID).
		Interface("params", event.Params).
		Msg("NodeWorker starting execution request processing")
	
	// Publish starting progress update
	n.publishProgressUpdate(event.FlowExecutionID, event.NodeExecutionID, "node_started", 0.0,
		fmt.Sprintf("%s node started processing", n.nodeType))
	
	// Get shared data from the store
	sharedData, err := n.store.GetSharedData(event.FlowExecutionID)
	if err != nil {
		log.Error().
			Err(err).
			Str("nodeType", n.nodeType).
			Str("flowExecutionID", event.FlowExecutionID).
			Str("nodeExecutionID", event.NodeExecutionID).
			Msg("NodeWorker failed to get shared data")
		return n.handleExecError(event, err, 0, false)
	}
	
	log.Debug().
		Str("nodeType", n.nodeType).
		Str("flowExecutionID", event.FlowExecutionID).
		Str("nodeExecutionID", event.NodeExecutionID).
		Interface("sharedData", sharedData).
		Msg("NodeWorker retrieved shared data")
	
	// Create node context
	ctx := core.NodeContext{
		FlowExecutionID: event.FlowExecutionID,
		NodeExecutionID: event.NodeExecutionID,
		NodeID:          event.NodeID,
		NodeType:        event.NodeType,
		Params:          event.Params,
		SharedData:      sharedData,
		StateStore:      n.store,
	}
	
	// Execute the prep phase
	log.Debug().
		Str("nodeType", n.nodeType).
		Str("flowExecutionID", event.FlowExecutionID).
		Str("nodeExecutionID", event.NodeExecutionID).
		Str("phase", "prep").
		Msg("NodeWorker executing prep phase")
	
	prepResult, err := n.handler.Prep(ctx)
	if err != nil {
		log.Error().
			Err(err).
			Str("nodeType", n.nodeType).
			Str("flowExecutionID", event.FlowExecutionID).
			Str("nodeExecutionID", event.NodeExecutionID).
			Str("phase", "prep").
			Msg("NodeWorker prep phase failed")
		return n.handleExecError(event, fmt.Errorf("prep phase failed: %w", err), 0, false)
	}
	
	log.Debug().
		Str("nodeType", n.nodeType).
		Str("flowExecutionID", event.FlowExecutionID).
		Str("nodeExecutionID", event.NodeExecutionID).
		Str("phase", "prep").
		Interface("prepResult", prepResult).
		Msg("NodeWorker prep phase completed")
	
	// Publish progress update
	n.publishProgressUpdate(event.FlowExecutionID, event.NodeExecutionID, "node_processing", 0.33,
		"Prep phase completed, executing main phase")
	
	// Execute the exec phase
	log.Debug().
		Str("nodeType", n.nodeType).
		Str("flowExecutionID", event.FlowExecutionID).
		Str("nodeExecutionID", event.NodeExecutionID).
		Str("phase", "exec").
		Msg("NodeWorker executing exec phase")
	
	execResult, err := n.handler.Exec(ctx, prepResult)
	if err != nil {
		log.Error().
			Err(err).
			Str("nodeType", n.nodeType).
			Str("flowExecutionID", event.FlowExecutionID).
			Str("nodeExecutionID", event.NodeExecutionID).
			Str("phase", "exec").
			Msg("NodeWorker exec phase failed")
		return n.handleExecError(event, fmt.Errorf("exec phase failed: %w", err), 0, false)
	}
	
	log.Debug().
		Str("nodeType", n.nodeType).
		Str("flowExecutionID", event.FlowExecutionID).
		Str("nodeExecutionID", event.NodeExecutionID).
		Str("phase", "exec").
		Interface("execResult", execResult).
		Msg("NodeWorker exec phase completed")
	
	// Publish progress update
	n.publishProgressUpdate(event.FlowExecutionID, event.NodeExecutionID, "node_finalizing", 0.67,
		"Main phase completed, executing post phase")
	
	// Execute the post phase
	log.Debug().
		Str("nodeType", n.nodeType).
		Str("flowExecutionID", event.FlowExecutionID).
		Str("nodeExecutionID", event.NodeExecutionID).
		Str("phase", "post").
		Msg("NodeWorker executing post phase")
	
	action, result, err := n.handler.Post(ctx, prepResult, execResult)
	if err != nil {
		log.Error().
			Err(err).
			Str("nodeType", n.nodeType).
			Str("flowExecutionID", event.FlowExecutionID).
			Str("nodeExecutionID", event.NodeExecutionID).
			Str("phase", "post").
			Msg("NodeWorker post phase failed")
		return n.handleExecError(event, fmt.Errorf("post phase failed: %w", err), 0, false)
	}
	
	log.Debug().
		Str("nodeType", n.nodeType).
		Str("flowExecutionID", event.FlowExecutionID).
		Str("nodeExecutionID", event.NodeExecutionID).
		Str("phase", "post").
		Str("action", action).
		Interface("result", result).
		Msg("NodeWorker post phase completed")
	
	// Publish completion
	log.Debug().
		Str("nodeType", n.nodeType).
		Str("flowExecutionID", event.FlowExecutionID).
		Str("nodeExecutionID", event.NodeExecutionID).
		Str("action", action).
		Str("topic", core.TopicNodeCompleted).
		Msg("NodeWorker publishing completion message")
	
	err = n.publisher.Publish(
		core.TopicNodeCompleted,
		core.NodeCompletedMessage{
			BaseMessage: core.BaseMessage{
				MessageType:     core.MessageTypeNodeCompleted,
				FlowExecutionID: event.FlowExecutionID,
				NodeExecutionID: event.NodeExecutionID,
				Timestamp:       time.Now(),
			},
			NodeType: n.nodeType,
			NodeID:   event.NodeID,
			Action:   action,
			Result:   result,
		},
	)
	
	if err != nil {
		log.Error().
			Err(err).
			Str("nodeType", n.nodeType).
			Str("flowExecutionID", event.FlowExecutionID).
			Str("nodeExecutionID", event.NodeExecutionID).
			Str("topic", core.TopicNodeCompleted).
			Msg("NodeWorker failed to publish completion message")
		return err
	}
	
	log.Debug().
		Str("nodeType", n.nodeType).
		Str("flowExecutionID", event.FlowExecutionID).
		Str("nodeExecutionID", event.NodeExecutionID).
		Str("action", action).
		Str("topic", core.TopicNodeCompleted).
		Msg("NodeWorker successfully published completion message")
	
	// Publish final progress update
	n.publishProgressUpdate(event.FlowExecutionID, event.NodeExecutionID, "node_completed", 1.0,
		fmt.Sprintf("%s node completed with action: %s", n.nodeType, action))
	
	return nil
}

func (n *SimpleNode) handleExecError(event core.ExecRequestedMessage, err error, retryCount int, willRetry bool) error {
	// Log the error
	log.Error().
		Err(err).
		Str("nodeType", n.nodeType).
		Str("flowExecutionID", event.FlowExecutionID).
		Str("nodeExecutionID", event.NodeExecutionID).
		Msg("Node execution failed")
	
	// Publish error message to node.failed topic instead of node's own topic
	n.publisher.Publish(
		"node.failed",
		core.ExecFailedMessage{
			BaseMessage: core.BaseMessage{
				MessageType:     core.MessageTypeExecFailed,
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
	n.publishProgressUpdate(event.FlowExecutionID, event.NodeExecutionID, "node_failed", 0.0,
		fmt.Sprintf("%s node failed: %s", n.nodeType, err.Error()))
	
	return nil
}

func (n *SimpleNode) publishProgressUpdate(flowExecutionID, nodeExecutionID, status string, progress float64, message string) {
	n.publisher.Publish(
		core.TopicProgress,
		core.ProgressUpdateMessage{
			BaseMessage: core.BaseMessage{
				MessageType:     core.MessageTypeProgressUpdate,
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
func (n *SimpleNode) NewNode(params core.NodeParams) core.Node {
	return NewNode(n.nodeType, params)
}

// Legacy methods (simplified implementations)
func (n *SimpleNode) HandlePrepRequested(event core.NodePrepRequested) {}
func (n *SimpleNode) HandleExecRequested(event core.NodeExecRequested) {}
func (n *SimpleNode) HandlePostRequested(event core.NodePostRequested) {}
func (n *SimpleNode) HandleExecFailed(event core.NodeExecFailed) {}