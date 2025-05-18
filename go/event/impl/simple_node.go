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
		return fmt.Errorf("invalid message type")
	}
	
	var base core.BaseMessage
	if err := json.Unmarshal(msg.Payload, &base); err != nil {
		return err
	}
	
	switch base.MessageType {
	case core.MessageTypeExecRequested:
		var execReq core.ExecRequestedMessage
		if err := json.Unmarshal(msg.Payload, &execReq); err != nil {
			return err
		}
		return n.handleExecRequested(execReq)
	default:
		return fmt.Errorf("unsupported message type: %s", base.MessageType)
	}
}

func (n *SimpleNode) handleExecRequested(event core.ExecRequestedMessage) error {
	// Publish starting progress update
	n.publishProgressUpdate(event.FlowExecutionID, event.NodeExecutionID, "node_started", 0.0,
		fmt.Sprintf("%s node started processing", n.nodeType))
	
	// Get shared data from the store
	sharedData, err := n.store.GetSharedData(event.FlowExecutionID)
	if err != nil {
		return n.handleExecError(event, err, 0, false)
	}
	
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
	log.Debug().Str("nodeType", n.nodeType).Str("phase", "prep").Msg("Executing node phase")
	prepResult, err := n.handler.Prep(ctx)
	if err != nil {
		return n.handleExecError(event, fmt.Errorf("prep phase failed: %w", err), 0, false)
	}
	
	// Publish progress update
	n.publishProgressUpdate(event.FlowExecutionID, event.NodeExecutionID, "node_processing", 0.33,
		"Prep phase completed, executing main phase")
	
	// Execute the exec phase
	log.Debug().Str("nodeType", n.nodeType).Str("phase", "exec").Msg("Executing node phase")
	execResult, err := n.handler.Exec(ctx, prepResult)
	if err != nil {
		return n.handleExecError(event, fmt.Errorf("exec phase failed: %w", err), 0, false)
	}
	
	// Publish progress update
	n.publishProgressUpdate(event.FlowExecutionID, event.NodeExecutionID, "node_finalizing", 0.67,
		"Main phase completed, executing post phase")
	
	// Execute the post phase
	log.Debug().Str("nodeType", n.nodeType).Str("phase", "post").Msg("Executing node phase")
	action, result, err := n.handler.Post(ctx, prepResult, execResult)
	if err != nil {
		return n.handleExecError(event, fmt.Errorf("post phase failed: %w", err), 0, false)
	}
	
	// Publish completion
	n.publisher.Publish(
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
	
	// Publish error message
	n.publisher.Publish(
		fmt.Sprintf("node.%s", n.nodeType),
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

// Legacy methods (simplified implementations)
func (n *SimpleNode) HandlePrepRequested(event core.NodePrepRequested) {}
func (n *SimpleNode) HandleExecRequested(event core.NodeExecRequested) {}
func (n *SimpleNode) HandlePostRequested(event core.NodePostRequested) {}
func (n *SimpleNode) HandleExecFailed(event core.NodeExecFailed) {}