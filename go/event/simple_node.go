package event

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

// SimpleNodeHandler is a simplified interface for node handlers
// that only need to implement the core business logic.
type SimpleNodeHandler interface {
	// Prep handles the preparation phase
	Prep(ctx NodeContext) (interface{}, error)

	// Exec handles the execution phase
	Exec(ctx NodeContext, prepResult interface{}) (interface{}, error)

	// Post handles the post-processing phase and returns the action to take
	Post(ctx NodeContext, prepResult, execResult interface{}) (string, interface{}, error)
}

// NodeContext provides access to the execution context of a node
type NodeContext struct {
	FlowExecutionID string
	NodeExecutionID string
	NodeID          string
	NodeType        string
	Params          map[string]interface{}
	SharedData      map[string]interface{}
	StateStore      StateStore
}

// SimpleNode is a base implementation that handles the messaging infrastructure
type SimpleNode struct {
	Handler    SimpleNodeHandler
	nodeType   string
	Publisher  EventPublisher
	StateStore StateStore
}

// NewSimpleNode creates a new SimpleNode
func NewSimpleNode(
	nodeType string,
	handler SimpleNodeHandler,
	publisher EventPublisher,
	stateStore StateStore,
) *SimpleNode {
	return &SimpleNode{
		Handler:    handler,
		nodeType:   nodeType,
		Publisher:  publisher,
		StateStore: stateStore,
	}
}

// NodeType returns the type of this node
func (n *SimpleNode) NodeType() string {
	return n.nodeType
}

// SupportedMessageTypes returns the message types this node can handle
func (n *SimpleNode) SupportedMessageTypes() []string {
	return []string{MessageTypeExecRequested}
}

// HandleMessage processes incoming messages
func (n *SimpleNode) HandleMessage(msgObj interface{}) error {
	msg, ok := msgObj.(*message.Message)
	if !ok {
		return fmt.Errorf("expected *message.Message but got %T", msgObj)
	}

	// Extract base message to determine type
	var base BaseMessage
	if err := json.Unmarshal(msg.Payload, &base); err != nil {
		return fmt.Errorf("failed to unmarshal base message: %v", err)
	}

	// Process based on message type
	switch base.MessageType {
	case MessageTypeExecRequested:
		var execReq ExecRequestedMessage
		if err := json.Unmarshal(msg.Payload, &execReq); err != nil {
			return fmt.Errorf("failed to unmarshal exec request: %v", err)
		}

		// Create node context
		sharedData, err := n.StateStore.GetSharedData(execReq.FlowExecutionID)
		if err != nil {
			return fmt.Errorf("failed to get shared data: %v", err)
		}

		ctx := NodeContext{
			FlowExecutionID: execReq.FlowExecutionID,
			NodeExecutionID: execReq.NodeExecutionID,
			NodeID:          execReq.NodeID,
			NodeType:        execReq.NodeType,
			Params:          execReq.Params,
			SharedData:      sharedData,
			StateStore:      n.StateStore,
		}

		// Run the handler pipeline
		return n.runHandlerPipeline(ctx)

	default:
		return fmt.Errorf("unsupported message type: %s", base.MessageType)
	}
}

// runHandlerPipeline runs the prep, exec, and post handlers in sequence
func (n *SimpleNode) runHandlerPipeline(ctx NodeContext) error {
	// Run prep phase
	prepResult, err := n.Handler.Prep(ctx)
	if err != nil {
		return n.publishError(ctx, "prep failed", err)
	}

	// Store prep result in shared data
	if err := n.StateStore.UpdateSharedData(ctx.FlowExecutionID, ctx.NodeID+"_prep_result", prepResult); err != nil {
		return n.publishError(ctx, "failed to store prep result", err) 
	}

	// Run exec phase
	execResult, err := n.Handler.Exec(ctx, prepResult)
	if err != nil {
		return n.publishError(ctx, "exec failed", err)
	}

	// Store exec result in shared data
	if err := n.StateStore.UpdateSharedData(ctx.FlowExecutionID, ctx.NodeID+"_exec_result", execResult); err != nil {
		return n.publishError(ctx, "failed to store exec result", err)
	}

	// Run post phase
	action, result, err := n.Handler.Post(ctx, prepResult, execResult)
	if err != nil {
		return n.publishError(ctx, "post failed", err)
	}

	// Update shared data with the result
	if err := n.StateStore.UpdateSharedData(ctx.FlowExecutionID, ctx.NodeID, result); err != nil {
		return n.publishError(ctx, "failed to update shared data", err)
	}

	// Publish completion message
	return n.Publisher.Publish("node.completed", NodeCompletedMessage{
		BaseMessage: BaseMessage{
			MessageType:     MessageTypeNodeCompleted,
			FlowExecutionID: ctx.FlowExecutionID,
			NodeExecutionID: ctx.NodeExecutionID,
			Timestamp:       time.Now(),
		},
		NodeType: ctx.NodeType,
		NodeID:   ctx.NodeID,
		Action:   action,
		Result:   result,
	})
}

// publishError publishes an error message
func (n *SimpleNode) publishError(ctx NodeContext, message string, err error) error {
	fullMessage := fmt.Sprintf("%s: %v", message, err)
	fmt.Printf("[%s] Error: %s\n", ctx.NodeType, fullMessage)

	// Publish error message
	return n.Publisher.Publish("node.exec.failed", ExecFailedMessage{
		BaseMessage: BaseMessage{
			MessageType:     MessageTypeExecFailed,
			FlowExecutionID: ctx.FlowExecutionID,
			NodeExecutionID: ctx.NodeExecutionID,
			Timestamp:       time.Now(),
		},
		NodeType:     ctx.NodeType,
		NodeID:       ctx.NodeID,
		ErrorMessage: fullMessage,
		RetryCount:   0,
		WillRetry:    false,
	})
}

// Legacy interface implementations
func (n *SimpleNode) HandlePrepRequested(event NodePrepRequested) {}
func (n *SimpleNode) HandleExecRequested(event NodeExecRequested) {}
func (n *SimpleNode) HandlePostRequested(event NodePostRequested) {}
func (n *SimpleNode) HandleExecFailed(event NodeExecFailed) {}