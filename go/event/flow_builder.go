package event

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// flowDefinition implements the Flow interface
type flowDefinition struct {
	id          string
	flowType    string
	name        string
	startNode   Node
	nodes       map[string]Node
	transitions map[string]map[string]string // map[sourceNodeID][action]targetNodeID
}

// NewFlowBuilder creates a new FlowBuilder instance
func NewFlowBuilder() FlowBuilder {
	return &flowBuilderImpl{
		flow: &flowDefinition{
			id:          uuid.New().String(),
			flowType:    "",
			name:        "",
			nodes:       make(map[string]Node),
			transitions: make(map[string]map[string]string),
		},
	}
}

// flowBuilderImpl implements the FlowBuilder interface
type flowBuilderImpl struct {
	flow        *flowDefinition
	currentNode Node
}

// nodeImpl implements the Node interface
type nodeImpl struct {
	id       string
	nodeType string
	name     string
	params   map[string]interface{}
}

// NewNode creates a new Node instance
func NewNode(nodeType string, params map[string]interface{}) Node {
	return &nodeImpl{
		id:       uuid.New().String(),
		nodeType: nodeType,
		name:     nodeType, // Default name is the type
		params:   params,
	}
}

// ID returns the unique ID of this node
func (n *nodeImpl) ID() string {
	return n.id
}

// Type returns the type of this node
func (n *nodeImpl) Type() string {
	return n.nodeType
}

// Name returns the display name of this node
func (n *nodeImpl) Name() string {
	return n.name
}

// Params returns the parameters for this node
func (n *nodeImpl) Params() map[string]interface{} {
	return n.params
}

// ID returns the unique ID of this flow
func (f *flowDefinition) ID() string {
	return f.id
}

// Type returns the type of this flow
func (f *flowDefinition) Type() string {
	return f.flowType
}

// Name returns the name of this flow
func (f *flowDefinition) Name() string {
	return f.name
}

// StartNode returns the starting node of this flow
func (f *flowDefinition) StartNode() Node {
	return f.startNode
}

// Nodes returns all nodes in this flow
func (f *flowDefinition) Nodes() map[string]Node {
	return f.nodes
}

// GetNextNode returns the next node based on current node and action
func (f *flowDefinition) GetNextNode(currentNodeID, action string) (Node, bool) {
	// Check if node exists in transitions map
	nodeTransitions, exists := f.transitions[currentNodeID]
	if !exists {
		return nil, false
	}

	// Check if action exists in node transitions
	targetNodeID, exists := nodeTransitions[action]
	if !exists {
		// Try default transition
		targetNodeID, exists = nodeTransitions["default"]
		if !exists {
			return nil, false
		}
	}

	// Get next node definition
	nextNode, exists := f.nodes[targetNodeID]
	if !exists {
		return nil, false
	}

	return nextNode, true
}

// Visualize generates a mermaid diagram for the flow
func (f *flowDefinition) Visualize() string {
	var sb strings.Builder

	sb.WriteString("flowchart TD\n")

	// Add nodes
	for _, node := range f.nodes {
		sb.WriteString(fmt.Sprintf("    %s[%s]\n", node.ID(), node.Name()))
	}

	// Add transitions
	for sourceID, actions := range f.transitions {
		for action, targetID := range actions {
			label := action
			if label == "default" {
				label = ""
			} else {
				label = "|" + label + "|"
			}

			sb.WriteString(fmt.Sprintf("    %s -->%s %s\n",
				sourceID, label, targetID))
		}
	}

	return sb.String()
}

// Begin sets the starting node for the flow
func (b *flowBuilderImpl) Begin(node Node) FlowBuilder {
	b.flow.startNode = node
	b.flow.nodes[node.ID()] = node
	b.currentNode = node
	return b
}

// Then creates a default transition from the previous node
func (b *flowBuilderImpl) Then(node Node) FlowBuilder {
	if b.currentNode == nil {
		panic("No current node set. Use Begin() or From() first.")
	}

	// Add node to flow
	b.flow.nodes[node.ID()] = node

	// Create transition map for source node if it doesn't exist
	sourceID := b.currentNode.ID()
	if _, exists := b.flow.transitions[sourceID]; !exists {
		b.flow.transitions[sourceID] = make(map[string]string)
	}

	// Set default transition
	b.flow.transitions[sourceID]["default"] = node.ID()

	// Update current node
	b.currentNode = node

	return b
}

// On defines an action-based transition from the previous node
func (b *flowBuilderImpl) On(action string) TransitionBuilder {
	if b.currentNode == nil {
		panic("No current node set. Use Begin() or From() first.")
	}

	return &transitionBuilderImpl{
		flowBuilder: b,
		action:      action,
	}
}

// From switches the source node for subsequent transitions
func (b *flowBuilderImpl) From(node Node) FlowBuilder {
	// Ensure node is in the flow
	if _, exists := b.flow.nodes[node.ID()]; !exists {
		b.flow.nodes[node.ID()] = node
	}

	b.currentNode = node
	return b
}

// Build finalizes the flow definition
func (b *flowBuilderImpl) Build() Flow {
	if b.flow.startNode == nil {
		panic("Flow must have a starting node. Use Begin() first.")
	}

	// Set flow type based on start node type if not set
	if b.flow.flowType == "" {
		b.flow.flowType = b.flow.startNode.Type()
	}

	// Set flow name if not set
	if b.flow.name == "" {
		b.flow.name = b.flow.flowType + " Flow"
	}

	return b.flow
}

// transitionBuilderImpl implements the TransitionBuilder interface
type transitionBuilderImpl struct {
	flowBuilder *flowBuilderImpl
	action      string
}

// Then sets the destination node for this action
func (t *transitionBuilderImpl) Then(node Node) FlowBuilder {
	// Add node to flow
	t.flowBuilder.flow.nodes[node.ID()] = node

	// Get source node ID
	sourceID := t.flowBuilder.currentNode.ID()

	// Create transition map for source node if it doesn't exist
	if _, exists := t.flowBuilder.flow.transitions[sourceID]; !exists {
		t.flowBuilder.flow.transitions[sourceID] = make(map[string]string)
	}

	// Set transition for this action
	t.flowBuilder.flow.transitions[sourceID][t.action] = node.ID()

	// Update current node in flow builder
	t.flowBuilder.currentNode = node

	return t.flowBuilder
}
