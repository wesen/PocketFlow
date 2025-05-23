package flow

import (
	"fmt"
	"strings"

	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/google/uuid"
)

// flowDefinition implements the Flow interface
type flowDefinition struct {
	id          string
	flowType    string
	name        string
	startNode   core.Node
	nodes       map[string]core.Node
	transitions map[string]map[string]string // map[sourceNodeID][action]targetNodeID
}

func (f *flowDefinition) ID() string {
	return f.id
}

func (f *flowDefinition) Type() string {
	return f.flowType
}

func (f *flowDefinition) Name() string {
	return f.name
}

func (f *flowDefinition) StartNode() core.Node {
	return f.startNode
}

func (f *flowDefinition) Nodes() map[string]core.Node {
	return f.nodes
}

func (f *flowDefinition) GetNextNode(currentNodeID, action string) (core.Node, bool) {
	// Check if there are any transitions for this node
	if actions, exists := f.transitions[currentNodeID]; exists {
		// Check if there's a transition for this action
		if nextNodeID, hasAction := actions[action]; hasAction {
			// Look up the next node by ID
			if nextNode, found := f.nodes[nextNodeID]; found {
				return nextNode, true
			}
		}

		// If no specific action matches, try the default action
		if nextNodeID, hasDefault := actions["default"]; hasDefault {
			if nextNode, found := f.nodes[nextNodeID]; found {
				return nextNode, true
			}
		}
	}

	// No valid transition found
	return nil, false
}

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

// NewFlowDefinition creates a new flow definition
func NewFlowDefinition(flowType string) *flowDefinition {
	return &flowDefinition{
		id:          uuid.New().String(),
		flowType:    flowType,
		name:        flowType, // Default name is the type
		nodes:       make(map[string]core.Node),
		transitions: make(map[string]map[string]string),
	}
}

// flowBuilderImpl implements the FlowBuilder interface
type flowBuilderImpl struct {
	flow        *flowDefinition
	currentNode core.Node
}

func (b *flowBuilderImpl) Begin(node core.Node) core.FlowBuilder {
	// Add the node to the flow
	b.flow.nodes[node.ID()] = node

	// Set this as the start node
	b.flow.startNode = node

	// Set as current node for chaining
	b.currentNode = node

	return b
}

func (b *flowBuilderImpl) Then(node core.Node) core.FlowBuilder {
	// Add the node to the flow
	b.flow.nodes[node.ID()] = node

	// Add a default transition from current node to this node
	if b.currentNode != nil {
		// Initialize transitions map for the current node if needed
		if _, exists := b.flow.transitions[b.currentNode.ID()]; !exists {
			b.flow.transitions[b.currentNode.ID()] = make(map[string]string)
		}

		// Add default transition
		b.flow.transitions[b.currentNode.ID()]["default"] = node.ID()
	}

	// Set as current node for chaining
	b.currentNode = node

	return b
}

func (b *flowBuilderImpl) On(action string, targetNode core.Node) core.FlowBuilder {
	// Check if we have a current node
	if b.currentNode == nil {
		panic("On() called before establishing a current node with Begin() or From()")
	}

	// Add the targetNode to the flow
	b.flow.nodes[targetNode.ID()] = targetNode

	// Initialize transitions map for the current node if needed
	if _, exists := b.flow.transitions[b.currentNode.ID()]; !exists {
		b.flow.transitions[b.currentNode.ID()] = make(map[string]string)
	}

	// Add the action-specific transition
	b.flow.transitions[b.currentNode.ID()][action] = targetNode.ID()

	// Do NOT change b.currentNode - this allows multiple On() calls to branch from same node

	return b
}

func (b *flowBuilderImpl) From(node core.Node) core.FlowBuilder {
	// Set the current node to the specified node
	// The node should already be in the flow
	if _, exists := b.flow.nodes[node.ID()]; !exists {
		// If not, add it
		b.flow.nodes[node.ID()] = node
	}

	b.currentNode = node

	return b
}

func (b *flowBuilderImpl) Build() core.Flow {
	return b.flow
}

// NewFlowBuilder creates a new FlowBuilder instance
func NewFlowBuilder(type_ string) core.FlowBuilder {
	return &flowBuilderImpl{
		flow: NewFlowDefinition(type_),
	}
}
