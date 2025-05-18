package impl

import (
	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/google/uuid"
)

// nodeImpl implements the Node interface
type nodeImpl struct {
	id       string
	nodeType string
	name     string
	params   map[string]interface{}
}

func (n *nodeImpl) ID() string {
	return n.id
}

func (n *nodeImpl) Type() string {
	return n.nodeType
}

func (n *nodeImpl) Name() string {
	return n.name
}

func (n *nodeImpl) Params() map[string]interface{} {
	return n.params
}

// NewNode creates a new Node instance
func NewNode(nodeType string, params map[string]interface{}) core.Node {
	return &nodeImpl{
		id:       uuid.New().String(),
		nodeType: nodeType,
		name:     nodeType, // Default name is the type
		params:   params,
	}
}

// WithName sets a custom name for the node
func (n *nodeImpl) WithName(name string) core.Node {
	n.name = name
	return n
}