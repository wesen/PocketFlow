package node

import (
	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/The-Pocket/PocketFlow/go/semantic"
)

// nodeBuilderImpl implements the NodeBuilder interface
type nodeBuilderImpl struct {
	nodeType  string
	name      string
	params    core.NodeParams
	prepFn    func(ctx semantic.NodeContext) (interface{}, error)
	execFn    func(ctx semantic.NodeContext, prepResult interface{}) (interface{}, error)
	postFn    func(ctx semantic.NodeContext, prepResult, execResult interface{}) (string, interface{}, error)
	publisher core.EventPublisher
	store     semantic.StateStore
}

// functionBasedHandler adapts callback functions to the SimpleNodeHandler interface
type functionBasedHandler struct {
	prepFn func(ctx semantic.NodeContext) (interface{}, error)
	execFn func(ctx semantic.NodeContext, prepResult interface{}) (interface{}, error)
	postFn func(ctx semantic.NodeContext, prepResult, execResult interface{}) (string, interface{}, error)
}

// defaultPrepFn is used when no prep function is provided
func defaultPrepFn(ctx semantic.NodeContext) (interface{}, error) {
	return nil, nil
}

// defaultExecFn is used when no exec function is provided
func defaultExecFn(ctx semantic.NodeContext, prepResult interface{}) (interface{}, error) {
	return prepResult, nil
}

// defaultPostFn is used when no post function is provided
func defaultPostFn(ctx semantic.NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
	return "default", execResult, nil
}

// Prep implements SimpleNodeHandler.Prep
func (h *functionBasedHandler) Prep(ctx semantic.NodeContext) (interface{}, error) {
	return h.prepFn(ctx)
}

// Exec implements SimpleNodeHandler.Exec
func (h *functionBasedHandler) Exec(ctx semantic.NodeContext, prepResult interface{}) (interface{}, error) {
	return h.execFn(ctx, prepResult)
}

// Post implements SimpleNodeHandler.Post
func (h *functionBasedHandler) Post(ctx semantic.NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
	return h.postFn(ctx, prepResult, execResult)
}

// NewNodeBuilder creates a new NodeBuilder instance
func NewNodeBuilder(nodeType string, publisher core.EventPublisher, store semantic.StateStore) core.NodeBuilder {
	return &nodeBuilderImpl{
		nodeType:  nodeType,
		name:      nodeType, // Default name is the type
		params:    make(core.NodeParams),
		prepFn:    defaultPrepFn,
		execFn:    defaultExecFn,
		postFn:    defaultPostFn,
		publisher: publisher,
		store:     store,
	}
}

// WithName sets the display name for the node
func (b *nodeBuilderImpl) WithName(name string) core.NodeBuilder {
	b.name = name
	return b
}

// WithParam adds a parameter to the node
func (b *nodeBuilderImpl) WithParam(key string, value interface{}) core.NodeBuilder {
	b.params[key] = value
	return b
}

// WithPrep sets the prep handler function
func (b *nodeBuilderImpl) WithPrep(handler func(ctx semantic.NodeContext) (interface{}, error)) core.NodeBuilder {
	b.prepFn = handler
	return b
}

// WithExec sets the exec handler function
func (b *nodeBuilderImpl) WithExec(handler func(ctx semantic.NodeContext, prepResult interface{}) (interface{}, error)) core.NodeBuilder {
	b.execFn = handler
	return b
}

// WithPost sets the post handler function
	func (b *nodeBuilderImpl) WithPost(handler func(ctx semantic.NodeContext, prepResult, execResult interface{}) (string, interface{}, error)) core.NodeBuilder {
	b.postFn = handler
	return b
}

// Build creates a NodeWorker instance with the specified configuration
func (b *nodeBuilderImpl) Build() core.NodeWorker {
	// Create a function-based handler
	handler := &functionBasedHandler{
		prepFn: b.prepFn,
		execFn: b.execFn,
		postFn: b.postFn,
	}
	
	// Add params to the node definition
	nodeDef := NewNode(b.nodeType, b.params)
	
	// If a name was specified, set it
	if b.name != b.nodeType {
		nodeDef.(*nodeImpl).WithName(b.name)
	}
	
	// Create a SimpleNode with the handler
	simpleNode := NewSimpleNode(b.nodeType, handler, b.publisher, b.store)
	
	// Return the node worker - it implements the NewNode method through SimpleNode
	return simpleNode
}