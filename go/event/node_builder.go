package event

// No imports needed

// NodeBuilder provides a fluent API for building nodes
type NodeBuilder interface {
	// WithName sets the display name for the node
	WithName(name string) NodeBuilder

	// WithParam adds a parameter to the node
	WithParam(key string, value interface{}) NodeBuilder

	// WithPrep sets the prep handler function
	WithPrep(handler func(ctx NodeContext) (interface{}, error)) NodeBuilder

	// WithExec sets the exec handler function
	WithExec(handler func(ctx NodeContext, prepResult interface{}) (interface{}, error)) NodeBuilder

	// WithPost sets the post handler function
	WithPost(handler func(ctx NodeContext, prepResult, execResult interface{}) (string, interface{}, error)) NodeBuilder

	// Build creates a Node instance with the specified configuration
	Build() NodeWorker
}

// nodeBuilderImpl implements the NodeBuilder interface
type nodeBuilderImpl struct {
	nodeType   string
	nodeName   string
	params     map[string]interface{}
	prepFunc   func(ctx NodeContext) (interface{}, error)
	execFunc   func(ctx NodeContext, prepResult interface{}) (interface{}, error)
	postFunc   func(ctx NodeContext, prepResult, execResult interface{}) (string, interface{}, error)
	publisher  EventPublisher
	stateStore StateStore
}

// callbackHandler implements SimpleNodeHandler with callback functions
type callbackHandler struct {
	prepFunc func(ctx NodeContext) (interface{}, error)
	execFunc func(ctx NodeContext, prepResult interface{}) (interface{}, error)
	postFunc func(ctx NodeContext, prepResult, execResult interface{}) (string, interface{}, error)
}

// Prep implements SimpleNodeHandler.Prep
func (h *callbackHandler) Prep(ctx NodeContext) (interface{}, error) {
	if h.prepFunc == nil {
		return nil, nil
	}
	return h.prepFunc(ctx)
}

// Exec implements SimpleNodeHandler.Exec
func (h *callbackHandler) Exec(ctx NodeContext, prepResult interface{}) (interface{}, error) {
	if h.execFunc == nil {
		return prepResult, nil
	}
	return h.execFunc(ctx, prepResult)
}

// Post implements SimpleNodeHandler.Post
func (h *callbackHandler) Post(ctx NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
	if h.postFunc == nil {
		return "default", execResult, nil
	}
	return h.postFunc(ctx, prepResult, execResult)
}

// NewNodeBuilder creates a new NodeBuilder for the specified node type
func NewNodeBuilder(nodeType string, publisher EventPublisher, stateStore StateStore) NodeBuilder {
	return &nodeBuilderImpl{
		nodeType:   nodeType,
		nodeName:   nodeType, // Default name is the type
		params:     make(map[string]interface{}),
		publisher:  publisher,
		stateStore: stateStore,
	}
}

// WithName sets the display name for the node
func (b *nodeBuilderImpl) WithName(name string) NodeBuilder {
	b.nodeName = name
	return b
}

// WithParam adds a parameter to the node
func (b *nodeBuilderImpl) WithParam(key string, value interface{}) NodeBuilder {
	b.params[key] = value
	return b
}

// WithPrep sets the prep handler function
func (b *nodeBuilderImpl) WithPrep(handler func(ctx NodeContext) (interface{}, error)) NodeBuilder {
	b.prepFunc = handler
	return b
}

// WithExec sets the exec handler function
func (b *nodeBuilderImpl) WithExec(handler func(ctx NodeContext, prepResult interface{}) (interface{}, error)) NodeBuilder {
	b.execFunc = handler
	return b
}

// WithPost sets the post handler function
func (b *nodeBuilderImpl) WithPost(handler func(ctx NodeContext, prepResult, execResult interface{}) (string, interface{}, error)) NodeBuilder {
	b.postFunc = handler
	return b
}

// Build creates a Node instance with the specified configuration
func (b *nodeBuilderImpl) Build() NodeWorker {
	handler := &callbackHandler{
		prepFunc: b.prepFunc,
		execFunc: b.execFunc,
		postFunc: b.postFunc,
	}

	return NewSimpleNode(
		b.nodeType,
		handler,
		b.publisher,
		b.stateStore,
	)
}

// Example usage:
/*
// Create a simple echo node
echoNode := NewNodeBuilder("echo", publisher, stateStore).
    WithParam("prefix", "Echo: ").
    WithExec(func(ctx NodeContext, prepResult interface{}) (interface{}, error) {
        // Get input from shared data
        input, ok := ctx.SharedData["input"].(string)
        if !ok {
            return nil, fmt.Errorf("input not found in shared data")
        }
        
        // Get prefix from params
        prefix := "Echo: "
        if val, ok := ctx.Params["prefix"].(string); ok {
            prefix = val
        }
        
        // Return echoed input
        return prefix + input, nil
    }).
    Build()

// Register the node
watermillRouter.RegisterNodeWorker(echoNode)
*/