package event

// FlowAdapter adapts the new Flow interface to the old FlowDefinition
type FlowAdapter struct {
	flow Flow
}

// NewFlowAdapter creates a new adapter around a Flow implementation
func NewFlowAdapter(flow Flow) *FlowAdapter {
	return &FlowAdapter{flow: flow}
}

// ID returns the flow ID
func (a *FlowAdapter) ID() string {
	return a.flow.ID()
}

// Type returns the flow type
func (a *FlowAdapter) Type() string {
	return a.flow.Type()
}

// Name returns the flow name
func (a *FlowAdapter) Name() string {
	return a.flow.Name()
}

// StartNode returns the start node
func (a *FlowAdapter) StartNode() Node {
	return a.flow.StartNode()
}

// Nodes returns all nodes in the flow
func (a *FlowAdapter) Nodes() map[string]Node {
	return a.flow.Nodes()
}

// GetNextNode returns the next node based on current node and action
func (a *FlowAdapter) GetNextNode(currentNodeID, action string) (Node, bool) {
	return a.flow.GetNextNode(currentNodeID, action)
}

// Visualize generates a visualization of the flow
func (a *FlowAdapter) Visualize() string {
	return a.flow.Visualize()
}