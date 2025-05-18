package event

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/The-Pocket/PocketFlow/go/logger"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/rs/zerolog"
)

type WatermillEventRouter struct {
	FlowOrchestrator *FlowOrchestrator
	NodeWorkers      map[string]NodeWorker
	FlowWorkers      map[string]FlowWorker
	PubSub           *gochannel.GoChannel
	Router           *message.Router
}

// NewWatermillEventRouter creates a new event router
func NewWatermillEventRouter(orchestrator *FlowOrchestrator) *WatermillEventRouter {
	// Initialize watermill logger
	logger := watermill.NewStdLogger(false, false)

	// Initialize pub/sub
	pubSub := gochannel.NewGoChannel(
		gochannel.Config{},
		logger,
	)

	// Initialize router
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	// We'll register middleware handlers here
	router.AddMiddleware(
		func(h message.HandlerFunc) message.HandlerFunc {
			return func(msg *message.Message) ([]*message.Message, error) {
				// Extract base message for logging
				var base BaseMessage
				if err := json.Unmarshal(msg.Payload, &base); err == nil {
					zerolog.Ctx(context.Background()).Debug().
						Str("messageType", base.MessageType).
						Str("flowExecutionID", base.FlowExecutionID).
						Msg("Processing message")
				}
				return h(msg)
			}
		},
	)

	return &WatermillEventRouter{
		FlowOrchestrator: orchestrator,
		NodeWorkers:      make(map[string]NodeWorker),
		FlowWorkers:      make(map[string]FlowWorker),
		PubSub:           pubSub,
		Router:           router,
	}
}

// UpdateOrchestrator sets the orchestrator
func (r *WatermillEventRouter) UpdateOrchestrator(orchestrator *FlowOrchestrator) {
	if orchestrator == nil {
		panic("orchestrator cannot be nil")
	}
	r.FlowOrchestrator = orchestrator
}

// RegisterNodeWorker registers a node worker with the router
func (r *WatermillEventRouter) RegisterNodeWorker(worker NodeWorker) {
	nodeType := worker.NodeType()
	r.NodeWorkers[nodeType] = worker
	r.setupNodeWorkerHandlers(worker)
}

// RegisterFlowWorker registers a flow worker with the router
func (r *WatermillEventRouter) RegisterFlowWorker(worker FlowWorker) {
	flowType := worker.FlowType()
	r.FlowWorkers[flowType] = worker
	r.setupFlowWorkerHandlers(worker)
}

// setupNodeWorkerHandlers sets up handlers for a node worker
func (r *WatermillEventRouter) setupNodeWorkerHandlers(worker NodeWorker) {
	nodeType := worker.NodeType()

	// Add handler for node-specific topic
	r.Router.AddNoPublisherHandler(
		fmt.Sprintf("node.%s.handler", nodeType),
		fmt.Sprintf("node.%s", nodeType),
		r.PubSub,
		func(msg *message.Message) error {
			return worker.HandleMessage(msg)
		},
	)

	// For backward compatibility, add handlers for the old-style topics
	for _, msgType := range worker.SupportedMessageTypes() {
		r.Router.AddNoPublisherHandler(
			fmt.Sprintf("node.%s.%s.handler", nodeType, msgType),
			fmt.Sprintf("node.%s.%s", nodeType, msgType),
			r.PubSub,
			func(msg *message.Message) error {
				return worker.HandleMessage(msg)
			},
		)
	}
}

// setupFlowWorkerHandlers sets up handlers for a flow worker
func (r *WatermillEventRouter) setupFlowWorkerHandlers(worker FlowWorker) {
	flowType := worker.FlowType()

	// Add handler for flow-specific topic
	r.Router.AddNoPublisherHandler(
		fmt.Sprintf("flow.%s.handler", flowType),
		fmt.Sprintf("flow.%s", flowType),
		r.PubSub,
		func(msg *message.Message) error {
			return worker.HandleMessage(msg)
		},
	)

	// Add handler for node completion events
	r.Router.AddNoPublisherHandler(
		fmt.Sprintf("flow.%s.node_completion_handler", flowType),
		"node.completed",
		r.PubSub,
		func(msg *message.Message) error {
			return worker.HandleNodeCompletedMessage(msg)
		},
	)
}

// RegisterAllNodeWorkers registers all the given node workers with the router
func (r *WatermillEventRouter) RegisterAllNodeWorkers(workers ...NodeWorker) {
	for _, worker := range workers {
		r.RegisterNodeWorker(worker)
	}
}

// RegisterAllFlowWorkers registers all the given flow workers with the router
func (r *WatermillEventRouter) RegisterAllFlowWorkers(workers ...FlowWorker) {
	for _, worker := range workers {
		r.RegisterFlowWorker(worker)
	}
}

// SetupProgressHandler sets up a handler for progress updates
func (r *WatermillEventRouter) SetupProgressHandler(handler func(ProgressUpdateMessage) error) {
	r.Router.AddNoPublisherHandler(
		"progress.handler",
		"progress",
		r.PubSub,
		func(msg *message.Message) error {
			var progress ProgressUpdateMessage
			if err := json.Unmarshal(msg.Payload, &progress); err != nil {
				return err
			}
			return handler(progress)
		},
	)
}

// SetupFlowCompletionHandler sets up a handler for flow completion events
func (r *WatermillEventRouter) SetupFlowCompletionHandler(handler func(FlowCompletedMessage) error) {
	r.Router.AddNoPublisherHandler(
		"flow.completed.handler",
		"flow.completed",
		r.PubSub,
		func(msg *message.Message) error {
			var completed FlowCompletedMessage
			if err := json.Unmarshal(msg.Payload, &completed); err != nil {
				return err
			}
			return handler(completed)
		},
	)
}

// SetupFlowFailureHandler sets up a handler for flow failure events
func (r *WatermillEventRouter) SetupFlowFailureHandler(handler func(FlowFailedMessage) error) {
	r.Router.AddNoPublisherHandler(
		"flow.failed.handler",
		"flow.failed",
		r.PubSub,
		func(msg *message.Message) error {
			var failed FlowFailedMessage
			if err := json.Unmarshal(msg.Payload, &failed); err != nil {
				return err
			}
			return handler(failed)
		},
	)
}

// Start the router
func (r *WatermillEventRouter) Start(ctx context.Context) error {
	return r.Router.Run(ctx)
}

// Stop the router
func (r *WatermillEventRouter) Stop() error {
	return r.Router.Close()
}

// Implementation of EventPublisher interface
type WatermillPublisher struct {
	PubSub *gochannel.GoChannel
	Log    zerolog.Logger
}

func NewWatermillPublisher(pubSub *gochannel.GoChannel) *WatermillPublisher {
	return &WatermillPublisher{
		PubSub: pubSub,
		Log:    logger.For("publisher"),
	}
}

func (p *WatermillPublisher) Publish(topic string, event interface{}) error {
	payload, err := json.Marshal(event)
	if err != nil {
		p.Log.Error().Err(err).Str("topic", topic).
			Interface("event", event).Msg("Failed to marshal event")
		return err
	}

	msg := message.NewMessage(watermill.NewUUID(), payload)
	p.Log.Debug().Str("topic", topic).RawJSON("payload", payload).Msg("Publishing event")

	err = p.PubSub.Publish(topic, msg)
	if err != nil {
		p.Log.Error().Err(err).Str("topic", topic).Msg("Failed to publish event")
		return err
	}

	p.Log.Trace().Str("topic", topic).Msg("Event published successfully")
	return nil
}
