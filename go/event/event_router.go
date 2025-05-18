package event

import (
	"context"
	"encoding/json"

	"github.com/The-Pocket/PocketFlow/go/logger"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/rs/zerolog"
)

type WatermillEventRouter struct {
	FlowOrchestrator *FlowOrchestrator
	NodeWorkers      map[string]NodeWorker
	PubSub           *gochannel.GoChannel
	Router           *message.Router
}

func NewWatermillEventRouter(orchestrator *FlowOrchestrator, workers map[string]NodeWorker) *WatermillEventRouter {
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

	// We'll register orchestrator-dependent handlers later if orchestrator is provided now
	if orchestrator != nil {
		setupOrchestratorHandlers(router, pubSub, orchestrator)
	}

	return &WatermillEventRouter{
		FlowOrchestrator: orchestrator,
		NodeWorkers:      workers,
		PubSub:           pubSub,
		Router:           router,
	}
}

// Add all node worker handlers
func (r *WatermillEventRouter) SetupNodeWorkerHandlers() {
	r.SetupNodeWorkerHandlersWithWorkers(r.NodeWorkers)
}

// SetupNodeWorkerHandlersWithWorkers registers handlers for the provided node workers
func (r *WatermillEventRouter) SetupNodeWorkerHandlersWithWorkers(workers map[string]NodeWorker) {
	r.NodeWorkers = workers
	for nodeType, worker := range workers {
		// Prep handler
		r.Router.AddNoPublisherHandler(
			"node."+nodeType+".prep.requested.handler",
			"node."+nodeType+".prep.requested",
			r.PubSub,
			func(w NodeWorker) func(msg *message.Message) error {
				return func(msg *message.Message) error {
					var event NodePrepRequested
					err := json.Unmarshal(msg.Payload, &event)
					if err != nil {
						return err
					}
					w.HandlePrepRequested(event)
					return nil
				}
			}(worker),
		)

		// Exec handler
		r.Router.AddNoPublisherHandler(
			"node."+nodeType+".exec.requested.handler",
			"node."+nodeType+".exec.requested",
			r.PubSub,
			func(w NodeWorker) func(msg *message.Message) error {
				return func(msg *message.Message) error {
					var event NodeExecRequested
					err := json.Unmarshal(msg.Payload, &event)
					if err != nil {
						return err
					}
					w.HandleExecRequested(event)
					return nil
				}
			}(worker),
		)

		// Post handler
		r.Router.AddNoPublisherHandler(
			"node."+nodeType+".post.requested.handler",
			"node."+nodeType+".post.requested",
			r.PubSub,
			func(w NodeWorker) func(msg *message.Message) error {
				return func(msg *message.Message) error {
					var event NodePostRequested
					err := json.Unmarshal(msg.Payload, &event)
					if err != nil {
						return err
					}
					w.HandlePostRequested(event)
					return nil
				}
			}(worker),
		)

		// Exec failed handler
		r.Router.AddNoPublisherHandler(
			"node."+nodeType+".exec.failed.handler",
			"node."+nodeType+".exec.failed",
			r.PubSub,
			func(w NodeWorker) func(msg *message.Message) error {
				return func(msg *message.Message) error {
					var event NodeExecFailed
					err := json.Unmarshal(msg.Payload, &event)
					if err != nil {
						return err
					}
					w.HandleExecFailed(event)
					return nil
				}
			}(worker),
		)
	}
}

// UpdateOrchestrator sets the orchestrator and registers its handlers
func (r *WatermillEventRouter) UpdateOrchestrator(orchestrator *FlowOrchestrator) {
	if orchestrator == nil {
		panic("orchestrator cannot be nil")
	}

	r.FlowOrchestrator = orchestrator
	setupOrchestratorHandlers(r.Router, r.PubSub, orchestrator)
}

// Helper function to set up orchestrator-dependent handlers
func setupOrchestratorHandlers(router *message.Router, pubSub *gochannel.GoChannel, orchestrator *FlowOrchestrator) {
	router.AddNoPublisherHandler(
		"flow.start.requested.handler",
		"flow.start.requested",
		pubSub,
		func(msg *message.Message) error {
			var event FlowStartRequested
			err := json.Unmarshal(msg.Payload, &event)
			if err != nil {
				return err
			}
			orchestrator.HandleFlowStartRequested(event)
			return nil
		},
	)

	// For handling node.*.post.completed events, we need to register handlers for each node type
	// First define the handler function
	postCompletedHandler := func(msg *message.Message) error {
		var event NodePostCompleted
		err := json.Unmarshal(msg.Payload, &event)
		if err != nil {
			return err
		}
		orchestrator.HandleNodePostCompleted(event)
		return nil
	}

	// Register for all node types
	router.AddNoPublisherHandler(
		"node.question.post.completed.handler",
		"node.question.post.completed",
		pubSub,
		postCompletedHandler,
	)

	router.AddNoPublisherHandler(
		"node.answer.post.completed.handler",
		"node.answer.post.completed",
		pubSub,
		postCompletedHandler,
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
